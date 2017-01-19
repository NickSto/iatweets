#!/usr/bin/env python
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import os
import sys
import time
import json
import uuid
import errno
import logging
import argparse
import urlparse
import subprocess
import ConfigParser
import requests
import warc
import warc_simple
import retweever
import tweet_tools

KEY_NAMES = ('consumer_key', 'consumer_secret', 'access_token_key', 'access_token_secret')
ARG_DEFAULTS = {'format':'human', 'output':sys.stdout, 'limit':sys.maxsize, 'log':sys.stderr,
                'volume':logging.WARNING}
DESCRIPTION = """This script will read a series of tweets from unzipped WARC files, then use the
Twitter API to re-retrieve them (to get the full, un-truncated text) and gather replies and other
information related to them."""


def main(argv):

  parser = argparse.ArgumentParser(description=DESCRIPTION)
  parser.set_defaults(**ARG_DEFAULTS)

  parser.add_argument('warcs', metavar='path/to/record.warc', nargs='+',
    help='The uncompressed WARC file(s).')
  parser.add_argument('-o', '--output', type=argparse.FileType('w'),
    help='Write output to this file. If --output type is "warc", the basename of this path will be '
         'used as the value for WARC-Filename in the warcinfo record.')
  parser.add_argument('-p', '--parse-tweets', action='store_true',
    help='Just parse the tweets from the WARC files and print them out. '
         'No Twitter API keys required. This will print the WARCs almost literally as they exist '
         'in the original files, but it will add a wARC-Record-Id to each (missing in the '
         'originals).')
  parser.add_argument('-f', '--format', choices=('human', 'warc'),
    help='Print either human-readable text or a WARC record for each tweet.')
  parser.add_argument('-O', '--oauth-file',
    help='A config file containing the OAuth keys. See "oauth.cfg.sample" for an example (with '
         'dummy keys). For obtaining these from Twitter, see '
         'https://python-twitter.readthedocs.io/en/latest/getting_started.html')
  parser.add_argument('-d', '--dedup', action='store_true',
    help='Don\'t retrieve tweets already obtained in this run.')
  parser.add_argument('-I', '--no-warcinfo', action='store_true',
    help='Don\'t write a warcinfo WARC record.')
  parser.add_argument('--operator',
    help='Value for this field in the warcinfo record.')
  parser.add_argument('--description',
    help='Value for this field in the warcinfo record.')
  parser.add_argument('--ip',
    help='Value for this field in the warcinfo record. If not given, this will try to determine '
         'your IP automatically.')
  parser.add_argument('-L', '--limit', type=int,
    help='Maximum number of tweets to request from the Twitter API.')
  parser.add_argument('-c', '--consumer-key')
  parser.add_argument('-C', '--consumer-secret')
  parser.add_argument('-a', '--access-token-key')
  parser.add_argument('-A', '--access-token-secret')
  parser.add_argument('-l', '--log', type=argparse.FileType('w'),
    help='Print log messages to this file instead of to stderr. Warning: Will overwrite the file.')
  parser.add_argument('-q', '--quiet', dest='volume', action='store_const', const=logging.CRITICAL)
  parser.add_argument('-v', '--verbose', dest='volume', action='store_const', const=logging.INFO)
  parser.add_argument('-D', '--debug', dest='volume', action='store_const', const=logging.DEBUG)

  args = parser.parse_args(argv[1:])

  logging.basicConfig(stream=args.log, level=args.volume, format='%(message)s')
  tone_down_logger()

  # Initialize the Twitter API object.
  if not args.parse_tweets:
    if args.oauth_file:
      keys = read_oauth_config(args.oauth_file, KEY_NAMES)
    else:
      keys = {}
      for key_name in KEY_NAMES:
        key = getattr(args, key_name)
        if key:
          keys[key_name] = key
        else:
          fail('All four OAuth tokens must be given unless --parse-tweets is.')
    api = retweever.Api(tweet_mode='extended', sleep_on_rate_limit=True, **keys)

  # The output filename, if present, should be put in the warcinfo record.
  if args.output is sys.stdout:
    outfile = None
  else:
    outfile = os.path.basename(args.output.name)

  done = {}
  api_requests = 0
  empties = 0
  file_num = 0
  entry_num = 0
  for warc_path in args.warcs:
    file_num += 1
    logging.info('Starting file {}: {}'.format(file_num, warc_path))
    # Create the warcinfo WARC record for this file.
    if args.format == 'warc':
      warc_name = os.path.basename(warc_path)
      warcinfo = create_warcinfo(infile=warc_name, outfile=outfile, ip=args.ip,
                                 operator=args.operator, description=args.description)
      warcinfo_id = warcinfo.header.record_id
      warcinfo.write_to(args.output)
      args.output.write('\r\n')
    for entry, headers in warc_simple.parse(warc_path, payload_json=False, header_dict=False):
      entry_num += 1
      tweet = tweet_tools.extract_tweet(entry)
      headers_dict = warc_simple.headers_to_dict(headers)
      if not tweet:
        # Empty entry.
        empties += 1
        logging.debug(entry)
        if args.format == 'human':
          args.output.write('{}/{}: Empty entry.\n\n'.format(file_num, entry_num))
        elif args.format == 'warc':
          # Print it literally and move on.
          args.output.write(warc_header_fix(headers, headers_dict, warcinfo_id, tweet)+'\r\n')
          args.output.write(entry+'\r\n')
      elif args.parse_tweets:
        if args.format == 'human':
          args.output.write(tweet_tools.format_tweet_for_humans(tweet, None, file_num, entry_num)+'\n')
        elif args.format == 'warc':
          args.output.write(warc_header_fix(headers, headers_dict, warcinfo_id, tweet)+'\r\n')
          args.output.write(entry+'\r\n')
      else:
        # Print this tweet and all others above it in the conversation.
        target_uri = headers_dict.get('WARC-Refers-To-Target-URI')
        is_profile = target_uri and target_uri.startswith('https://api.twitter.com/1.1/users/lookup.json')
        tweet_looks_truncated = tweet_tools.does_tweet_look_truncated(tweet)
        # Determine whether to skip re-retrieving the first tweet.
        # If it's a profile, or if it didn't get truncated, we can use the original.
        use_original = is_profile or not tweet_looks_truncated
        if use_original:
          # We already have the whole tweet or it's a profile Only get replied-to tweets.
          logging.info('{}/{}: Tweet is a profile or not truncated. Not re-retrieving.'
                       .format(file_num, entry_num))
          tweet_id = tweet.get('in_reply_to_status_id')
        else:
          # Include this tweet in those retrieved from the Twitter API.
          logging.info('{}/{}: Non-profile tweet looks truncated. Re-retrieving.'
                       .format(file_num, entry_num))
          tweet_id = tweet['id']
        # Retrieve all tweets in the conversation.
        remaining = args.limit - api_requests
        if args.dedup:
          reply_chain = get_replied_tweets(tweet_id, api, remaining=remaining, done=done)
        else:
          reply_chain = get_replied_tweets(tweet_id, api, remaining=remaining)
        api_requests += len(reply_chain)
        if tweet_id != tweet['id']:
          # The original tweet isn't yet in the list of those retrieved from Twitter, since it's a
          # profile or it wasn't truncated. Add it to the front.
          reply_chain.insert(0, tweet)
        limit = api.get_rate_limit()
        rate_limit_summary = summarize_rate_limit_status(api)
        if rate_limit_summary:
          logging.info('{}/{}: {}'.format(file_num, entry_num, rate_limit_summary))
        if tweet['in_reply_to_status_id']:
          logging.info('{}/{}: Reply tweet; retrieved {} in reply chain.'
                       .format(file_num, entry_num, len(reply_chain)))
        elif len(reply_chain) == 0:
          logging.warn('{}/{}: No tweets in conversation.'.format(file_num, entry_num))
        first_tweet = True
        # Print out the conversation.
        replied_by = None
        for response in reply_chain:
          if args.format == 'human':
            if not hasattr(response, 'status_code'):
              # It's the JSON of the original tweet, meaning we didn't need to re-retrieve it.
              # Just print the original tweet.
              args.output.write(tweet_tools.format_tweet_for_humans(tweet, replied_by, file_num, entry_num))
            elif response.status_code == 200:
              args.output.write(tweet_tools.format_tweet_for_humans(response, replied_by, file_num, entry_num))
            elif first_tweet:
              # It's the first tweet in the conversation, but it's truncated, and retrieval from
              # the Twitter API failed. Use the original data from the input WARC instead.
              args.output.write(tweet_tools.format_tweet_for_humans(tweet, replied_by, file_num, entry_num))
            else:
              # It's an earlier tweet in the conversation, but retrieval from the Twitter API
              # failed. All we can do is print the error response.
              logging.warn('{}/{}: Twitter API error {}'
                           .format(file_num, entry_num, response.status_code))
          elif args.format == 'warc':
            if not hasattr(response, 'status_code'):
              # It's the JSON of the original tweet, meaning we didn't need to re-retrieve it.
              # Printing the original data from the WARC is all we need.
              new_headers = warc_header_fix(headers, headers_dict, warcinfo_id, tweet, replied_by)
              args.output.write(new_headers+'\r\n')
              args.output.write(entry+'\r\n')
            elif response.status_code == 200:
              write_warcs(response, args.output, warcinfo_id, replied_by)
            elif first_tweet:
              # It's the first tweet in the conversation, but it's truncated, and retrieval from
              # the Twitter API failed. Use the original data from the input WARC instead.
              logging.warn('{}/{}: Twitter API error {} on old tweet. Using original data instead.'
                           .format(file_num, entry_num, response.status_code))
              new_headers = warc_header_fix(headers, headers_dict, warcinfo_id, tweet, replied_by)
              args.output.write(new_headers+'\r\n')
              args.output.write(entry+'\r\n')
            else:
              # It's an earlier tweet in the conversation, but retrieval from the Twitter API
              # failed. All we can do is print the error response.
              logging.warn('{}/{}: Twitter API error {}.'
                           .format(file_num, entry_num, response.status_code))
              write_warcs(response, args.output, warcinfo_id, replied_by)
          first_tweet = False
          replied_by = response
        if args.format == 'warc':
          args.output.write('\r\n')
        else:
          args.output.write('\n')
      if api_requests >= args.limit:
        logging.info('--limit exceeded: {} >= {}'.format(api_requests, args.limit))
        break
    if api_requests >= args.limit:
      break
  logging.info('Total: {}'.format(entry_num))
  logging.info('Empties: {}'.format(empties))
  logging.info('Skipped: {}'.format(sum(done.values())))
  logging.info('Twitter API requests: {}'.format(api_requests))


def create_warcinfo(infile=None, outfile=None, ip=None, operator=None, description=None):
  # Compile WARC headers.
  warc_headers_dict = {'WARC-Type':'warcinfo'}
  if outfile:
    warc_headers_dict['WARC-Filename'] = outfile
  warc_headers = warc.WARCHeader(warc_headers_dict, defaults=True)
  # Compile info for body of the warcinfo record.
  info_fields = {
    'format': 'WARC File Format 1.0',
    'conformsTo': 'http://bibnum.bnf.fr/WARC/WARC_ISO_28500_version1_latestdraft.pdf',
    'software': 'https://github.com/NickSto/iatweets',
    'http-header-user-agent': 'python-requests/'+requests.__version__,
  }
  if infile:
    info_fields['modified-from'] = infile
  git_commit = get_git_commit()
  if git_commit:
    info_fields['software'] += ' (commit {})'.format(git_commit)
  if ip:
    info_fields['ip'] = ip
  else:
    ip = run_command(['curl', '-s', 'https://icanhazip.com'], strip_newline=True)
    if ip:
      info_fields['ip'] = ip
  if operator:
    info_fields['operator'] = operator
  if description:
    info_fields['description'] = description
  info_header_str = '\r\n'.join([header+': '+value for header, value in info_fields.items()])
  return warc.WARCRecord(warc_headers, info_header_str)


def warc_header_fix(headers, headers_dict=None, warcinfo_id=None, tweet=None, replied_by=None):
  """Let's add some headers to one of the original WARCs.
  Most importantly, they lack a WARC-Record-ID."""
  if not headers_dict:
    headers_dict = warc_simple.headers_to_dict(headers)
  if tweet and tweet.get('in_reply_to_screen_name') and tweet.get('in_reply_to_status_id'):
    headers += 'WARC-X-Tweet-Reply-To: '+tweet_tools.get_in_reply_to_url(tweet)+'\r\n'
  if replied_by:
    headers += 'WARC-X-Tweet-Replied-By: '+tweet_tools.get_tweet_url(replied_by)+'\r\n'
  if warcinfo_id and 'WARC-Warcinfo-ID' not in headers_dict:
    headers += 'WARC-Warcinfo-ID: '+warcinfo_id+'\r\n'
  if 'WARC-Record-ID' not in headers_dict:
    headers += 'WARC-Record-ID: <urn:uuid:{}>\r\n'.format(uuid.uuid4())
  return headers


def write_warcs(response, destination=sys.stdout, warcinfo_id=None, replied_by=None):
  response_warc = make_response_warc(response, warcinfo_id, replied_by)
  record_id = response_warc.header['WARC-Record-Id']
  request_warc = make_request_warc(response, record_id, warcinfo_id, replied_by)
  request_warc.write_to(destination)
  response_warc.write_to(destination)


def make_response_warc(response, warcinfo_id=None, replied_by=None):
  warc_headers_dict = {'WARC-Type':'response',
                       'WARC-Target-URI':response.request.url}
  if warcinfo_id:
    warc_headers_dict['WARC-Warcinfo-ID'] = warcinfo_id
  if replied_by:
    warc_headers_dict['WARC-X-Tweet-Replied-By'] = tweet_tools.get_tweet_url(replied_by)
  tweet = response.json()
  if tweet and tweet.get('in_reply_to_screen_name') and tweet.get('in_reply_to_status_id'):
    warc_headers_dict['WARC-X-Tweet-Reply-To'] = tweet_tools.get_in_reply_to_url(tweet)
  warc_headers = warc.WARCHeader(warc_headers_dict, defaults=True)

  response_headers = 'HTTP/1.1 {} {}\r\n'.format(response.status_code, response.reason)
  for header, value in response.headers.items():
    response_headers += '{}: {}\r\n'.format(header, value)

  payload = response_headers+'\r\n'+response.content
  return warc.WARCRecord(warc_headers, payload)


def make_request_warc(response, response_id, warcinfo_id=None, replied_by=None):
  request = response.request
  warc_headers_dict = {'WARC-Type':'request',
                       'WARC-Concurrent-To':response_id,
                       'WARC-Target-URI':request.url}
  if warcinfo_id:
    warc_headers_dict['WARC-Warcinfo-ID'] = warcinfo_id
  if replied_by:
    warc_headers_dict['WARC-X-Tweet-Replied-By'] = tweet_tools.get_tweet_url(replied_by)
  tweet = response.json()
  if tweet and tweet.get('in_reply_to_screen_name') and tweet.get('in_reply_to_status_id'):
    warc_headers_dict['WARC-X-Tweet-Reply-To'] = tweet_tools.get_in_reply_to_url(tweet)
  warc_headers = warc.WARCHeader(warc_headers_dict, defaults=True)

  request_headers = '{} {} HTTP/1.1\r\n'.format(request.method, request.path_url)
  request_headers += 'Host: {}\r\n'.format(urlparse.urlparse(request.url)[1])
  for header, value in request.headers.items():
    request_headers += '{}: {}\r\n'.format(header, value)

  return warc.WARCRecord(warc_headers, request_headers)


def get_replied_tweets(id, api, remaining=None, done=None):
  """Retrieve a tweet and all tweets before it in the conversation chain.
  Supply the status id as an integer and an authenticated retweever.Api object.
  The status id can be None or 0 if no tweets need to be retrieved. Then this will return an empty
  list.
  Returns a list of requests.models.Response objects, one for each tweet in the chain."""
  if done is None:
    done = {}
  reply_chain = []
  while id:
    if id in done:
      logging.info('Tweet {} already done {} times. Skipping..'.format(id, done[id]))
      done[id] += 1
      break
    if remaining is None or remaining > 0:
      data, response = api.GetStatus(id)
      remaining -= 1
    else:
      logging.warn('--limit exceeded when there were tweets from a conversation remaining to be '
                   'requested.')
      break
    reply_chain.append(response)
    if response.status_code == 200:
      done[id] = done.get(id, 0) + 1
      response_json = response.json()
      try:
        id = response_json['in_reply_to_status_id']
      except KeyError:
        break
    else:
      break
  return reply_chain


def summarize_rate_limit_status(api):
  limit = api.get_rate_limit()
  now = time.time()
  until_reset = limit.reset - now
  if limit.reset == 0:
    return ''
  else:
    return '{} requests remaining in next {:0.1f} minutes'.format(limit.remaining, until_reset/60)


def read_oauth_config(oauth_file, key_names):
  config = ConfigParser.RawConfigParser()
  config.read(oauth_file)
  keys = {}
  for key_name in key_names:
    if config.has_option('auth', key_name):
      keys[key_name] = config.get('auth', key_name)
    else:
      fail('OAuth token "{}" not found in --oauth-file "{}".'.format(key_name, oauth_file))
  return keys


def get_git_commit():
  """Get the current git commit of this script, for the warcinfo record."""
  script_dir = os.path.dirname(os.path.realpath(__file__))
  # We have to cd to the repository directory because the --git-dir and --work-tree options don't
  # work on BSD.
  original_cwd = os.getcwd()
  try:
    if original_cwd != script_dir:
      os.chdir(script_dir)
    commit = run_command(['git', 'log', '-n', '1', '--pretty=%h'], strip_newline=True)
  except OSError:
    return None
  finally:
    if original_cwd != os.getcwd():
      os.chdir(original_cwd)
  return commit


def run_command(command, strip_newline=False):
  devnull = open(os.devnull, 'w')
  try:
    output = subprocess.check_output(command, stderr=devnull)
    exit_status = 0
  except subprocess.CalledProcessError as cpe:
    output = cpe.output
    exit_status = cpe.returncode
  except OSError as ose:
    exit_status = None
  finally:
    devnull.close()
  if exit_status is None or exit_status != 0:
    return None
  elif strip_newline:
    return output.rstrip('\r\n')
  else:
    return output


def tone_down_logger():
  """Change the logging level names from all-caps to capitalized lowercase.
  E.g. "WARNING" -> "Warning" (turn down the volume a bit in your log files)"""
  for level in (logging.CRITICAL, logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG):
    level_name = logging.getLevelName(level)
    logging.addLevelName(level, level_name.capitalize())


def fail(message):
  logging.critical(message)
  sys.exit(1)


if __name__ == '__main__':
  try:
    sys.exit(main(sys.argv))
  except IOError as ioe:
    if ioe.errno != errno.EPIPE:
      raise
