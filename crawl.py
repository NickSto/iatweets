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
  rate_limit = -1
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
      target_uri = headers_dict.get('WARC-Refers-To-Target-URI')
      is_profile = target_uri and target_uri.startswith('https://api.twitter.com/1.1/users/lookup.json')
      if tweet:
        tweet['is_profile'] = is_profile
      if not tweet:
        # Empty entry.
        empties += 1
        if args.format == 'human':
          args.output.write('{}/{}: Empty entry.\n\n'.format(file_num, entry_num))
        elif args.format == 'warc':
          # Print it literally and move on.
          args.output.write(warc_header_fix(headers, headers_dict, warcinfo_id)+'\r\n')
          args.output.write(entry+'\r\n')
      elif args.parse_tweets:
        # Don't make any Twitter API requests. Just print what you see, with some fixes.
        tweet_data = {'tweet':tweet}
        if args.format == 'human':
          args.output.write(tweet_tools.format_tweet_for_humans(tweet_data, file_num, entry_num)+'\n')
        elif args.format == 'warc':
          args.output.write(warc_header_fix(headers, headers_dict, warcinfo_id, tweet_data)+'\r\n')
          args.output.write(entry+'\r\n')
      else:
        # Print this tweet and all others above it in the conversation.
        looks_truncated = tweet_tools.does_tweet_look_truncated(tweet)
        # Determine whether to skip re-retrieving the first tweet.
        # If it's a profile, or if it didn't get truncated, we can use the original.
        use_original = is_profile or not looks_truncated
        logging.info('{}/{}: is_profile: {}, looks_truncated: {}, use_original: {}'
                     .format(file_num, entry_num, is_profile, looks_truncated, use_original))
        # Retrieve all tweets in the conversation.
        remaining = args.limit - api_requests
        if args.dedup:
          try:
            conversation = get_conversation(tweet, api, use_original, remaining, done)
          except AttributeError:
            logging.warn('{}/{}: {}'.format(file_num, entry_num, tweet))
            raise
        else:
          conversation = get_conversation(tweet, api, use_original, remaining)
        api_requests += len(conversation)
        if use_original:
          api_requests -= 1
        rate_limit = summarize_rate_limit_status(api, rate_limit, file_num, entry_num)
        if tweet['in_reply_to_id']:
          logging.info('{}/{}: Reply tweet; retrieved {} in conversation chain.'
                       .format(file_num, entry_num, len(conversation)))
        elif len(conversation) == 0:
          logging.warn('{}/{}: No tweets in conversation.'.format(file_num, entry_num))
        # Print out the conversation.
        first_tweet = True
        for tweet_data in conversation:
          response = tweet_data['response']
          api_error = get_api_error(response)
          if use_original:
            # We didn't need to re-retrieve the tweet. Just print the original.
            if args.format == 'human':
              args.output.write(tweet_tools.format_tweet_for_humans(tweet_data, file_num, entry_num))
            elif args.format == 'warc':
              new_headers = warc_header_fix(headers, headers_dict, warcinfo_id, tweet_data)
              args.output.write(new_headers+'\r\n')
              args.output.write(entry+'\r\n')
          elif response.status_code == 200:
            if args.format == 'human':
              args.output.write(tweet_tools.format_tweet_for_humans(tweet_data, file_num, entry_num))
            elif args.format == 'warc':
              write_warcs(tweet_data, args.output, warcinfo_id)
          elif first_tweet:
            # It's the first tweet in the conversation, but it's truncated, and retrieval from
            # the Twitter API failed. Use the original data from the input WARC instead.
            if args.format == 'human':
              args.output.write(tweet_tools.format_tweet_for_humans(tweet_data, file_num, entry_num))
            elif args.format == 'warc':
              logging.warn('{}/{}: {} on old tweet. Using original data instead.'
                           .format(file_num, entry_num, api_error))
              new_headers = warc_header_fix(headers, headers_dict, warcinfo_id, tweet_data)
              args.output.write(new_headers+'\r\n')
              args.output.write(entry+'\r\n')
          else:
            # It's an earlier tweet in the conversation, but retrieval from the Twitter API
            # failed. All we can do is print the error response.
            if args.format == 'human':
              logging.warn('{}/{}: {}'.format(file_num, entry_num, api_error))
            elif args.format == 'warc':
              logging.warn('{}/{}: {}'.format(file_num, entry_num, api_error))
              write_warcs(tweet_data, args.output, warcinfo_id)
          first_tweet = False
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
    'extension-fields': ('WARC-X-Tweet-Reply-To WARC-X-Tweet-Replied-By '
                         'WARC-X-Tweet-Retweeted WARC-X-Tweet-Retweeted-By')
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


def warc_header_fix(headers, headers_dict=None, warcinfo_id=None, tweet_data=None):
  """Let's add some headers to one of the original WARCs.
  Most importantly, they lack a WARC-Record-ID."""
  if not headers_dict:
    headers_dict = warc_simple.headers_to_dict(headers)
  if tweet_data and tweet_data.get('in_reply_to_id'):
    headers += 'WARC-X-Tweet-Reply-To: '+tweet_tools.get_tweet_url(tweet_data, 'reply_to')+'\r\n'
  elif tweet_data and tweet_data['tweet'] and tweet_data['tweet'].get('in_reply_to_id'):
    headers += 'WARC-X-Tweet-Reply-To: '+tweet_tools.get_tweet_url(tweet_data['tweet'], 'reply_to')+'\r\n'
  if tweet_data and tweet_data.get('replied_by_id'):
    headers += 'WARC-X-Tweet-Replied-By: '+tweet_tools.get_tweet_url(tweet_data, 'replied_by')+'\r\n'
  if tweet_data and tweet_data.get('retweeted_by_id'):
    headers += 'WARC-X-Tweet-Retweeted-By: '+tweet_tools.get_tweet_url(tweet_data, 'retweeted_by')+'\r\n'
  if warcinfo_id and 'WARC-Warcinfo-ID' not in headers_dict:
    headers += 'WARC-Warcinfo-ID: '+warcinfo_id+'\r\n'
  if 'WARC-Record-ID' not in headers_dict:
    headers += 'WARC-Record-ID: <urn:uuid:{}>\r\n'.format(uuid.uuid4())
  return headers


def write_warcs(tweet_data, destination=sys.stdout, warcinfo_id=None):
  response_warc = make_response_warc(tweet_data, warcinfo_id)
  record_id = response_warc.header['WARC-Record-Id']
  request_warc = make_request_warc(tweet_data, record_id, warcinfo_id)
  request_warc.write_to(destination)
  response_warc.write_to(destination)


def make_response_warc(tweet_data, warcinfo_id=None):
  response = tweet_data['response']
  warc_headers_dict = {'WARC-Type':'response',
                       'WARC-Target-URI':response.request.url}
  if warcinfo_id:
    warc_headers_dict['WARC-Warcinfo-ID'] = warcinfo_id
  if tweet_data.get('replied_by_id'):
    warc_headers_dict['WARC-X-Tweet-Replied-By'] = tweet_tools.get_tweet_url(tweet_data, 'replied_by')
  if tweet_data.get('in_reply_to_id'):
    warc_headers_dict['WARC-X-Tweet-Reply-To'] = tweet_tools.get_tweet_url(tweet_data, 'reply_to')
  if tweet_data.get('retweeted_by_id'):
    warc_headers_dict['WARC-X-Tweet-Retweeted-By'] = tweet_tools.get_tweet_url(tweet_data, 'retweeted_by')
  warc_headers = warc.WARCHeader(warc_headers_dict, defaults=True)

  response_headers = 'HTTP/1.1 {} {}\r\n'.format(response.status_code, response.reason)
  for header, value in response.headers.items():
    response_headers += '{}: {}\r\n'.format(header, value)

  payload = response_headers+'\r\n'+response.content
  return warc.WARCRecord(warc_headers, payload)


def make_request_warc(tweet_data, response_id, warcinfo_id=None):
  response = tweet_data['response']
  request = response.request
  warc_headers_dict = {'WARC-Type':'request',
                       'WARC-Concurrent-To':response_id,
                       'WARC-Target-URI':request.url}
  if warcinfo_id:
    warc_headers_dict['WARC-Warcinfo-ID'] = warcinfo_id
  if tweet_data.get('replied_by_id'):
    warc_headers_dict['WARC-X-Tweet-Replied-By'] = tweet_tools.get_tweet_url(tweet_data, 'replied_by')
  if tweet_data.get('in_reply_to_id'):
    warc_headers_dict['WARC-X-Tweet-Reply-To'] = tweet_tools.get_tweet_url(tweet_data, 'reply_to')
  if tweet_data.get('retweeted_by_id'):
    warc_headers_dict['WARC-X-Tweet-Retweeted-By'] = tweet_tools.get_tweet_url(tweet_data, 'retweeted_by')
  warc_headers = warc.WARCHeader(warc_headers_dict, defaults=True)

  request_headers = '{} {} HTTP/1.1\r\n'.format(request.method, request.path_url)
  request_headers += 'Host: {}\r\n'.format(urlparse.urlparse(request.url)[1])
  for header, value in request.headers.items():
    request_headers += '{}: {}\r\n'.format(header, value)

  return warc.WARCRecord(warc_headers, request_headers)


def get_conversation(tweet, api, use_original=False, remaining=None, done=None):
  """Retrieve a tweet and all tweets before it in the conversation chain.
  Supply the root tweet JSON and an authenticated retweever.Api object."""
  conversation = []
  if done is None:
    done = {}
  # If use_original, use the first tweet as-is instead of fetching it again from the API.
  if use_original:
    id = tweet['id']
    conversation.append({'id':id,
                         'tweet':tweet,
                         'response':None,
                         'in_reply_to_id':tweet.get('in_reply_to_id'),
                         'in_reply_to_user':tweet.get('in_reply_to_user'),
                         'replied_by_id':None,
                         'replied_by_id':None,
                         'retweeted_id':tweet.get('retweeted_id'),
                         'retweeted_by_id':None,
                         'retweeted_by_user':None,
                        })
    id = tweet.get('in_reply_to_id')
  else:
    id = tweet['id']
  replied_by_id = None
  replied_by_user = None
  retweeted_by_id = None
  retweeted_by_user = None
  while id:
    if id in done:
      logging.info('Tweet {} already done. Skipping..'.format(id))
      break
    if remaining is None or remaining > 0:
      response = api.GetStatus(id)
      remaining -= 1
    else:
      logging.warn('--limit exceeded when there were tweets from a conversation remaining to be '
                   'requested.')
      break
    if response.status_code == 200:
      done[id] = done.get(id, 0) + 1
      try:
        tweet = tweet_tools.extract_tweet(response, datatype='request') or {}
      except ValueError:
        tweet = {}
    else:
      tweet = {}
    retweeted_id = tweet.get('retweeted_id')
    in_reply_to_id = tweet.get('in_reply_to_id')
    in_reply_to_user = tweet.get('in_reply_to_user')
    conversation.append({'id':id,
                         'tweet':tweet,
                         'response':response,
                         'in_reply_to_id':in_reply_to_id,
                         'in_reply_to_user':in_reply_to_user,
                         'replied_by_id':replied_by_id,
                         'replied_by_user':replied_by_user,
                         'retweeted_id':retweeted_id,
                         'retweeted_by_id':retweeted_by_id,
                         'retweeted_by_user':retweeted_by_user
                        })
    if not tweet:
      break
    if in_reply_to_id:
      replied_by_id = id
      replied_by_user = tweet.get('user')
    else:
      replied_by_id = None
      replied_by_user = None
    if retweeted_id:
      retweeted_by_id = id
      retweeted_by_user = tweet.get('user')
    else:
      retweeted_by_id = None
      retweeted_by_user = None
    # The next tweet in the conversation could be one this replied to, or one it retweeted.
    # In this dataset, it's never both.
    id = in_reply_to_id or retweeted_id
  return conversation


def get_api_error(response):
  if response is None:
    return ''
  status_code = str(response.status_code)
  try:
    resp_json = response.json()
  except ValueError:
    return status_code
  error_strs = []
  for error in resp_json.get('errors', ()):
    error_strs.append('{}: "{}"'.format(error.get('code'), error.get('message')))
  if len(error_strs) == 0:
    return 'Twitter API status code '+status_code
  if len(error_strs) == 1:
    return 'Twitter API status code {}, API error {}'.format(status_code, error_strs[0])
  elif len(error_strs) > 1:
    return 'Twitter API status code {}, API errors {}'.format(status_code, ', '.join(error_strs))


def summarize_rate_limit_status(api, last_remaining, file_num, entry_num):
  limit = api.get_rate_limit()
  if limit.remaining == last_remaining:
    return last_remaining
  now = time.time()
  until_reset = limit.reset - now
  if limit.reset == 0:
    return limit.remaining
  logging.info('{}/{}: {} requests remaining in next {:0.1f} minutes'
               .format(file_num, entry_num, limit.remaining, until_reset/60))
  return limit.remaining


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
