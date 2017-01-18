#!/usr/bin/env python
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import sys
import time
import json
import uuid
import errno
import urlparse
import logging
import argparse
import ConfigParser
import requests
import warc
import warc_simple
import retweever
import tweet_tools

KEY_NAMES = ('consumer_key', 'consumer_secret', 'access_token_key', 'access_token_secret')
ARG_DEFAULTS = {'output':'human', 'log':sys.stderr, 'volume':logging.WARNING}
DESCRIPTION = """This script will read a series of tweets from unzipped WARC files, then use the
Twitter API to re-retrieve them (to get the full, un-truncated text) and gather replies and other
information related to them."""


def main(argv):

  parser = argparse.ArgumentParser(description=DESCRIPTION)
  parser.set_defaults(**ARG_DEFAULTS)

  parser.add_argument('warcs', metavar='path/to/record.warc', nargs='+',
    help='The uncompressed WARC file(s).')
  parser.add_argument('-p', '--parse-tweets', action='store_true',
    help='Just parse the tweets from the WARC files and print them out. '
         'No Twitter API keys required. This will print the WARCs almost literally as they exist '
         'in the original files, but it will add a wARC-Record-Id to each (missing in the '
         'originals).')
  parser.add_argument('-o', '--output', choices=('human', 'warc'),
    help='Print either human-readable text or a WARC record for each tweet.')
  parser.add_argument('-O', '--oauth-file',
    help='A config file containing the OAuth keys. See "oauth.cfg.sample" for an example (with '
         'dummy keys). For obtaining these from Twitter, see '
         'https://python-twitter.readthedocs.io/en/latest/getting_started.html')
  parser.add_argument('-d', '--dedup', action='store_true',
    help='Don\'t retrieve tweets already obtained in this run.')
  parser.add_argument('-c', '--consumer-key')
  parser.add_argument('-C', '--consumer-secret')
  parser.add_argument('-a', '--access-token-key')
  parser.add_argument('-A', '--access-token-secret')
  parser.add_argument('-L', '--limit', type=int,
    help='Maximum number of tweets to request from the Twitter API.')
  parser.add_argument('-l', '--log', type=argparse.FileType('w'),
    help='Print log messages to this file instead of to stderr. Warning: Will overwrite the file.')
  parser.add_argument('-q', '--quiet', dest='volume', action='store_const', const=logging.CRITICAL)
  parser.add_argument('-v', '--verbose', dest='volume', action='store_const', const=logging.INFO)
  parser.add_argument('-D', '--debug', dest='volume', action='store_const', const=logging.DEBUG)

  args = parser.parse_args(argv[1:])

  logging.basicConfig(stream=args.log, level=args.volume, format='%(message)s')
  tone_down_logger()

  if args.parse_tweets or args.limit is None:
    remaining = sys.maxsize
  else:
    remaining = args.limit

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

  done = {}
  empties = 0
  file_num = 0
  entry_num = 0
  for warc_path in args.warcs:
    file_num += 1
    logging.info('Starting file {}: {}'.format(file_num, warc_path))
    for entry, headers in warc_simple.parse(warc_path, payload_json=False, header_dict=False):
      entry_num += 1
      tweet = tweet_tools.extract_tweet(entry)
      if not tweet:
        # Empty entry.
        empties += 1
        logging.debug(entry)
        if args.output == 'human':
          print('{}/{}: Empty entry.\n'.format(file_num, entry_num))
        elif args.output == 'warc':
          # Print it literally and move on.
          sys.stdout.write(warc_header_fix(headers)+'\r\n')
          sys.stdout.write(entry+'\r\n')
      elif args.parse_tweets:
        if args.output == 'human':
          print(tweet_tools.format_tweet_for_humans(tweet, file_num, entry_num))
          print()
        elif args.output == 'warc':
          sys.stdout.write(warc_header_fix(headers)+'\r\n')
          sys.stdout.write(entry+'\r\n')
      else:
        # Print this tweet and all others above it in the conversation.
        # Use the Twitter API to re-retrieve this tweet if it's truncated, and the rest of the
        # conversation chain.
        tweet_looks_truncated = tweet_tools.does_tweet_look_truncated(tweet)
        if tweet_looks_truncated:
          # Include this tweet in those retrieved from the Twitter API.
          logging.info('{}/{}: Tweet truncated. Re-retrieving.'.format(file_num, entry_num))
          tweet_id = tweet['id']
        else:
          # We already have the whole tweet. Only get replied-to tweets.
          logging.info('{}/{}: Tweet not truncated. Not re-retrieving.'.format(file_num, entry_num))
          tweet_id = tweet.get('in_reply_to_status_id')
        # Retrieve all tweets in the conversation.
        if args.dedup:
          reply_chain = get_replied_tweets(tweet_id, api, remaining=remaining, done=done)
        else:
          reply_chain = get_replied_tweets(tweet_id, api, remaining=remaining)
        remaining -= len(reply_chain)
        if tweet_id != tweet['id']:
          # The original tweet isn't yet in the list of those retrieved from Twitter, since it
          # wasn't truncated. Add it to the front.
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
        for response in reply_chain:
          if args.output == 'human':
            if not hasattr(response, 'status_code'):
              # It's the JSON of the original tweet, meaning we didn't need to re-retrieve it.
              # Just print the original tweet.
              print(tweet_tools.format_tweet_for_humans(tweet, file_num, entry_num))
            elif response.status_code == 200:
              print(tweet_tools.format_tweet_for_humans(response, file_num, entry_num))
            elif first_tweet:
              # It's the first tweet in the conversation, but it's truncated, and retrieval from
              # the Twitter API failed. Use the original data from the input WARC instead.
              print(tweet_tools.format_tweet_for_humans(tweet, file_num, entry_num))
            else:
              # It's an earlier tweet in the conversation, but retrieval from the Twitter API
              # failed. All we can do is print the error response.
              logging.warn('{}/{}: Twitter API error {}'
                           .format(file_num, entry_num, response.status_code))
          elif args.output == 'warc':
            if not hasattr(response, 'status_code'):
              # It's the JSON of the original tweet, meaning we didn't need to re-retrieve it.
              # Printing the original data from the WARC is all we need.
              sys.stdout.write(warc_header_fix(headers)+'\r\n')
              sys.stdout.write(entry+'\r\n')
            elif response.status_code == 200:
              write_warcs(response, sys.stdout)
            elif first_tweet:
              # It's the first tweet in the conversation, but it's truncated, and retrieval from
              # the Twitter API failed. Use the original data from the input WARC instead.
              logging.warn('{}/{}: Twitter API error {} on old tweet. Using original data instead.'
                           .format(file_num, entry_num, response.status_code))
              sys.stdout.write(warc_header_fix(headers)+'\r\n')
              sys.stdout.write(entry+'\r\n')
            else:
              # It's an earlier tweet in the conversation, but retrieval from the Twitter API
              # failed. All we can do is print the error response.
              logging.warn('{}/{}: Twitter API error {}.'
                           .format(file_num, entry_num, response.status_code))
              write_warcs(response, sys.stdout)
          first_tweet = False
        if args.output == 'warc':
          sys.stdout.write('\r\n')
        else:
          print()
      if remaining <= 0:
        break
    if remaining <= 0:
      break
  logging.info('Empties: {}'.format(empties))
  logging.info('Skipped: '+str(sum(done.values())))


def warc_header_fix(headers):
  """The WARCs holding the original tweets lack a WARC-Record-Id."""
  headers_dict = warc_simple.headers_to_dict(headers)
  if 'WARC-Record-ID' not in headers_dict:
    headers += 'WARC-Record-ID: <urn:uuid:{}>\r\n'.format(uuid.uuid4())
  return headers


def write_warcs(response, destination=sys.stdout):
  response_warc = make_warc_from_response(response)
  record_id = response_warc.header['WARC-Record-Id']
  request_warc = make_warc_from_request(response.request, record_id)
  request_warc.write_to(destination)
  response_warc.write_to(destination)


def make_warc_from_response(response):
  warc_headers_dict = {'WARC-Type':'response',
                       'WARC-Target-URI':response.request.url}
  warc_headers = warc.WARCHeader(warc_headers_dict, defaults=True)

  raw_response_headers = 'HTTP/1.1 {} {}\r\n'.format(response.status_code, response.reason)
  for header, value in response.headers.items():
    raw_response_headers += '{}: {}\r\n'.format(header, value)

  payload = raw_response_headers+'\r\n'+response.content
  return warc.WARCRecord(warc_headers, payload)


def make_warc_from_request(request, response_id):
  warc_headers_dict = {'WARC-Type':'request',
                       'WARC-Concurrent-To':response_id,
                       'WARC-Target-URI':request.url}
  warc_headers = warc.WARCHeader(warc_headers_dict, defaults=True)

  raw_request_headers = '{} {} HTTP/1.1\r\n'.format(request.method, request.path_url)
  raw_request_headers += 'Host: {}\r\n'.format(urlparse.urlparse(request.url)[1])
  for header, value in request.headers.items():
    raw_request_headers += '{}: {}\r\n'.format(header, value)

  return warc.WARCRecord(warc_headers, raw_request_headers)


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


def json_pretty_format(jobj):
  return json.dumps(jobj, sort_keys=True, indent=2, separators=(',', ': ')).encode('utf-8')


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
