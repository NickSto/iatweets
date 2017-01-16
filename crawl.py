#!/usr/bin/env python
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import sys
import json
import errno
import logging
import argparse
import ConfigParser
import parse_warc

KEY_NAMES = ('consumer_key', 'consumer_secret', 'access_token_key', 'access_token_secret')
ARG_DEFAULTS = {'log':sys.stderr, 'volume':logging.WARNING}
DESCRIPTION = """This script will read a series of tweets from unzipped WARC files, then use the
Twitter API to re-retrieve them (to get the full, un-truncated text) and gather replies and other
information related to them."""
EPILOG = """Requires the python-twitter module in order to interact with Twitter:
https://pypi.python.org/pypi/python-twitter/"""


def main(argv):

  parser = argparse.ArgumentParser(description=DESCRIPTION, epilog=EPILOG)
  parser.set_defaults(**ARG_DEFAULTS)

  parser.add_argument('warcs', metavar='path/to/record.warc', nargs='+',
    help='The uncompressed WARC file(s).')
  parser.add_argument('-p', '--parse-tweets', action='store_true',
    help='Just parse the tweets from the WARC files and print them out. '
         'No Twitter API keys required.')
  parser.add_argument('-O', '--oauth-file',
    help='A config file containing the OAuth keys. For obtaining these from Twitter, see '
         'https://python-twitter.readthedocs.io/en/latest/getting_started.html')
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
      for key_name in key_names:
        key = getattr(args, key_name)
        if key:
          keys[key_name] = key
        else:
          fail('All four OAuth tokens must be given unless --parse-tweets is.')
    try:
      import twitter
    except ImportError:
      fail('Interacting with Twitter requires the python-twitter module: '
           'https://pypi.python.org/pypi/python-twitter/')
    api = twitter.Api(tweet_mode='extended', sleep_on_rate_limit=True, **keys)

  empties = 0
  entry_num = 0
  for warc_path in args.warcs:
    for entry in parse_warc.parse_warc(warc_path, payload_json=True, omit_headers=True):
      entry_num += 1
      tweet = extract_tweet(entry)
      if not tweet:
        empties += 1
        logging.debug(json_pretty_format(entry))
        continue
      if args.parse_tweets:
        print('https://twitter.com/{screen_name}/status/{id}'.format(**tweet))
        print(tweet['text'])
        if tweet['in_reply_to_status_id']:
          print('A reply to: https//twitter.com/{in_reply_to_screen_name}/status/'
                '{in_reply_to_status_id}'.format(**tweet))
        print()
      else:
        # Use the Twitter API to re-retrieve this tweet (to get the full text), and the full reply
        # chain if it was a reply.
        #TODO: Check if it's actually truncated, and if not, just use the original tweet data.
        reply_chain = get_replied_tweets(tweet['id'], api, remaining=remaining)
        remaining -= len(reply_chain)
        if tweet['in_reply_to_status_id']:
          logging.info('Reply tweet; retrieved {} in reply chain.'.format(len(reply_chain)))
        for reply in reply_chain:
          # Note: 'full_text' is needed instead of 'text' in order to get new-style tweets over
          # 140 characters, including @mentions and links:
          # https://dev.twitter.com/overview/api/upcoming-changes-to-tweets
          print('https://twitter.com/{}/status/{}'.format(reply.user.screen_name, reply.id))
          print(reply.full_text.encode('utf-8'))
        print()
      if remaining <= 0:
        break
    if remaining <= 0:
      break
  logging.info('Empties: {}'.format(empties))


def extract_tweet(entry):
  """Figure out what kind of Twitter API object this is, and, if possible, extract
  the data we need in a standard data format."""
  #TODO: Just skip profile types, since I think the point of those isn't to actually contain tweets.
  #      And the tweets they do contain may be duplicates of others in the archive.
  if 'user' in entry:
    # It's a tweet type of entry.
    return {'id':entry['id'],
            'screen_name':entry['user']['screen_name'],
            'in_reply_to_status_id':entry.get('in_reply_to_status_id'),
            'in_reply_to_screen_name':entry.get('in_reply_to_screen_name'),
            'text':entry['text'].encode('utf-8')}
  elif 'status' in entry:
    # It's a profile type of entry.
    return {'id':entry['status']['id'],
            'screen_name':entry['screen_name'],
            'in_reply_to_status_id':entry['status'].get('in_reply_to_status_id'),
            'in_reply_to_screen_name':entry['status'].get('in_reply_to_screen_name'),
            'text':entry['status']['text'].encode('utf-8')}
  else:
    # It's a profile with no attached tweet (or something else).
    return None


def get_replied_tweets(id, api, remaining=None):
  reply_chain = []
  while id:
    if remaining is None or remaining > 0:
      tweet = api.GetStatus(id)
      remaining -= 1
    else:
      logging.warn('--limit exceeded when there were tweets from a conversation remaining to be '
                   'requested.')
      break
    id = tweet.in_reply_to_status_id
    reply_chain.append(tweet)
  return reply_chain


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
      fail('OAuth token "{}" not found in --oauth-file "{}".'.format(key_name, config_file))
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
