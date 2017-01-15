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
ARG_DEFAULTS = {'log':sys.stderr, 'volume':logging.ERROR}
DESCRIPTION = """This script will read a series of tweets, then crawl Twitter to gather replies and
other information related to them."""


def main(argv):

  parser = argparse.ArgumentParser(description=DESCRIPTION)
  parser.set_defaults(**ARG_DEFAULTS)

  parser.add_argument('warcs', metavar='path/to/record.warc', nargs='+',
    help='The uncompressed WARC file(s).')
  parser.add_argument('-O', '--oauth-file',
    help='A config file containing the OAuth keys.')
  parser.add_argument('-c', '--consumer-key')
  parser.add_argument('-C', '--consumer-secret')
  parser.add_argument('-a', '--access-token-key')
  parser.add_argument('-A', '--access-token-secret')
  parser.add_argument('-p', '--parse-tweets', action='store_true',
    help='Just parse the tweets from the WARC files and print them out. '
         'No Twitter API keys required.')
  parser.add_argument('-l', '--log', type=argparse.FileType('w'),
    help='Print log messages to this file instead of to stderr. Warning: Will overwrite the file.')
  parser.add_argument('-q', '--quiet', dest='volume', action='store_const', const=logging.CRITICAL)
  parser.add_argument('-v', '--verbose', dest='volume', action='store_const', const=logging.INFO)
  parser.add_argument('-D', '--debug', dest='volume', action='store_const', const=logging.DEBUG)

  args = parser.parse_args(argv[1:])

  logging.basicConfig(stream=args.log, level=args.volume, format='%(message)s')
  tone_down_logger()

  keys = {}
  if args.oauth_file:
    keys = read_oauth_config(args.oauth_file, KEY_NAMES)
  else:
    for key_name in key_names:
      key = getattr(args, key_name)
      if key:
        keys[key_name] = key
      else:
        keys = {}
        break

  if keys:
    import twitter
    api = twitter.Api(sleep_on_rate_limit=True, **keys)
  elif not args.parse_tweets:
    fail('OAuth keys must be given if --parse-tweets isn\'t.')

  empties = 0
  entry_num = 0
  for warc_path in args.warcs:
    for entry in parse_warc.parse_warc(warc_path):
      entry_num += 1
      tweet = extract_tweet(entry)
      if not tweet:
        empties += 1
        logging.debug(json_pretty_format(entry))
        continue
      #TODO: The tweet text is often cut off. This seems to be related to:
      #      https://dev.twitter.com/overview/api/upcoming-changes-to-tweets
      #      It looks like they're going to (already have?) stop counting things like links, media,
      #      and usernames toward the 140 character limit, so tweets will actually be (already are?)
      #      longer than 140 characters with all that stuff included.
      #      Maybe these tweets were fetched with an older, backward compatible API that keeps all
      #      that stuff in the tweet text, but truncates it to 140 characters with an ellipsis.
      #      Figure out if we can use a different API to re-fetch the entire tweet.
      if args.parse_tweets:
        print('https://twitter.com/{}/status/{}'.format(tweet['screen_name'], tweet['id']))
        print(tweet['text'])
        reply_chain = get_replied_tweets(tweet['in_reply_to_status_id'], api)
        for reply in reply_chain:
          print('Replied: https://twitter.com/{}/status/{}'.format(reply.user.screen_name, reply.id))
          print(reply.text.encode('utf-8'))
        print()
  logging.info('Empties: {}'.format(empties))


def extract_tweet(entry):
  """Figure out what kind of Twitter API object this is, and, if possible, extract
  the data we need in a standard data format."""
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


def get_replied_tweets(id, api):
  reply_chain = []
  while id:
    tweet = api.GetStatus(id)
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
