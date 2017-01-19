#!/usr/bin/env python
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import sys
import json
import errno
import logging
import argparse
import requests
import warc_simple


ARG_DEFAULTS = {'columns':'WARC-Target-URI,screen_name,id,text', 'log':sys.stderr, 'volume':logging.ERROR}
DESCRIPTION = """"""


def make_argparser():

  parser = argparse.ArgumentParser(description=DESCRIPTION)
  parser.set_defaults(**ARG_DEFAULTS)

  parser.add_argument('warcs', metavar='path/to/record.warc', nargs='+',
    help='Un-gzipped WARC files.')
  parser.add_argument('-c', '--columns',
    help='Output columns, comma-delimited. Names are WARC headers or fields from the tweet JSON. '
         'Default: %(default)s')
  parser.add_argument('-i', '--ignore-empties', action='store_true')
  parser.add_argument('-L', '--log', type=argparse.FileType('w'),
    help='Print log messages to this file instead of to stderr. Warning: Will overwrite the file.')
  parser.add_argument('-q', '--quiet', dest='volume', action='store_const', const=logging.CRITICAL)
  parser.add_argument('-v', '--verbose', dest='volume', action='store_const', const=logging.INFO)
  parser.add_argument('-D', '--debug', dest='volume', action='store_const', const=logging.DEBUG)

  return parser


def main(argv):

  parser = make_argparser()
  args = parser.parse_args(argv[1:])

  outfmt = '{'+'}\t{'.join(args.columns.split(','))+'}'

  logging.basicConfig(stream=args.log, level=args.volume, format='%(message)s')
  tone_down_logger()

  for warc_path in args.warcs:
    for payload, headers in warc_simple.parse(warc_path, payload_json=True, header_dict=True):
      if not payload and args.ignore_empties:
        continue
      columns_dict = extract_tweet(payload, already_json=True, empty_empties=False)
      if columns_dict['text']:
        columns_dict['text'] = columns_dict['text'].replace('\n', '\\n')
      columns_dict.update(headers)
      print(outfmt.format(**columns_dict))


def extract_tweet(entry_raw, already_json=False, empty_empties=True):
  """Figure out what kind of Twitter API object this is, and, if possible, extract
  the data we need in a standard data format."""
  if already_json:
    entry = entry_raw
  else:
    try:
      entry = json.loads(entry_raw)
    except ValueError:
      logging.critical('Content: {}'.format(type(entry_raw).__name__, entry_raw[:90]))
      raise
  # Find the user and status objects in the entry.
  if 'user' in entry:
    # It's a status type of entry.
    status = entry
    user = entry['user']
  elif 'status' in entry:
    # It's a profile type of entry.
    status = entry['status']
    user = entry
  else:
    # It's a profile with no attached tweet (or something else).
    if empty_empties:
      return None
    status = {}
    user = {}
  # Get and utf8-encode the status text.
  if 'full_text' in status:
    text = status.get('full_text')
  else:
    text = status.get('text')
  if text is not None:
    text = text.encode('utf-8')
  # Construct the return data structure.
  return {'id':status.get('id'),
          'truncated':status.get('truncated'),
          'screen_name':user.get('screen_name'),
          'in_reply_to_status_id':status.get('in_reply_to_status_id'),
          'in_reply_to_screen_name':status.get('in_reply_to_screen_name'),
          'text':text}


def get_status(entry):
  if 'user' in entry:
    return entry
  elif 'status' in entry:
    return entry['status']
  else:
    return None


def format_tweet_for_humans(raw_tweet, file_num, entry_num):
  output = ''
  if isinstance(raw_tweet, dict):
    data_type = 'json'
    tweet = raw_tweet
  elif isinstance(raw_tweet, requests.models.Response):
    data_type = 'request'
    tweet = raw_tweet.json()
  elif isinstance(raw_tweet, basestring):
    data_type = 'json_str'
    tweet = json.loads(raw_tweet)
  else:
    fail('{}/{}: Object of unsupported type ({}) given to format_tweet_for_humans().'
         .format(file_num, entry_num, type(raw_tweet)))
  try:
    if data_type == 'request':
      screen_name = tweet['user']['screen_name']
    else:
      screen_name = tweet['screen_name']
    id = tweet['id']
    # Note: 'full_text' is needed instead of 'text' in order to get new-style tweets over 140
    # characters, including @mentions and links:
    # https://dev.twitter.com/overview/api/upcoming-changes-to-tweets
    if data_type == 'request':
      content = tweet['full_text'].encode('utf-8')
    else:
      content = tweet['text'].decode('utf-8').encode('utf-8')
    output += '{}/{}: https://twitter.com/{}/status/{}\n'.format(file_num, entry_num, screen_name, id)
    try:
      output += content
    except UnicodeDecodeError:
      logging.error('{}/{}: data_type: {}, content: {}'
                    .format(file_num, entry_num, data_type, content))
      raise
    if tweet['in_reply_to_status_id']:
      output += ('\nA reply to: https//twitter.com/{in_reply_to_screen_name}/status/'
                 '{in_reply_to_status_id}'.format(**tweet))
    output += '\nLooks truncated: {}'.format(does_tweet_look_truncated(tweet))
  except KeyError as ke:
    logging.warn('{}/{}: Error in tweet data converted from {}: JSON is missing key "{}".\n '
                 'Tweet: {}..'.format(file_num, entry_num, data_type, ke[0], json.dumps(tweet))[:200])
    raise
  return output


def does_tweet_look_truncated(tweet):
  """Returns True if the tweet doesn't contain 'full_text' and contains the \u2026
  (horizontal ellipsis) character."""
  # If 'full_text' is in there, that's using the new, extended mode. It's the whole thing.
  if 'full_text' in tweet:
    return False
  content = tweet['text'].decode('utf-8')
  if u'\u2026' in content:
    return True
  else:
    return False


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
