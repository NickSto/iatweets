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


ARG_DEFAULTS = {'columns':'WARC-Target-URI,user,id,text', 'log':sys.stderr, 'volume':logging.ERROR}
DESCRIPTION = """"""


def make_argparser():

  parser = argparse.ArgumentParser(description=DESCRIPTION)
  parser.set_defaults(**ARG_DEFAULTS)

  parser.add_argument('warcs', metavar='path/to/record.warc', nargs='+',
    help='Un-gzipped WARC files.')
  parser.add_argument('-c', '--columns',
    help='Output columns, comma-delimited. Names are WARC headers or fields from the tweet JSON. '
         'Also includes special columns: empty, id, user, text, truncated, in_reply_to_id, '
         'in_reply_to_user, is_retweet, retweeted_id, retweeted_text, user_mentions, '
         'filename, tweet_num. Default: %(default)s')
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

  columns = args.columns.split(',')
  outfmt = '{'+'}\t{'.join(columns)+'}'
  warc_headers_dict = {}
  for column in columns:
    if column.startswith('WARC-'):
      warc_headers_dict[column] = None

  logging.basicConfig(stream=args.log, level=args.volume, format='%(message)s')
  tone_down_logger()

  tweet_num = 0
  for warc_path in args.warcs:
    for payload, headers in warc_simple.parse(warc_path, payload_json=True, header_dict=True):
      tweet_num += 1
      if not payload and args.ignore_empties:
        continue
      columns_dict = warc_headers_dict.copy()
      columns_dict.update(headers)
      columns_dict.update(extract_tweet(payload, datatype='json', empty_empties=False))
      if columns_dict.get('text'):
        columns_dict['text'] = columns_dict['text'].replace('\n', '\\n')
      columns_dict['filename'] = warc_path
      columns_dict['tweet_num'] = tweet_num
      try:
        print(outfmt.format(**columns_dict))
      except KeyError as ke:
        fail('Invalid column name "{}" given with --columns. Failed on tweet {}.'
             .format(ke[0], tweet_num))


def extract_tweet(entry_raw, datatype=None, empty_empties=True):
  """Figure out what kind of Twitter API object this is, and, if possible, extract
  the data we need in a standard data format."""
  if datatype is None:
    datatype = detect_datatype(entry_raw)
  if datatype == 'json':
    entry = entry_raw
    # Check if it's already an extracted tweet.'
    if 'user_mentions' in entry and 'id' in entry and 'screen_name' in entry:
      return entry
  elif datatype == 'request':
    entry = entry_raw.json()
  elif datatype == 'json_str':
    try:
      entry = json.loads(entry_raw)
    except ValueError:
      logging.critical('Content ({}): "{}.."'.format(type(entry_raw).__name__, entry_raw[:90]))
      raise
  is_profile = 'status' in entry and 'screen_name' in entry
  # Find the user and status objects in the entry.
  status, user = get_user_and_status(entry)
  if status is None and user is None:
    # It's a profile with no attached tweet (or something else).
    empty = True
    if empty_empties:
      return None
    status = {}
    user = {}
  else:
    empty = False
  # Get and utf8-encode the status text.
  if datatype == 'request':
    text = status.get('full_text')
    if text:
      text = text.encode('utf-8')
  elif 'full_text' in status:
    text = status.get('full_text')
  elif datatype == 'json_str':
    text = status.get('text')
  else:
    text = status.get('text')
    if text:
      text = text.encode('utf-8')
  # If it's a retweet, get data about the original tweet.
  retweeted_status = status.get('retweeted_status')
  if retweeted_status:
    retweeted_id = retweeted_status.get('id')
    retweeted_text = retweeted_status.get('full_text') or retweeted_status.get('text')
    if retweeted_text:
      retweeted_text = retweeted_text.encode('utf-8')
  else:
    retweeted_id = None
    retweeted_text = None
  retweeted_user = None
  # Get users @mentioned by the tweet.
  mention_entities = status.get('entities', {}).get('user_mentions')
  if mention_entities:
    user_mentions_list = []
    for entity in mention_entities:
      user_mentions_list.append(entity.get('screen_name'))
      if retweeted_status and entity['indices'][0] == 3:
        # This may not always be correct. It's assuming all retweets start with "RT @user:".
        retweeted_user = entity.get('screen_name')
    user_mentions = ','.join(user_mentions_list)
  else:
    user_mentions = None
  # Construct the return data structure.
  tweet = {'empty':empty,
           'id':status.get('id'),
           'user':user.get('screen_name'),
           'screen_name':user.get('screen_name'),
           'description':user.get('description'),
           'is_profile':is_profile,
           'truncated':status.get('truncated'),
           'in_reply_to_id':status.get('in_reply_to_status_id'),
           'in_reply_to_user':status.get('in_reply_to_screen_name'),
           'in_reply_to_status_id':status.get('in_reply_to_status_id'),
           'in_reply_to_screen_name':status.get('in_reply_to_screen_name'),
           'is_retweet':bool(retweeted_status),
           'retweeted_id':retweeted_id,
           'retweeted_text':retweeted_text,
           'retweeted_user':retweeted_user,
           'user_mentions':user_mentions,
           'text':text}
  if is_profile:
    tweet['in_reply_to_id'] = None
    tweet['in_reply_to_user'] = None
    tweet['in_reply_to_status_id'] = None
    tweet['in_reply_to_screen_name'] = None
    tweet['is_retweet'] = None
    tweet['retweeted_id'] = None
    tweet['retweeted_text'] = None
    tweet['retweeted_user'] = None
  return tweet


def get_user_and_status(entry):
  if 'user' in entry:
    # It's a status type of entry.
    return entry, entry['user']
  elif 'status' in entry:
    # It's a profile type of entry.
    return entry['status'], entry
  else:
    return None, None


def format_tweet_for_humans(tweet_data, file_num, entry_num):
  output = ''
  tweet = tweet_data.get('tweet')
  try:
    if tweet['is_profile']:
      output += '{}/{}: Profile: @{}\n'.format(file_num, entry_num, tweet.get('user'))
      output += tweet.get('description')+'\n'
      return output.encode('utf-8')
    # Note: 'full_text' is needed instead of 'text' in order to get new-style tweets over 140
    # characters, including @mentions and links:
    # https://dev.twitter.com/overview/api/upcoming-changes-to-tweets
    if 'full_text' in tweet:
      content = tweet['full_text']
    else:
      content = tweet['text']
    try:
      output += '{}/{}: {}\n'.format(file_num, entry_num, get_tweet_url(tweet, 'this'))
    except ValueError:
      logging.critical('{}/{}:'.format(file_num, entry_num))
      raise
    try:
      output += content+'\n'
    except UnicodeDecodeError:
      logging.error('{}/{}: content: {}'
                    .format(file_num, entry_num, content))
      raise
    if tweet['in_reply_to_id']:
      output += 'A reply to: '+get_tweet_url(tweet, 'reply_to')+'\n'
    if tweet_data.get('replied_by_id'):
      output += 'Replied by: '+get_tweet_url(tweet_data, 'replied_by')+'\n'
    if tweet_data.get('retweeted_by_id'):
      output += 'Retweeted by: '+get_tweet_url(tweet_data, 'retweeted_by')+'\n'
    output += 'Looks truncated? {}\n'.format(does_tweet_look_truncated(tweet))
  except KeyError as ke:
    logging.warn('{}/{}: Error in tweet data: JSON is missing key "{}".\n '
                 'Tweet: {}..'.format(file_num, entry_num, ke[0], json.dumps(tweet))[:200])
    raise
  try:
    return output.encode('utf-8')
  except UnicodeDecodeError:
    return output


def get_tweet_url(tweet_data, urltype):
  if urltype == 'this':
    user = tweet_data['user']
    id = tweet_data['id']
  elif urltype == 'reply_to':
    user = tweet_data['in_reply_to_user']
    id = tweet_data['in_reply_to_id']
  elif urltype == 'replied_by':
    user = tweet_data['replied_by_user']
    id = tweet_data['replied_by_id']
  elif urltype == 'retweeted_by':
    user = tweet_data['retweeted_by_user']
    id = tweet_data['retweeted_by_id']
  return 'https://twitter.com/{}/status/{}'.format(user, id)


def does_tweet_look_truncated(tweet):
  """Returns True if the tweet doesn't contain 'full_text' and contains the \u2026
  (horizontal ellipsis) character."""
  # If 'full_text' is in there, that's using the new, extended mode. It's the whole thing.
  if 'full_text' in tweet:
    return False
  try:
    looks_truncated = u'\u2026' in tweet['text']
  except UnicodeDecodeError:
    looks_truncated = u'\u2026' in tweet['text'].decode('utf-8')
  return looks_truncated


def detect_datatype(raw_tweet):
  if isinstance(raw_tweet, dict):
    return 'json'
  elif isinstance(raw_tweet, requests.models.Response):
    return 'request'
  elif isinstance(raw_tweet, basestring):
    return 'json_str'
  else:
    raise ValueError('Object of unsupported type {}'.format(type(raw_tweet).__name__))


def json_pretty_format(jobj):
  return json.dumps(jobj, sort_keys=True, indent=2, separators=(',', ': ')).encode('utf-8')


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
