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
         'Also includes special columns: empty, id, user, text, truncated, in_reply_to_status_id, '
         'in_reply_to_screen_name, is_retweet, retweeted_id, retweeted_text, user_mentions, '
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
      columns_dict.update(extract_tweet(payload, already_json=True, empty_empties=False))
      if columns_dict.get('text'):
        columns_dict['text'] = columns_dict['text'].replace('\n', '\\n')
      columns_dict['filename'] = warc_path
      columns_dict['tweet_num'] = tweet_num
      try:
        print(outfmt.format(**columns_dict))
      except KeyError as ke:
        fail('Invalid column name "{}" given with --columns. Failed on tweet {}.'
             .format(ke[0], tweet_num))


def extract_tweet(entry_raw, already_json=False, empty_empties=True):
  """Figure out what kind of Twitter API object this is, and, if possible, extract
  the data we need in a standard data format."""
  if already_json:
    entry = entry_raw
  else:
    try:
      entry = json.loads(entry_raw)
    except ValueError:
      logging.critical('Content ({}): "{}.."'.format(type(entry_raw).__name__, entry_raw[:90]))
      raise
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
  if 'full_text' in status:
    text = status.get('full_text')
  else:
    text = status.get('text')
  if text is not None:
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
  return {'empty':empty,
          'id':status.get('id'),
          'user':user.get('screen_name'),
          'screen_name':user.get('screen_name'),
          'truncated':status.get('truncated'),
          'in_reply_to_status_id':status.get('in_reply_to_status_id'),
          'in_reply_to_screen_name':status.get('in_reply_to_screen_name'),
          'is_retweet':bool(retweeted_status),
          'retweeted_id':retweeted_id,
          'retweeted_text':retweeted_text,
          'retweeted_user':retweeted_user,
          'user_mentions':user_mentions,
          'text':text}


def get_user_and_status(entry):
  if 'user' in entry:
    # It's a status type of entry.
    return entry, entry['user']
  elif 'status' in entry:
    # It's a profile type of entry.
    return entry['status'], entry
  else:
    return None, None


def format_tweet_for_humans(raw_tweet, replied_by, file_num, entry_num):
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
    # Note: 'full_text' is needed instead of 'text' in order to get new-style tweets over 140
    # characters, including @mentions and links:
    # https://dev.twitter.com/overview/api/upcoming-changes-to-tweets
    if data_type == 'request':
      content = tweet['full_text'].encode('utf-8')
    else:
      content = tweet['text'].decode('utf-8').encode('utf-8')
    try:
      output += '{}/{}: {}\n'.format(file_num, entry_num, get_tweet_url(tweet, 'json'))
    except ValueError:
      logging.critical('{}/{}:'.format(file_num, entry_num))
      raise
    try:
      output += content+'\n'
    except UnicodeDecodeError:
      logging.error('{}/{}: data_type: {}, content: {}'
                    .format(file_num, entry_num, data_type, content))
      raise
    if tweet['in_reply_to_status_id']:
      output += 'A reply to: {}\n'.format(get_in_reply_to_url(tweet))
    if replied_by:
      output += 'Replied by: '+get_tweet_url(replied_by)+'\n'
    output += 'Looks truncated: {}\n'.format(does_tweet_look_truncated(tweet))
  except KeyError as ke:
    logging.warn('{}/{}: Error in tweet data converted from {}: JSON is missing key "{}".\n '
                 'Tweet: {}..'.format(file_num, entry_num, data_type, ke[0], json.dumps(tweet))[:200])
    raise
  return output


def get_tweet_url(raw_tweet, data_type=None):
  if data_type is None:
    # Detect the type of raw_tweet, if it wasn't specified.
    if isinstance(raw_tweet, dict):
      data_type = 'json'
    elif isinstance(raw_tweet, requests.models.Response):
      data_type = 'request'
    elif isinstance(raw_tweet, basestring):
      data_type = 'json_str'
    else:
      raise ValueError('Object of unsupported type ({}) given to format_tweet_for_humans().'
                       .format(type(raw_tweet).__name__))
  if data_type == 'json':
    if 'screen_name' in raw_tweet:
      return 'https://twitter.com/{screen_name}/status/{id}'.format(**raw_tweet)
    else:
      return 'https://twitter.com/{}/status/{}'.format(raw_tweet['user']['screen_name'], raw_tweet['id'])
  elif data_type == 'request':
    tweet_json = raw_tweet.json()
  elif data_type == 'json_str':
    tweet_json = json.loads(raw_tweet)
  screen_name = tweet_json.get('user', {}).get('screen_name')
  id = tweet_json.get('id')
  return 'https://twitter.com/{}/status/{}'.format(screen_name, id)


def get_in_reply_to_url(tweet):
  return ('https://twitter.com/{in_reply_to_screen_name}/status/{in_reply_to_status_id}'
          .format(**tweet))


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
