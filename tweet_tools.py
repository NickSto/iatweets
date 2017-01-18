#!/usr/bin/env python
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import json
import logging
import requests


def extract_tweet(entry_raw):
  """Figure out what kind of Twitter API object this is, and, if possible, extract
  the data we need in a standard data format."""
  #TODO: Just skip profile types, since I think the point of those isn't to actually contain tweets.
  #      And the tweets they do contain may be duplicates of others in the archive.
  entry = json.loads(entry_raw)
  if 'user' in entry:
    # It's a tweet type of entry.
    return {'id':entry['id'],
            'truncated':entry['truncated'],
            'screen_name':entry['user']['screen_name'],
            'in_reply_to_status_id':entry.get('in_reply_to_status_id'),
            'in_reply_to_screen_name':entry.get('in_reply_to_screen_name'),
            'text':entry['text'].encode('utf-8')}
  elif 'status' in entry:
    # It's a profile type of entry.
    return {'id':entry['status']['id'],
            'truncated':entry['status']['truncated'],
            'screen_name':entry['screen_name'],
            'in_reply_to_status_id':entry['status'].get('in_reply_to_status_id'),
            'in_reply_to_screen_name':entry['status'].get('in_reply_to_screen_name'),
            'text':entry['status']['text'].encode('utf-8')}
  else:
    # It's a profile with no attached tweet (or something else).
    return None


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


def fail(message):
  logging.critical(message)
  sys.exit(1)
