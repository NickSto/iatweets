#!/usr/bin/env python
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals
import sys
import json
import logging
import argparse


def parse_warc(warc_path):
  """Usage:
  import parse_warc
  for tweet in parse_warc.parse_warc('path/to/filename.warc'):
    # "tweet" is a JSON object.
    print tweet.location
  """
  tweet_json = ''
  header = False
  with open(warc_path, 'rU') as warc:
    for line in warc:
      if header:
        if line.startswith('Content-Length:'):
          header = False
        continue
      else:
        if line == 'WARC/1.0\n':
          header = True
          if tweet_json:
            tweet = json.loads(tweet_json)
            yield tweet
          tweet_json = ''
          continue
      tweet_json += line


def main(argv):
  tweets = []
  for tweet in parse_warc(argv[1]):
    tweets.append(tweet)
  json.dump(tweets, sys.stdout)

if __name__ == '__main__':
  sys.exit(main(sys.argv))
