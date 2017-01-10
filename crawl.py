#!/usr/bin/env python
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
# from __future__ import unicode_literals
import sys
import json
import errno
import logging
import argparse
import parse_warc

ARG_DEFAULTS = {'log':sys.stderr, 'volume':logging.ERROR}
DESCRIPTION = """This script will eventually read a series of tweets, then crawl Twitter to gather
replies and other information related to them. Currently, it just parses and prints the tweets."""


def main(argv):

  parser = argparse.ArgumentParser(description=DESCRIPTION)
  parser.set_defaults(**ARG_DEFAULTS)

  parser.add_argument('warcs', metavar='path/to/record.warc', nargs='+',
    help='The uncompressed WARC file(s).')
  parser.add_argument('-l', '--log', type=argparse.FileType('w'),
    help='Print log messages to this file instead of to stderr. Warning: Will overwrite the file.')
  parser.add_argument('-q', '--quiet', dest='volume', action='store_const', const=logging.CRITICAL)
  parser.add_argument('-v', '--verbose', dest='volume', action='store_const', const=logging.INFO)
  parser.add_argument('-D', '--debug', dest='volume', action='store_const', const=logging.DEBUG)

  args = parser.parse_args(argv[1:])

  logging.basicConfig(stream=args.log, level=args.volume, format='%(message)s')
  tone_down_logger()

  empties = 0
  entry_num = 0
  for warc_path in args.warcs:
    for entry in parse_warc.parse_warc(warc_path):
      entry_num += 1
      if 'user' in entry:
        # It's a tweet type of entry.
        print('https://twitter.com/{}/status/{}'.format(entry['user']['screen_name'], entry['id']))
        print(entry['text'].encode('utf-8'))
        if entry['in_reply_to_status_id_str']:
          print('Reply:  https://twitter.com/{}/status/{}'.format(
                entry['in_reply_to_screen_name'], entry['in_reply_to_status_id_str']))
      elif 'status' in entry:
        # It's a profile type of entry.
        print('https://twitter.com/{}/status/{}'.format(entry['screen_name'], entry['status']['id']))
        print(entry['status']['text'].encode('utf-8'))
        if entry['status']['in_reply_to_status_id_str']:
          print('Reply:  https://twitter.com/{}/status/{}'.format(
                entry['status']['in_reply_to_screen_name'], entry['status']['in_reply_to_status_id_str']))
      else:
        # It's a profile with no attached tweet.
        empties += 1
        logging.info(json_pretty_format(entry))
      print()
  print('Empties: {}'.format(empties))


def json_pretty_format(jobj):
  return json.dumps(jobj, sort_keys=True, indent=2, separators=(',', ': ')).encode('utf-8')


def tone_down_logger():
  """Change the logging level names from all-caps to capitalized lowercase.
  E.g. "WARNING" -> "Warning" (turn down the volume a bit in your log files)"""
  for level in (logging.CRITICAL, logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG):
    level_name = logging.getLevelName(level)
    logging.addLevelName(level, level_name.capitalize())


def fail(message):
  sys.stderr.write(message+"\n")
  sys.exit(1)


if __name__ == '__main__':
  try:
    sys.exit(main(sys.argv))
  except IOError as ioe:
    if ioe.errno != 32:
      raise
