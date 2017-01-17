#!/usr/bin/env python
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import sys
import json
import errno
import logging
import argparse
import collections
"""
This module parses WARC-like files with few restrictions.
Any file should work, if it consists of one or more WARC records, each beginning with a version
number line like "WARC/1.0". The headers must end with one blank line.
The version number is the only required header (for parsing).
"""

ARG_DEFAULTS = {'log':sys.stderr, 'llevel':logging.ERROR}
DESCRIPTION = """Prints all tweets as a list of JSON objects.
If multiple WARC files are given, prints a list of them, as JSON of this format:
[
  {
    "path":"path/to/file1.warc", "tweets":[{tweet1..},{tweet2...}]
  }
  {
    "path":"path/to/file2.warc", "tweets":[{tweet1..},{tweet2...}]
  }
]
"""


# Note: the problem with the tweet WARCs is that they lack a WARC-Record-ID header.
# Looks like it should be a UUID.
def main(argv):

  parser = argparse.ArgumentParser(description=DESCRIPTION)
  parser.set_defaults(**ARG_DEFAULTS)

  parser.add_argument('warcs', metavar='path/to/record.warc', nargs='+',
    help='Un-gzipped WARC files.')
  parser.add_argument('-l', '--list', action='store_true',
    help='Just print a list of tweets as independent JSON objects, one per line.')
  parser.add_argument('-L', '--log', type=argparse.FileType('w'),
    help='Print log messages to this file instead of to stderr. Warning: Will overwrite the file.')
  parser.add_argument('-q', '--quiet', dest='llevel', action='store_const', const=logging.CRITICAL)
  parser.add_argument('-v', '--verbose', dest='llevel', action='store_const', const=logging.INFO)
  parser.add_argument('-D', '--debug', dest='log_level', action='store_const', const=logging.DEBUG)

  args = parser.parse_args(argv[1:])

  logging.basicConfig(stream=args.log, level=args.llevel, format='%(message)s')
  tone_down_logger()

  tweet_files = []
  for path in args.warcs:
    tweets = list(parse_warc(path, payload_json=True, omit_headers=True))
    tweet_files.append({'path':path, 'tweets':tweets})

  if args.list:
    for tweets in tweet_files:
      for tweet in tweets['tweets']:
        json.dump(tweet, sys.stdout)
        print()
  else:
    if len(tweet_files) == 1:
      json.dump(tweet_files[0]['tweets'], sys.stdout)
    else:
      json.dump(tweet_files, sys.stdout)


def parse(warc_path, payload_json=False, header_dict=False, omit_headers=False):
  """Usage:
  import warc_simple
  for payload, headers in parse_warc.parse_warc('path/to/filename.warc'):
    print payload
  """
  headers = ''
  content = ''
  header = False
  with open(warc_path, 'rU') as warc:
    for line in warc:
      if header:
        if not line.rstrip('\r\n'):
          # The header ends at the first blank line.
          header = False
        else:
          headers += line
      else:
        if line.startswith('WARC/'):
          # Does the line look like the start of a WARC header? ("WARC/1.0")
          try:
            float(line[5:].rstrip('\r\n'))
            header = True
          except ValueError:
            pass
        if header:
          # We're starting a new record. Output the previous record, if any, and reset.
          if content:
            yield create_return_data(content, headers, payload_json, omit_headers, header_dict)
          headers = line
          content = ''
        else:
          content += line
    if content:
      yield create_return_data(content, headers, payload_json, omit_headers, header_dict)


def create_return_data(content, headers, payload_json, omit_headers, header_dict):
  if payload_json:
    payload = json.loads(content)
  else:
    payload = content
  if omit_headers:
    return payload
  else:
    if header_dict:
      header_payload = headers_to_dict(headers)
    else:
      header_payload = headers
    return payload, header_payload


def headers_to_dict(headers):
  header_dict = collections.OrderedDict()
  for header_line in headers.splitlines():
    fields = header_line.split(':')
    if header_line.startswith('WARC/') and len(fields) == 1:
      header_dict['__VERSION__'] = header_line
      continue
    assert len(fields) >= 2, header_line
    header = fields[0]
    value = ':'.join(fields[1:]).lstrip(' ')
    header_dict[header] = value
  return header_dict


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
    if ioe.errno != errno.EPIPE:
      raise
