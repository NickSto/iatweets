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
This module is a simplified parser for WARC-like files.
Any file should work, if it consists of one or more WARC records, each beginning with a version
number line like "WARC/1.0". The headers must end with one blank line.
The version number (any number will work) is the only required header (for parsing).
"""

HTTP_METHODS = ('get', 'post', 'head', 'put', 'delete', 'trace', 'options', 'connect', 'patch')
ARG_DEFAULTS = {'log':sys.stderr, 'volume':logging.ERROR}
DESCRIPTION = """This is a simplified parser for WARC-like files.
When run as a script, it will prints all WARC records as a list of JSON objects.
With the --one-object option, this will print all input files in JSON form:
[
  {
    "path":"path/to/file1.warc", "payloads":[{payload1..},{payload2...}]
  }
  {
    "path":"path/to/file2.warc", "payloads":[{payload1..},{payload2...}]
  }
]
"""


# For it to operate the old way, use $ ./warc_simple.py -oj --tweets
def main(argv):

  parser = argparse.ArgumentParser(description=DESCRIPTION)
  parser.set_defaults(**ARG_DEFAULTS)

  parser.add_argument('warcs', metavar='path/to/record.warc', nargs='+',
    help='Un-gzipped WARC files.')
  parser.add_argument('-j', '--json', action='store_true',
    help='Parse the payload of each WARC record as JSON. In this mode, HTTP headers will be '
         'ignored in records with a WARC-Type of "request" or "response". Records with no payload '
         'will be printed as an empty object ("{}")')
  parser.add_argument('-p', '--prettify', action='store_true',
    help='Print the JSON object(s) as pretty, whitespaced strings instead of compact ones.')
  parser.add_argument('-o', '--one-object', action='store_true',
    help='Print the contents of all files as one, big JSON object. This will consist of a JSON '
         'list of objects, one for each file. Each has two keys: "path", the path to the file as '
         'given on the command line, and "payloads", a list where each element is the payload of '
         'a WARC record.')
  parser.add_argument('--tweets', action='store_true',
    help='When printing multiple files as one, big JSON, label the payloads as "tweets" instead of '
         '"payloads", for backward compatibility.')
  parser.add_argument('-L', '--log', type=argparse.FileType('w'),
    help='Print log messages to this file instead of to stderr. Warning: Will overwrite the file.')
  parser.add_argument('-q', '--quiet', dest='volume', action='store_const', const=logging.CRITICAL)
  parser.add_argument('-v', '--verbose', dest='volume', action='store_const', const=logging.INFO)
  parser.add_argument('-D', '--debug', dest='volume', action='store_const', const=logging.DEBUG)

  args = parser.parse_args(argv[1:])

  logging.basicConfig(stream=args.log, level=args.volume, format='%(message)s')
  tone_down_logger()

  records = []
  for path in args.warcs:
    for record in parse(path, payload_json=args.json, omit_headers=True):
      if args.one_object:
        if args.tweets:
          payloads_key = 'tweets'
        else:
          payloads_key = 'payloads'
        records.append({'path':path, payloads_key:record})
      elif args.json:
        if args.prettify:
          json.dump(record, sys.stdout, sort_keys=True, indent=2, separators=(',', ': '))
        else:
          json.dump(record, sys.stdout)
        print('\n')
      else:
        print(record)
        print()

  if args.one_object:
    if args.prettify:
      json.dump(records, sys.stdout, sort_keys=True, indent=2, separators=(',', ': '))
    else:
      json.dump(records, sys.stdout)


def parse(warc_path, payload_json=False, header_dict=False, omit_headers=False):
  """Usage:
  If payload_json is False, this will return the WARC record content as a single string.
  If True, this will take the entire payload of the WARC record, parse it with json.loads(), and
  return it. If the WARC-Type is "request" or "response", though, the HTTP headers will be stripped
  out before parsing the content as JSON.
  If header_dict is False, the WARC headers will be returned as a single string.
  If True, the headers will be parsed into a dict of header:value pairs and returned.
  If omit_headers is True, this will only return one value per WARC record: the payload.
  import warc_simple
  for payload, headers in parse_warc.parse_warc('path/to/filename.warc'):
    print payload
  """
  headers = ''
  content = ''
  warc_type = None
  header = False
  with open(warc_path, 'rU') as warc:
    for line in warc:
      if header:
        if not line.rstrip('\r\n'):
          # The header ends at the first blank line.
          header = False
        else:
          if line.startswith('WARC-Type:'):
            fields = line.split(':')
            if len(fields) == 2:
              warc_type = fields[1].strip().lower()
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
            yield create_return_data(content, headers, warc_type, payload_json, omit_headers, header_dict)
          warc_type = None
          headers = line
          content = ''
        else:
          content += line
    if content:
      yield create_return_data(content, headers, warc_type, payload_json, omit_headers, header_dict)


def create_return_data(content, headers, warc_type, payload_json, omit_headers, header_dict):
  if payload_json:
    if warc_type == 'request' or warc_type == 'response':
      content = strip_http_headers(content, warc_type)
    if content:
      try:
        payload = json.loads(content)
      except ValueError:
        logging.critical('Payload: "{}"'.format(content[:130]))
        raise
    else:
      payload = {}
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


def strip_http_headers(payload, warc_type):
  new_payload = ''
  first_line = True
  in_http_header = False
  for line in payload.splitlines():
    if first_line:
      # Ignore blank lines before the HTTP header.
      if line.rstrip('\r\n'):
        in_http_header = looks_like_http_header(line, warc_type)
        # logging.info('Type: {}, Looks like HTTP header: {}'.format(warc_type, in_http_header))
        first_line = False
      continue
    if in_http_header:
      # HTTP header ends at the first blank line.
      if not line.rstrip('\r\n'):
        in_http_header = False
    else:
      new_payload += line
  return new_payload


def looks_like_http_header(line, warc_type):
  """Does this look like the first line of an HTTP request or response?"""
  fields = line.lower().split()
  if warc_type == 'request':
    if len(fields) >= 3:
      method = fields[0]
      protocol = fields[2]
      if method in HTTP_METHODS and protocol.startswith('http/'):
        return True
  elif warc_type == 'response':
    if len(fields) >= 2:
      protocol = fields[0]
      status = fields[1]
      try:
        int(status)
        status_is_int = True
      except ValueError:
        status_is_int = False
      if protocol.startswith('http/') and status_is_int:
        return True
  return False


def tone_down_logger():
  """Change the logging level names from all-caps to capitalized lowercase.
  E.g. "WARNING" -> "Warning" (turn down the volume a bit in your log files)"""
  for level in (logging.CRITICAL, logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG):
    level_name = logging.getLevelName(level)
    logging.addLevelName(level, level_name.capitalize())


if __name__ == '__main__':
  try:
    sys.exit(main(sys.argv))
  except IOError as ioe:
    if ioe.errno != errno.EPIPE:
      raise
