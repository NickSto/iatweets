#!/usr/bin/env python
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals
import sys
import time
import json
import errno
import logging
import argparse
from urllib import urlencode
from urlparse import urlparse, urlunparse
import requests
from requests_oauthlib import OAuth1
from ratelimit import RateLimit

ARG_DEFAULTS = {'log':sys.stderr, 'volume':logging.ERROR}
DESCRIPTION = """This is a pared-down modification of the python-twitter library that allows access
to the raw HTTP headers and data returned from the API. Necessary for saving in WARC format.
Only enough code to retrieve statuses is preserved."""


def main(argv):

  parser = argparse.ArgumentParser(description=DESCRIPTION)
  parser.set_defaults(**ARG_DEFAULTS)

  parser.add_argument('positional1', metavar='dispname',
    help='')
  parser.add_argument('-l', '--log', type=argparse.FileType('w'),
    help='Print log messages to this file instead of to stderr. Warning: Will overwrite the file.')
  parser.add_argument('-q', '--quiet', dest='volume', action='store_const', const=logging.CRITICAL)
  parser.add_argument('-v', '--verbose', dest='volume', action='store_const', const=logging.INFO)
  parser.add_argument('-D', '--debug', dest='volume', action='store_const', const=logging.DEBUG)

  args = parser.parse_args(argv[1:])

  logging.basicConfig(stream=args.log, level=args.volume, format='%(message)s')
  tone_down_logger()


class Api(object):

  def __init__(self,
               consumer_key=None,
               consumer_secret=None,
               access_token_key=None,
               access_token_secret=None,
               timeout=None,
               tweet_mode='extended',
               sleep_on_rate_limit=True):
    self.tweet_mode = tweet_mode
    self.sleep_on_rate_limit = sleep_on_rate_limit
    self._timeout = timeout
    self.__auth = OAuth1(consumer_key, consumer_secret, access_token_key, access_token_secret)
    self.base_url = 'https://api.twitter.com/1.1'
    self.rate_limit = RateLimit()

  def GetStatus(self,
                status_id,
                trim_user=False,
                include_my_retweet=True,
                include_entities=True,
                include_ext_alt_text=True):
    """Returns a single status message, specified by the status_id parameter.
    Args:
      status_id:
        The numeric ID of the status you are trying to retrieve.
      trim_user:
        When set to True, each tweet returned in a timeline will include
        a user object including only the status authors numerical ID.
        Omit this parameter to receive the complete user object. [Optional]
      include_my_retweet:
        When set to True, any Tweets returned that have been retweeted by
        the authenticating user will include an additional
        current_user_retweet node, containing the ID of the source status
        for the retweet. [Optional]
      include_entities:
        If False, the entities node will be disincluded.
        This node offers a variety of metadata about the tweet in a
        discreet structure, including: user_mentions, urls, and
        hashtags. [Optional]
    Returns:
      A twitter.Status instance representing that status message
    """
    url = '%s/statuses/show.json' % (self.base_url)

    parameters = {
      'id': enf_type('status_id', int, status_id),
      'trim_user': enf_type('trim_user', bool, trim_user),
      'include_my_retweet': enf_type('include_my_retweet', bool, include_my_retweet),
      'include_entities': enf_type('include_entities', bool, include_entities),
      'include_ext_alt_text': enf_type('include_ext_alt_text', bool, include_ext_alt_text)
    }

    resp = self._RequestUrl(url, data=parameters)
    if resp:
      data = resp.content.decode('utf-8')
      self._ParseAndCheckTwitter(data)

      return data, resp


  def _RequestUrl(self, url, data=None, json=None):
    """Request a url.
    Args:
      url:
        The web location we want to retrieve.
      data:
        A dict of (str, unicode) key/value pairs.
    Returns:
      A JSON object.
    """
    if not self.__auth:
      raise TwitterError("The twitter.Api instance must be authenticated.")

    if url and self.sleep_on_rate_limit:
      limit = self.CheckRateLimit(url)

      if limit.remaining == 0:
        try:
          time.sleep(max(int(limit.reset - time.time()) + 2, 0))
        except ValueError:
          pass
    if not data:
      data = {}

    data['tweet_mode'] = self.tweet_mode
    full_url = self._BuildUrl(url, extra_params=data)
    resp = requests.get(full_url, auth=self.__auth, timeout=self._timeout)

    if full_url and resp and self.rate_limit:
      limit = resp.headers.get('x-rate-limit-limit', 0)
      remaining = resp.headers.get('x-rate-limit-remaining', 0)
      reset = resp.headers.get('x-rate-limit-reset', 0)

      self.rate_limit.set_limit(url, limit, remaining, reset)

    return resp


  def CheckRateLimit(self, url):
    """ Checks a URL to see the rate limit status for that endpoint.
    Args:
      url (str):
        URL to check against the current rate limits.
    Returns:
      namedtuple: EndpointRateLimit namedtuple.
    """
    if not self.rate_limit.__dict__.get('resources', None):
      self.InitializeRateLimit()

    if url:
      limit = self.rate_limit.get_limit(url)

    return limit


  def InitializeRateLimit(self):
    """ Make a call to the Twitter API to get the rate limit
    status for the currently authenticated user or application.
    Returns:
      None.
    """
    _sleep = self.sleep_on_rate_limit
    if self.sleep_on_rate_limit:
      self.sleep_on_rate_limit = False

    url = '%s/application/rate_limit_status.json' % self.base_url

    resp = self._RequestUrl(url)  # No-Cache
    data = resp.content.decode('utf-8')
    json_data = self._ParseAndCheckTwitter(data)

    self.sleep_on_rate_limit = _sleep
    self.rate_limit = RateLimit(**json_data)


  def _BuildUrl(self, url, path_elements=None, extra_params=None):
    # Break url into constituent parts
    (scheme, netloc, path, params, query, fragment) = urlparse(url)

    # Add any additional path elements to the path
    if path_elements:
      # Filter out the path elements that have a value of None
      p = [i for i in path_elements if i]
      if not path.endswith('/'):
        path += '/'
      path += '/'.join(p)

    # Add any additional query parameters to the query string
    if extra_params and len(extra_params) > 0:
      extra_query = self._EncodeParameters(extra_params)
      # Add it to the existing query
      if query:
        query += '&' + extra_query
      else:
        query = extra_query

    # Return the rebuilt URL
    return urlunparse((scheme, netloc, path, params, query, fragment))


  @staticmethod
  def _EncodeParameters(parameters):
    """Return a string in key=value&key=value form.
    Values of None are not included in the output string.
    Args:
      parameters (dict): dictionary of query parameters to be converted into a
      string for encoding and sending to Twitter.
    Returns:
      A URL-encoded string in "key=value&key=value" form
    """
    if parameters is None:
      return None
    if not isinstance(parameters, dict):
      raise TwitterError("`parameters` must be a dict.")
    else:
      return urlencode(dict((k, v) for k, v in parameters.items() if v is not None))


  def _ParseAndCheckTwitter(self, raw_data):
    """Try and parse the JSON returned from Twitter.
    This is a purely defensive check because during some Twitter
    network outages it will return an HTML failwhale page.
    """
    try:
      json_data = json.loads(raw_data)
    except ValueError:
      if "<title>Twitter / Over capacity</title>" in raw_data:
        raise TwitterError({'message': "Capacity Error"})
      if "<title>Twitter / Error</title>" in raw_data:
        raise TwitterError({'message': "Technical Error"})
      if "Exceeded connection limit for user" in raw_data:
        raise TwitterError({'message': "Exceeded connection limit for user"})
      if "Error 401 Unauthorized" in raw_data:
        raise TwitterError({'message': "Unauthorized"})
      raise TwitterError({'Unknown error: {0}'.format(raw_data)})
    self._CheckForTwitterError(json_data)
    return json_data


  @staticmethod
  def _CheckForTwitterError(data):
    """Raises a TwitterError if twitter returns an error message.
    Args:
      data (dict):
        A python dict created from the Twitter json response
    Raises:
      (twitter.TwitterError): TwitterError wrapping the twitter error
      message if one exists.
    """
    # Twitter errors are relatively unlikely, so it is faster
    # to check first, rather than try and catch the exception
    if 'error' in data:
      raise TwitterError(data['error'])
    if 'errors' in data:
      raise TwitterError(data['errors'])


  def get_rate_limit(self):
    """Shortcut for getting the rate limit for the GetStatus url (the only one used here).
    Returns an EndpointRateLimit object with 3 relevant attributes:
    limit: the total limit per window
    remaining: how many requests are remaining in this window
    reset: when the window resets"""
    return self.rate_limit.get_limit('/statuses/show.json')


def enf_type(field, _type, val):
  """ Checks to see if a given val for a field (i.e., the name of the field)
  is of the proper _type. If it is not, raises a TwitterError with a brief
  explanation.
  Args:
      field:
          Name of the field you are checking.
      _type:
          Type that the value should be returned as.
      val:
          Value to convert to _type.
  Returns:
      val converted to type _type.
  """
  try:
    return _type(val)
  except ValueError:
    raise TwitterError({'message': '"{0}" must be type {1}'.format(field, _type.__name__)})


class TwitterError(Exception):
  """Base class for Twitter errors"""
  @property
  def message(self):
    '''Returns the first argument used to construct this error.'''
    return self.args[0]


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
