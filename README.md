Candidate Tweets
================

This is some code developed at the Internet Archive's [Social Media and Gov Data Hackathon](https://blog.archive.org/2017/01/02/join-us-for-a-white-house-social-media-and-gov-data-hackathon/) on Jan 7, 2016.

The goal of this project is to build on [a log](https://archive.org/details/CandidatesAndOtherPoliticans) the Archive made of the 2016 presidential candidates' tweets. This archive contains the tweets by the candidates, but we wanted to make sure to save the full context of the tweets: the replies, conversations, images, links, and any other relevant data.

`parse_warc.py` is a module that can read the idiosyncratic WARC files in the archive. Usage is in comments in the source.  
`crawl.py` is the main script which reads the tweets and uses the Twitter API to retrieve related tweets and data. At the moment it can parse the saved tweet data and retrieve tweets earlier in the conversation. Run with the -h option for usage.

`crawl.py` requires the [python-twitter](https://pypi.python.org/pypi/python-twitter/) module to be installed in order to retrieve tweets from Twitter. See [their documentation](https://python-twitter.readthedocs.io/en/latest/getting_started.html) for instructions on obtaining Twitter API tokens.

