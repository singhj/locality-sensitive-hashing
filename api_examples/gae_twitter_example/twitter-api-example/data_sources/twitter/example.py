from twython import TwythonStreamer
import os
import timeit

APP_KEY = os.environ['TWITTER_ACCESS_KEY']
APP_SECRET = os.environ['TWITTER_SECRET_KEY']
TWITTER_OAUTH_ACCESS_KEY = os.environ['TWITTER_OAUTH_ACCESS_KEY']
TWITTER_OAUTH_SECRET_KEY = os.environ['TWITTER_OAUTH_SECRET_KEY']

class MyStreamer(TwythonStreamer):

    def __init__(self, app_key, app_secret, oauth_token, oauth_token_secret,
                 timeout=300, retry_count=None, retry_in=10, client_args=None,
                 handlers=None, chunk_size=1):
        TwythonStreamer.__init__(self, app_key, app_secret, oauth_token, oauth_token_secret)

        self.num_tweets = 0
        self.start_time = timeit.default_timer()

    def on_success(self, data):
        # filter out non-english tweets
        if 'text' in data and 'lang' in data and data['lang'] == 'en':
            print data['text'].encode('utf-8') + '\n'

            self.num_tweets += 1
            elapsed = int(timeit.default_timer()) - int(self.start_time)

            if elapsed == 900:
                print "number of english language tweets from sample stream in 15 mins: %s" % str(self.num_tweets)
                self.disconnect()

    def on_error(self, status_code, data):

        print status_code
        self.disconnect()


def get_public_tweets():
    """
        Get public statuses using streaming api's GET statuses/sample.
    """
    stream = MyStreamer(APP_KEY, APP_SECRET, TWITTER_OAUTH_ACCESS_KEY, TWITTER_OAUTH_SECRET_KEY)
    stream.statuses.sample()
