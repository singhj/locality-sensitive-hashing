import session
import tweepy
from tweepy import StreamListener
import sys
from tweepy.api import API
import socket

import twitter_settings
import logging

APP_KEY = twitter_settings.consumer_key
APP_SECRET = twitter_settings.consumer_secret
DEFAULT_NUM_TWEETS = 100

class TwitterStatusListener(StreamListener):

    def __init__(self, api=None):
        StreamListener.__init__(self, api=api)
        self.api = api or API()
        self.tweet_counter = 0

    def on_connect(self):
        """Called once connected to streaming server.

        This will be invoked once a successful response
        is received from the server. Allows the listener
        to perform some work prior to entering the read loop.
        """
        logging.info("on_connect()")

    def on_status(self, status):
        """Called when a new status arrives"""
        text = status.text.encode('utf-8') + '\n'
        logging.info('status: %s', text)

        self.tweet_counter += 1

        if self.tweet_counter >= DEFAULT_NUM_TWEETS:
            return False
        else:
            return True

    def on_error(self, status_code):
        logging.info('Error: ' + str(status_code) + "\n")
        return False

    def on_timeout(self):
        logging.info("Timeout, sleeping for 60 seconds...\n")
        time.sleep(60)
        return

class PublicTweets(session.BaseRequestHandler):
    def get(self):
        auth = tweepy.OAuthHandler(APP_KEY, APP_SECRET)
        # Redirect user to Twitter to authorize
        url = auth.get_authorization_url()
        self.session['request_token_key'] = auth.request_token.key
        self.session['request_token_secret'] = auth.request_token.secret
        self.redirect(url)

class TwitterCallback(session.BaseRequestHandler):
    def get_args(self):
        """
        All the args a request was called with
        """
        rqst = self.request
        args = rqst.arguments()
        resp = {}
        for arg in args:
            resp[arg] = repr(rqst.get_all(arg))
        return resp
    def get(self):
        resp = self.get_args()
        rqst = self.request
        verifier = rqst.get('oauth_verifier')

        auth = tweepy.OAuthHandler(APP_KEY, APP_SECRET)
        auth.set_request_token(self.session['request_token_key'], self.session['request_token_secret'])

        try:
            auth.get_access_token(verifier)
        except tweepy.TweepError:
            logging.error('Error! Failed to get access token.')

        api = tweepy.API(auth)
        listen = TwitterStatusListener(api)

        #note, tired doing secure=False which is not support by twitter api this gives an
        # error for the sample.json end_point
        stream = tweepy.Stream(auth, listen)
        logging.info("getting stream now!")

        try:
            stream.sample()
        except tweepy.TweepError:
            logging.error("error with streaming api")
            stream.disconnect()

        # home_timeline = api.home_timeline()
        # for status in home_timeline:
        #     logging.info('home_timeline %s: %s', status.user.screen_name, status.text)

urls = [
     ('/get_tweets', PublicTweets),
     ('/twitter_callback', TwitterCallback)
]
