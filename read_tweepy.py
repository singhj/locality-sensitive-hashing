import session
import tweepy
from tweepy import StreamListener
import time
from tweepy.api import API
from google.appengine.ext import ndb
import twitter_settings
import logging
from pipe_node import PipeNode, NotFound, NotLoggedIn
from data_queues.fifo_queue import FIFOQueue

APP_KEY = twitter_settings.consumer_key
APP_SECRET = twitter_settings.consumer_secret
DEFAULT_NUM_TWEETS = 100

class TwitterStatusListener(StreamListener):

    def __init__(self, queue, api=None, num_tweets=0, language=None):
        StreamListener.__init__(self, api=api)
        self.api = api or API()
        self.tweet_counter = 0
        self.language = language # should be a ISO 639-1 code

        if num_tweets == 0:
            self.num_tweets = DEFAULT_NUM_TWEETS
        else:
            self.num_tweets = num_tweets

        self.start_time = time.gmtime()
        self.prefix = str(int(time.time()))
        self.queue = queue

    def on_connect(self):
        """Called once connected to streaming server.

        This will be invoked once a successful response
        is received from the server. Allows the listener
        to perform some work prior to entering the read loop.
        """
        logging.info("on_connect()")

    def on_status(self, status):
        """Called when a new status arrives"""
        self.tweet_counter += 1
        text = status.text
        lang = status.user.lang

        # filter by languauge if one was given otherwise add all tweets to queue.
        if self.language:
            if lang.lower() == self.language.lower():
                self.queue.enqueue(text)
        else:
            self.queue.enqueue(text)

        if self.tweet_counter == self.num_tweets:
            return False # this should trigger closing the connection
        else:
            return True # always return True if we want to keep the connection open

    def on_error(self, status_code):
        logging.info('Error: ' + str(status_code) + "\n")
        return False

    def on_timeout(self):
        logging.info("Timeout, sleeping for 60 seconds...\n")
        time.sleep(60)
        return

class TwitterLogin(session.BaseRequestHandler):
    def get(self):
        auth = tweepy.OAuthHandler(APP_KEY, APP_SECRET)
        # Redirect user to Twitter to authorize
        url = auth.get_authorization_url()
        self.session['request_token_key'] = auth.request_token.key
        self.session['request_token_secret'] = auth.request_token.secret
        self.redirect(url)
    def post(self):
        self.get()

class TwitterLogout(session.BaseRequestHandler):
    def get(self):
        self.session['tw_auth'] = None
        self.session['tweets'] = []
        self.redirect('/')
        return
    def post(self):
        self.get()

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
        
        self.session['tw_auth'] = auth
        self.session['tw_status'] = 'Logged In and Ready'
        self.redirect('/')


class TwitterGetTweets(session.BaseRequestHandler):
    def get(self):
        auth = self.session['tw_auth']
        api = tweepy.API(auth)
        data_queue = FIFOQueue.instance()
        listener = TwitterStatusListener(queue=data_queue, api=api, num_tweets=200, language="en")

        #note, tried doing secure=False which is not support by twitter api this gives an
        # error for the sample.json end_point
        stream = tweepy.Stream(auth, listener)

        try:
            stream.sample()
        except tweepy.TweepError:
            logging.error("error with streaming api")
            stream.disconnect()
        return (listener)

    def post(self):
        self.get()

class TwitterStreamDump(ndb.Model):
    asof = ndb.DateTimeProperty(auto_now_add=True)
    content = ndb.TextProperty()

class TwitterReadNode(TwitterGetTweets, PipeNode):
    def open(self):
        if not ('tw_auth' in self.session):
            logging.error("Not logged in into twitter, tw_auth key not found in session dict")
            raise NotLoggedIn("Not logged in into twitter...auth not in session dict")
        auth = self.session['tw_auth']
        api = tweepy.API(auth)

        if not api:
            logging.error("Not logged in into twitter, no tweepy api")
            raise NotLoggedIn("Not logged in into twitter...no tweepy api")
        
        # Read tweets from the stream
        self.twitter_listener = super(TwitterReadNode, self).get()

        logging.info('TwitterReadNode.Open completed')

    def get_next(self):
        while not self.twitter_listener.queue.empty():
            yield self.twitter_listener.queue.dequeue()

        raise NotFound('Tweets exhausted')
    
    def close(self, save=False):
        tweets = '<br/>\n&mdash; '.join(self.tweets)
        if save:
            all_the_tweets = TwitterStreamDump(content=tweets)
            all_the_tweets.put()

        logging.info('TwitterReadNode.Close completed')

        num_tweets_status = "Number of tweets received - %s" % str(self.twitter_listener.tweet_counter)
        banner = 'Done getting tweets at %s. %s' % (time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.gmtime()), num_tweets_status)

        self.session['tw_status'] = banner
        self.session['tweets'] = tweets
        self.redirect('/')
    
    def post(self):
        try:
            self.open()
            self.tweets = []
        except:
            self.session['tw_auth'] = None
            self.redirect('/')
            return
        
        while True:
            try:
                for tweet in self.get_next():
                    self.tweets.append(tweet) # adding to tweets collection so that we print the output in the browser during testing
            except NotFound as nf:
                logging.info('TwitterReadNode.GetNext completed, %s', nf.value)
                break

        self.close(save=True)

urls = [
     ('/twitter_login', TwitterLogin),
     ('/twitter_callback', TwitterCallback),
#      ('/twitter_get_tweets', TwitterGetTweets),
     ('/twitter_read_node', TwitterReadNode),
     ('/twitter_logout', TwitterLogout),
]
