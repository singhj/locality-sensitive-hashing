import session
import tweepy
from tweepy import StreamListener
import time
from tweepy.api import API

from google.appengine.ext import ndb
import twitter_settings
import logging
from pipe_node import PipeNode, NotFound, NotLoggedIn

APP_KEY = twitter_settings.consumer_key
APP_SECRET = twitter_settings.consumer_secret
DEFAULT_NUM_TWEETS = 100

class TwitterStatusListener(StreamListener):

    def __init__(self, api=None):
        StreamListener.__init__(self, api=api)
        self.api = api or API()
        self.tweets = []
        self.start_time = time.gmtime()
        self.prefix = str(int(time.time()))

    def on_connect(self):
        """Called once connected to streaming server.

        This will be invoked once a successful response
        is received from the server. Allows the listener
        to perform some work prior to entering the read loop.
        """
        logging.info("on_connect()")

    def on_status(self, status):
        """Called when a new status arrives"""
        text = status.text #.encode('utf-8')
        self.tweets.append(text) 
#         status = TwitterStatus(text = text)
#         status.put()
        #logging.info('status: %s', text)

        if len(self.tweets) >= DEFAULT_NUM_TWEETS:
            return False # this should trigger closing the connection
        else:
            return True

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
        auth = self.session['auth']
        api = tweepy.API(auth)
        listen = TwitterStatusListener(api)

        #note, tried doing secure=False which is not support by twitter api this gives an
        # error for the sample.json end_point
        stream = tweepy.Stream(auth, listen)
        logging.info("getting stream now!")

        try:
            stream.sample()
        except tweepy.TweepError:
            logging.error("error with streaming api")
            stream.disconnect()
        return (listen.tweets)

    def post(self):
        self.get()

class TwitterStreamDump(ndb.Model):
    asof = ndb.DateTimeProperty(auto_now_add=True)
    content = ndb.TextProperty()

class TwitterReadNode(TwitterGetTweets, PipeNode):
    def Open(self):
        if not ('auth' in self.session): 
            raise NotLoggedIn("Not logged in into twitter")
        auth = self.session['auth']
        api = tweepy.API(auth)
        if not api:
            raise NotLoggedIn("Not logged in into twitter")
        
        # Read tweets from the stream
        self.tweets = super(TwitterReadNode, self).get()
        self.cursor = 0
        self.count = len(self.tweets)
        
        logging.info('TwitterReadNode.Open completed')
    
    def GetNext(self):
        if self.cursor < self.count:
            tweet = self.tweets[self.cursor]
            self.cursor += 1
            return tweet
        raise NotFound('Tweets exhausted')
    
    def Close(self, save = False):
        tweets = '<br/>\n&mdash; '.join(self.tweets)
        if save:
            all_the_tweets = TwitterStreamDump(content = tweets)
            all_the_tweets.put()

        logging.info('TwitterReadNode.Close completed')
        banner = 'Done getting tweets at %s' % time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.gmtime()) 
        self.session['tw_status'] = banner
        self.session['tweets'] = tweets
        self.redirect('/')
    
    def post(self):
        try:
            self.Open()
        except:
            self.session['tw_auth'] = None
            self.redirect('/')
            return
        
        while True:
            try:
                self.GetNext()
            except NotFound as nf:
                logging.info('TwitterReadNode.GetNext completed, %s', nf.value)
                break
        
        self.Close(save = True)

urls = [
     ('/twitter_login', TwitterLogin),
     ('/twitter_callback', TwitterCallback),
#      ('/twitter_get_tweets', TwitterGetTweets),
     ('/twitter_read_node', TwitterReadNode),
]
