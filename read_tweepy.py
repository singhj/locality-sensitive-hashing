import session
import tweepy

import twitter_settings
import logging

APP_KEY = twitter_settings.consumer_key
APP_SECRET = twitter_settings.consumer_secret

class PublicTweets(session.BaseRequestHandler):
    def get(self):
        auth = tweepy.OAuthHandler(APP_KEY, APP_SECRET)
        # Redirect user to Twitter to authorize
        logging.info('reading tweets with %s', auth.str())
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
            logging.error( 'Error! Failed to get access token.' )

        api = tweepy.API(auth)
        home_timeline = api.home_timeline()
        for status in home_timeline:
            logging.info('home_timeline %s: %s', status.user.screen_name, status.text)

    def post(self):
        msg = self.get_args()

urls = [
     ('/get_tweets', PublicTweets),
     ('/twitter_callback', TwitterCallback)
]
