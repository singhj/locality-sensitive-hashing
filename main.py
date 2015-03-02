import os, time, webapp2, jinja2
import session, logging, json
import settings
from inspect import currentframe, getframeinfo

from google.appengine.api import users
from google.appengine.ext import ndb

JINJA_ENVIRONMENT = jinja2.Environment(
    loader=jinja2.FileSystemLoader(os.path.join(os.path.dirname(__file__), 'templates')),
    extensions=['jinja2.ext.autoescape'],
    autoescape=True)

import read_tweepy

from models import *

class MainPage(session.BaseRequestHandler):

    def get(self, command = ''):
        def lookup(dict_or_obj, member):
            try:
                # could be a dictionary or a NoneType
                member_value = dict_or_obj[member]
            except (KeyError, TypeError):
                try:
                    # could have it as an attribute
                    member_value = getattr(dict_or_obj, member)
                except AttributeError:
                    member_value = False
            return member_value

        this_app = AppOpenLSH.get_or_insert('KeyOpenLSH')
        app_is_open = this_app.is_open
        frameinfo = getframeinfo(currentframe())
        
        app_is_closed = False
        u = users.get_current_user()
        if u:
            demo_user = DemoUser.get_or_insert(u.user_id(), email = u.email(), nickname = u.nickname())

        ulogged = 'User not logged in' if not u else 'User is %s' % u.nickname()
        app_status = 'App is Open' if app_is_open else "App is Closed"
        if u and u.user_id() == '108492098862327080451':
            frameinfo = getframeinfo(currentframe())
            logging.info('file %s, line %s Admin User, %s', frameinfo.filename, frameinfo.lineno+1, app_status)
            url = users.create_logout_url(self.request.uri)
            url_linktext = 'Google Logout'
        elif u and app_is_open:
            frameinfo = getframeinfo(currentframe())
            logging.info('file %s, line %s %s %s', frameinfo.filename, frameinfo.lineno+1, ulogged, app_status)
            url = users.create_logout_url(self.request.uri)
            url_linktext = 'Google Logout'
        elif not u:
            frameinfo = getframeinfo(currentframe())
            logging.info('file %s, line %s %s %s', frameinfo.filename, frameinfo.lineno+1, ulogged, app_status)
            url = users.create_login_url(self.request.uri)
            url_linktext = 'Google Login -- use your Gmail'
        else:
            frameinfo = getframeinfo(currentframe())
            logging.info('file %s, line %s %s %s', frameinfo.filename, frameinfo.lineno+1, ulogged, app_status)
            url = users.create_logout_url(self.request.uri)
            url_linktext = 'Google Logout'
            app_is_closed = not app_is_open
        
        tw_auth = False
        try:
            tw_auth = self.session['tw_auth']
        except: pass

        tw_logged_in = False
        try:
            tw_logged_in = self.session['tw_logged_in']
        except: pass

        tw_banner = ''
        if tw_logged_in:
            tw_banner = 'Ready for Tweets'
        tweets = []
        tweet_display = ''
        if not app_is_closed:
            duik = lookup(self.session, 'duik')
            dui = ndb.Key(urlsafe = duik).get() if duik else None
            if not dui:
                dui = DemoUserInteraction.latest_for_user( u )
                self.session['duik'] = dui.key.urlsafe() if dui else None
            if dui:
                tweets = dui.tweets
                tw_banner = '%d Tweets as of %s' % (len(tweets), dui.asof.isoformat(' ')[:19]) 
                tweet_display = '<br/>\n&mdash; '.join(tweets)
        else:
            dui = None

        similar_sets, same_sets, accounted_ids = ([], [], [])
        frameinfo = getframeinfo(currentframe())
        logging.info('file %s, line %s %s', frameinfo.filename, frameinfo.lineno+1, similar_sets)
        if lookup(dui, 'calc_done'):
            read_tweepy.LshTweets.show(self.session)
            similar_sets, same_sets, accounted_ids  = self.session['lsh_results']
        frameinfo = getframeinfo(currentframe())
        logging.info('file %s, line %s %s %s', frameinfo.filename, frameinfo.lineno+1, similar_sets)
            
        try:
            if command == 'show_lsh_results':
#                 matched_tweets = [tweets[twid] for twid in range(len(tweets)) if twid     in self.session['lsh_results'][1]]
                other_tweets   = [tweets[twid] for twid in range(len(tweets)) if twid not in self.session['lsh_results'][1]]
                tweet_display = '<br/>\n&mdash; '.join(other_tweets)
        except: pass

        template_values = {
            'app_is_closed': app_is_closed,
            'google_logged_in': u,
            'url': url,
            'url_linktext': url_linktext,
            'tw_auth': tw_auth,
            'tw_banner': tw_banner,
            'similar_sets': similar_sets,
            'same_sets': same_sets,
            'tweets': tweets,
            'fetching': lookup(dui, 'fetching'),
            'calculating': lookup(dui, 'calculating'),
            'calc_done': lookup(dui, 'calc_done'),
            'gaCode': settings.gaCode,
        }

        template = JINJA_ENVIRONMENT.get_template('tweets_index.html')
        try:
            self.response.write(template.render(template_values))
        except UnicodeDecodeError:
            template_values['tweets'] = ['unreadable content']
            self.response.write(template.render(template_values))
    def post(self):
        cmd = self.request.get('command')
        if cmd == 'calc_lsh':
            read_tweepy.LshTweets.calc(self.session)
        elif cmd == 'show_lsh_results':
            read_tweepy.LshTweets.show(self.session)
        self.get(cmd)

class WaitPage(session.BaseRequestHandler):
    def get(self):
        template_values = {
            'gaCode': settings.gaCode,
        }

        template = JINJA_ENVIRONMENT.get_template('coming_soon.html')
        try:
            self.response.write(template.render(template_values))
        except UnicodeDecodeError:
            template_values['tweets'] = 'unreadable content'
            self.response.write(template.render(template_values))
    def post(self):
        pass

urls = [
    ('/', MainPage),
    ('/coming_soon', WaitPage),
]
import read_tweepy
urls += read_tweepy.urls
import blobs
urls += blobs.urls
import mr_main
urls += mr_main.urls
import peer_belt_driver
urls += peer_belt_driver.urls
import test_db_datastore
urls += test_db_datastore.urls

sess_config = {}
sess_config['webapp2_extras.sessions'] = {
    'secret_key': 'dcd99df0-824a-4331-9a55-2d5900e27732'
}
application = webapp2.WSGIApplication(urls, debug=True, config=sess_config)
