import os, webapp2, jinja2
import session
import settings

from google.appengine.api import users
from google.appengine.ext import ndb

JINJA_ENVIRONMENT = jinja2.Environment(
    loader=jinja2.FileSystemLoader(os.path.join(os.path.dirname(__file__), 'templates')),
    extensions=['jinja2.ext.autoescape'],
    autoescape=True)

import read_tweepy

class MainPage(session.BaseRequestHandler):

    def get(self, command = ''):
        def lookup(dict_or_obj, flag):
            try:
                # could be a dictionary or a NoneType
                flag_value = dict_or_obj[flag]
            except (KeyError, TypeError):
                try:
                    # could have it as an attribute
                    flag_value = getattr(dict_or_obj, flag)
                except AttributeError:
                    flag_value = False
            return flag_value

        u = users.get_current_user()
        if u:
            url = users.create_logout_url(self.request.uri)
            url_linktext = 'Google Logout'
        else:
            url = users.create_login_url(self.request.uri)
            url_linktext = 'Google Login -- use your Gmail'
        
        tw_auth = False
        try:
            tw_auth = self.session['tw_auth']
        except: pass

        tw_status = False
        try:
            tw_status = self.session['tw_status']
        except: pass

        duik = lookup(self.session, 'duik')
        dui = ndb.Key(urlsafe = duik).get() if duik else None

        if not dui:        
            dui = read_tweepy.DemoUserInfo.latest_for_user( u )
            self.session['duik'] = dui.key.urlsafe() if dui else None

        display = ''
        try:
            if command == 'show_lsh_results':
                display = '%s' % self.session['lsh_results']
            elif command == 'calc_lsh':
                display = self.session['tweets']
            else:
                display = self.session['tweets']
        except: pass

        template_values = {
            'google_logged_in': u,
            'url': url,
            'url_linktext': url_linktext,
            'tw_auth': tw_auth,
            'tw_status': tw_status,
            'display': display,
            'fetched': lookup(self.session, 'fetched'),
            'calculating': lookup(dui, 'calculating'),
            'calc_done': lookup(dui, 'calc_done'),
            'showing_lsh_results': command == 'show_lsh_results',
            'gaCode': settings.gaCode,
        }

        template = JINJA_ENVIRONMENT.get_template('tweets_index.html')
        try:
            self.response.write(template.render(template_values))
        except UnicodeDecodeError:
            template_values['tweets'] = 'unreadable content'
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
