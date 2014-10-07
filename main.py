import os
import jinja2
import webapp2
import session

from google.appengine.api import users

JINJA_ENVIRONMENT = jinja2.Environment(
    loader=jinja2.FileSystemLoader(os.path.join(os.path.dirname(__file__), 'templates')),
    extensions=['jinja2.ext.autoescape'],
    autoescape=True)

class MainPage(session.BaseRequestHandler):

    def get(self):

        if users.get_current_user():
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

        tweets = ''
        try:
            tweets = self.session['tweets']
        except: pass

        template_values = {
            'url': url,
            'url_linktext': url_linktext,
            'tw_auth': tw_auth,
            'tw_status': tw_status,
            'tweets': tweets
        }

        template = JINJA_ENVIRONMENT.get_template('tweets_index.html')
        try:
            self.response.write(template.render(template_values))
        except UnicodeDecodeError:
            template_values['tweets'] = 'unreadable content'
            self.response.write(template.render(template_values))
    def post(self):
        pass

urls = [
    ('/', MainPage),
]
import read_tweepy
urls += read_tweepy.urls
import blobs
urls += blobs.urls
import mr_main
urls += mr_main.urls

sess_config = {}
sess_config['webapp2_extras.sessions'] = {
    'secret_key': 'dcd99df0-824a-4331-9a55-2d5900e27732'
}
application = webapp2.WSGIApplication(urls, debug=True, config=sess_config)
