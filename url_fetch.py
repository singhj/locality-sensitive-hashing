import os
import webapp2
import jinja2
from google.appengine.api import urlfetch

JINJA_ENVIRONMENT = jinja2.Environment(
    loader=jinja2.FileSystemLoader(os.path.dirname(__file__)),
    extensions=['jinja2.ext.autoescape'],
    autoescape=True)

class the_book(webapp2.RequestHandler):
    def get(self):
        response = urlfetch.fetch('http://www.gutenberg.org/ebooks/2591.txt.utf-8', deadline=10)

        template_values = {
            'text': response.content,
        }

        template = JINJA_ENVIRONMENT.get_template('book.html')
        self.response.write(template.render(template_values))

urls = [
     ('/fetch', the_book),
]
