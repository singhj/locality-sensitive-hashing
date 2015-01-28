import sys, re, math, random, struct, zipfile, json
import webapp2, hashlib
import logging
import operator, datetime, time
import jinja2

from google.appengine.ext import blobstore
from google.appengine.ext.webapp import blobstore_handlers
from google.appengine.ext import ndb
from google.appengine.api import users

from mapreduce import base_handler
from mapreduce import mapreduce_pipeline

from lsh.utils.similarity import compute_positive_hash
from lsh.shingles.shingles import _get_list_of_shingles

sys.path.insert(0, 'libs')
from bs4 import BeautifulSoup

max_bits = int(math.log(sys.maxsize+2, 2))
url_file_pattern = re.compile('^."id":"([^"]*)","url":"([^"]*)".*')
text_file_pattern = re.compile('^{"id":"([^"]*):html","text":"(.*)}', flags=re.DOTALL)
symbols = re.compile('\W+')

class TestHandler(webapp2.RequestHandler):
    template_env = jinja2.Environment(loader=jinja2.FileSystemLoader("templates"),
                                      autoescape=True)
    def get(self):
        user = users.get_current_user()
        username = user.nickname()

        dummydoc = MatrixRow.create()
        dataset = Matrix.create('myFile', 'gae')

        self.response.out.write(self.template_env.get_template("blobs.html").render(
            {"username": username,
         }))


urls = [('/test_db_datastore', TestHandler),
        ]
