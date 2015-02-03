import sys, re, math, random, struct, zipfile, json
import webapp2, hashlib, urllib
import logging, inspect
import operator, datetime, time
import jinja2
from collections import defaultdict

from google.appengine.api import taskqueue
from google.appengine.ext import blobstore
from google.appengine.ext.webapp import blobstore_handlers
from google.appengine.ext import ndb
from google.appengine.api import users

from lsh_matrix import *
from serial import lsh_zipfile, PeerbeltLine

from lsh.utils.similarity import compute_positive_hash
from lsh.shingles.shingles import _get_list_of_shingles

sys.path.insert(0, 'libs')
from bs4 import BeautifulSoup

url_file_pattern = re.compile('^."id":"([^"]*)","url":"([^"]*)".*')
text_file_pattern = re.compile('^{"id":"([^"]*):html","text":"(.*)}', flags=re.DOTALL)
symbols = re.compile('\W+')


class TestHandler(webapp2.RequestHandler):
    template_env = jinja2.Environment(loader=jinja2.FileSystemLoader("templates"),
                                      autoescape=True)
    def get(self):
        user = users.get_current_user()
        username = user.nickname()
        
        items = Matrix.all()
        for item in items:
            logging.info('item key %s blob_key %s, filename %s ', item.key.urlsafe(), item.file_key, item.filename)
        length = len(items)
        upload_url = blobstore.create_upload_url("/test_upload_blob")

        self.response.out.write(self.template_env.get_template("blobs.html").render(
            {"username": username,
             "items": items,
             "length": length,
             "upload_url": upload_url,
             "top_form_url": "test_db_datastore"}))
    
    def post(self):
        filename = self.request.get("filename")
        blob_key = self.request.get("blobkey")
        ds_key   = self.request.get("ds_key")
        output_link   = self.request.get("output_link")

        if self.request.get("run_lsh"):
            taskqueue.add(url='/test_zip_worker', 
                          params={'filename': filename,
                                  'blob_key': blob_key,
                                  })
        else:
            pass

        time.sleep(1)
        self.get()

class UploadHandler(blobstore_handlers.BlobstoreUploadHandler):
    def post(self):
        upload_files = self.get_uploads("file")
        blob_info = upload_files[0]
        blob_key = blob_info.key()
        logging.info('filename %s key %s', blob_info.filename, blob_key)
        Matrix.create('gae_test', blob_info.filename, file_key = blob_key)
        time.sleep(1)
        self.redirect('/test_db_datastore')

class ZipWorker(webapp2.RequestHandler):
    def post(self):
        blob_key = self.request.get('blob_key')
        filename = self.request.get("filename")
        blob_reader = blobstore.BlobReader(blob_key)
        zip_reader = zipfile.ZipFile(blob_reader)
        lsh_zipfile(PeerbeltLine, zip_reader, 'gae_test', filename, file_key = blob_key)
        return

def all(cls):
    cls._initialize()
    items = [result for result in cls._instances[cls].StorageProxy.query().fetch()]
    for item in items:
        valnames = vars(item)['_values'].keys()
        logging.info('vals %s', valnames)
        attributes = {}
        for name in valnames:
            try:
                attributes[name] = getattr(item, name)
            except AttributeError:
                logging.error('%s: %s', name, '...missing. Check memcache -- it may be serving junk')
        logging.info('Dataset %s', attributes)
    return items

Matrix.all = classmethod(all)

urls = [('/test_db_datastore', TestHandler),
        ('/test_upload_blob', UploadHandler),
        ('/test_zip_worker', ZipWorker),
        ]
