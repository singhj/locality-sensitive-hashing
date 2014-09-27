import os
import urllib
import webapp2
import logging
import zipfile

from google.appengine.ext import blobstore
from google.appengine.ext.webapp import blobstore_handlers

class MainHandler(webapp2.RequestHandler):
    def get(self):
        upload_url = blobstore.create_upload_url('/upload_blob')
        self.response.out.write('<html><body>')
        self.response.out.write('<ol>')
        for blob_info in blobstore.BlobInfo.all():
            self.response.out.write('<li><a href="/serve_blob/%s">%s</a> %s</li>' % (blob_info.key(), blob_info.filename, [('%s: %s' % (p, str(getattr(blob_info, p)))) for p in blob_info._all_properties if p not in ('md5_hash','filename',)]))
        self.response.out.write('</ol>')
        self.response.out.write('<form action="%s" method="POST" enctype="multipart/form-data">' % upload_url)
        self.response.out.write("""Upload File: <input type="file" name="file"><br> <input type="submit"
            name="submit" value="Submit"> </form></body></html>""")

class UploadHandler(blobstore_handlers.BlobstoreUploadHandler):
    def post(self):
        upload_files = self.get_uploads('file')  # 'file' is file upload field in the form
        blob_info = upload_files[0]
        self.redirect('/serve_blob/%s' % blob_info.key())

class ServeHandler(blobstore_handlers.BlobstoreDownloadHandler):
    def get(self, resource):
        blob_key = str(urllib.unquote(resource))
        blob_reader = blobstore.BlobReader(blob_key)
        zip_reader = zipfile.ZipFile(blob_reader)
        logging.info('contents: %s', zip_reader.namelist())
        url_file_reader = zip_reader.open('url.out')
        for line in xrange(3):
            logging.info('    line %d: %s', line, url_file_reader.readline()) 
        self.redirect('/blobs')
#         resource = str(urllib.unquote(resource))
#         blob_info = blobstore.BlobInfo.get(resource)
#         logging.info('blob_info: %s', [('%s: %s' % (p, str(getattr(blob_info, p)))) for p in blob_info._all_properties])
#         self.send_blob(blob_info, save_as = True)

urls = [('/blobs', MainHandler),
        ('/upload_blob', UploadHandler),
        ('/serve_blob/([^/]+)?', ServeHandler)]
