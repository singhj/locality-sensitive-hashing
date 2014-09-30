import re
import urllib
import webapp2
import logging
import zipfile

from google.appengine.ext import blobstore
from google.appengine.ext.webapp import blobstore_handlers
from google.appengine.api import taskqueue

class MainHandler(webapp2.RequestHandler):
    def get(self):
        upload_url = blobstore.create_upload_url('/upload_blob')
        self.response.out.write('<html><body>')
        self.response.out.write('<ol>')
        for blob_info in blobstore.BlobInfo.all():
            blob_key = blob_info.key()
            blob_reader = blobstore.BlobReader(blob_key)
            zip_reader = zipfile.ZipFile(blob_reader)
            url_file_reader = zip_reader.open('url.out')
            line_count = 0
            while url_file_reader.readline():
                line_count += 1
            self.response.out.write('<li><a href="/serve_blob/%s">%s</a> %s %d urls</li>' % (blob_info.key(), blob_info.filename, [('%s: %s' % (p, str(getattr(blob_info, p)))) for p in blob_info._all_properties if p not in ('md5_hash','filename',)], line_count))
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
        url_file_pattern = re.compile('^."id":"([^"]*)","url":"([^"]*)".*')
        lno = 0
        urls = {}
        with zip_reader.open('url.out') as url_file_reader:
            for line in url_file_reader:
                lno += 1
                found_pattern  = url_file_pattern.search(line)
                urls[found_pattern.group(1)] = found_pattern.group(2)
                if lno < 3:
                    logging.info('    line %d: %s: %s', lno, found_pattern.group(1), found_pattern.group(2))
        logging.info('url.out: %d ids', len(urls.keys()))

        taskqueue.add(url='/text_worker', params={'key': blob_key})
                
        self.redirect('/blobs')

class TextWorker(webapp2.RequestHandler):
    def post(self): # should run at most 1/s due to entity group limit
        blob_key = self.request.get('key')
        blob_reader = blobstore.BlobReader(blob_key)
        zip_reader = zipfile.ZipFile(blob_reader)
        text_file_pattern = re.compile('^."id":"([^"]*):html","text":"(.*".*).', flags=re.DOTALL)
        lno = 0
        texts = {}
        with zip_reader.open('text.out') as text_file_reader:
            for line in text_file_reader:
                found_pattern = text_file_pattern.search(line)
                if found_pattern:
#                     texts[found_pattern.group(1)] = found_pattern.group(2)
                    lno += 1
                    if lno < 3:
                        logging.info('    line %d: %s: %d characters', lno, found_pattern.group(1), len(found_pattern.group(2)))
        logging.info('text.out: %d ids', lno)

urls = [('/blobs', MainHandler),
        ('/upload_blob', UploadHandler),
        ('/serve_blob/([^/]+)?', ServeHandler),
        ('/text_worker', TextWorker),
        ]
