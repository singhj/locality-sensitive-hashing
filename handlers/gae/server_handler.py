from google.appengine.ext.webapp import blobstore_handlers
import urllib, zipfile, logging, re
from repositories.gae.blobstore import get_reader
from utils.zip_utils import all_matching_files

url_file_pattern = re.compile('^."id":"([^"]*)","url":"([^"]*)".*')

class ServeHandler(blobstore_handlers.BlobstoreDownloadHandler):
    def get(self, resource):
        blob_key = str(urllib.unquote(resource))
        blob_reader = get_reader(blob_key)
        zip_reader = zipfile.ZipFile(blob_reader)
        logging.info('contents: %s', zip_reader.namelist())
        
        urls = {}
        for lno, mno, _id, text in all_matching_files(zip_reader, 'url.out', url_file_pattern): 
            urls[_id] = text
            if lno < 3:
                logging.info('    match %d (line %d): %s: %s', mno, lno, _id, text)
        logging.info('url.out: %d ids', len(urls.keys()))

        self.redirect('/blobs')