import zipfile
import logging

import webapp2
import re
import jinja2
from google.appengine.api import users

from bs4 import BeautifulSoup
from utils.zip_utils import all_matching_files
from repositories.gae.blobstore import get_all_blob_info
from repositories.gae.blob_dataset import BlobDataset
from repositories.gae.blobstore import create_upload_url, get_blob_key
from lsh.gae.pipelines.map_reduce_pipeline_factory import MapReducePipelineFactory
from lsh.gae.pipelines.lsh_pipelines import LshBlobPipeline
from lsh.gae.map_reduce import map, reduce

symbols = re.compile('\W+')
text_file_pattern = re.compile('^{"id":"([^"]*):html","text":"(.*)}', flags=re.DOTALL)

import os

class MainHandler(webapp2.RequestHandler):
    template_env = jinja2.Environment(loader=jinja2.FileSystemLoader("templates"),
                                      autoescape=True)
    def get(self):
        logging.info(os.path.abspath(os.path.curdir))
        logging.info('Peer Belt Driver get method called!')
        user = users.get_current_user()
        username = user.nickname()

        results = BlobDataset.query()
        items = [result for result in results]

        for item in items:
            item.ds_key = item.key.urlsafe()
        length = len(items)

        upload_url = create_upload_url("upload_blob2")

        self.response.out.write(self.template_env.get_template("blobs2.html").render(
            {"username": username,
             "items": items,
             "length": length,
             "upload_url": upload_url}))

    def post(self):
        filename = self.request.get("filename")
        blob_key = self.request.get("blobkey")
        ds_key   = self.request.get("ds_key")

        map_reduce_pipeline_dict = self.get_pipeline(blob_key)

        logging.info('filename %s key %s', filename, blob_key)
        logging.info(map_reduce_pipeline_dict)

        pipeline = LshBlobPipeline(filename, blob_key, ds_key, map_reduce_pipeline_dict)
        pipeline.start()

        self.redirect(pipeline.base_path + "/status?root=" + pipeline.pipeline_id)

    def get_pipeline(self, blob_key):
        return MapReducePipelineFactory('locality_sensitive_hashing',
            'peer_belt_driver.lsh_map',
            'peer_belt_driver.lsh_reduce',
            'mapreduce.input_readers.BlobstoreZipLineInputReader',
            "mapreduce.output_writers.BlobstoreOutputWriter",
            mapper_params={
                "blob_keys": blob_key,
            },
            reducer_params={
                "mime_type": "text/plain",
            },
            shards=16).create()

#wrapper functions that call the PeerBelt specific map and reduce functions
def lsh_map(data):
    # pre-process data
    dataset, id, text = pre_process(data)

    #parse text
    parsed_text = parse_text(text)

    for bkt,output_str in map(dataset, parsed_text, id):
        yield bkt,output_str

def lsh_reduce(key, values):
    for k,v in reduce(key,values):
        yield k,v

def pre_process(data):
    logging.info("Peer Belt > pre_process() called.")
    (blob_key, file_no, line) = (data[0][0], data[0][1], data[1])
    found_pattern = text_file_pattern.search(line)
    if not found_pattern:
        return
    (_id, text) = (found_pattern.group(1), found_pattern.group(2))

    dataset = BlobDataset.query(BlobDataset.blob_key == get_blob_key(blob_key)).get()

    return (dataset, _id, text)

def parse_text(text):
    logging.info("Peer Belt > parse_text() called.")
    soup = BeautifulSoup(text.replace('\\n',' '))
    [s.extract() for s in soup(['script', 'style'])]
    text = soup.get_text(separator=' ', strip=True)
    text = symbols.sub(' ', text.lower())

    # Remove spurious white space characters
    text = ' '.join(text.split())
    return text

class ViewHandler(webapp2.RequestHandler):
    def get(self, dataset_name, file_id):
        def cleanup(text):
            return text.replace('\\n', ' ')
        for blob_info, blob_reader in get_all_blob_info():
            if blob_info.filename == dataset_name:
                zip_reader = zipfile.ZipFile(blob_reader)
                for member in zip_reader.namelist():
                    for lno, mno, _id, text in all_matching_files(zip_reader, member, text_file_pattern):
                        if file_id == _id:
                            self.response.out.write(cleanup(text))
                            return
                message = 'ID %s not found' % file_id
                self.response.out.write('<html><body><p>%s</p></body></html>' % message)
                return
        message = 'Blob %s not found' % dataset_name
        self.response.out.write('<html><body><p>%s</p></body></html>' % message)
        return

from handlers.gae.upload_handler import UploadHandler
from handlers.gae.server_handler import ServeHandler

urls = [('/blobs2', MainHandler),
        ('/upload_blob2', UploadHandler),
        ('/serve_blob2/([^/]+)?', ServeHandler),
        ('/view2/([^/]+)?/([^/]+)?', ViewHandler),
        ]
