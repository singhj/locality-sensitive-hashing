import webapp2, re, zipfile
import jinja2
from google.appengine.api import users
from bs4 import BeautifulSoup

from repositories.gae.blobstore import get_all_blob_info
from repositories.gae.blob_dataset import BlobDataset
from repositories.gae.blobstore import create_upload_url, get_blob_key
from pipelines.gae.lsh_pipelines import LshBlobPipeline
from pipelines.gae.map_reduce_pipeline_factory import MapReducePipelineFactory
from lsh_map_reduce.lsh_map_base import LshMapBase
from utils.zip_utils import all_matching_files


symbols = re.compile('\W+')

def parse_text(text):
    soup = BeautifulSoup(text.replace('\\n',' '))
    [s.extract() for s in soup(['script', 'style'])]
    text = soup.get_text(separator=' ', strip=True)
    text = symbols.sub(' ', text.lower())

    # Remove spurious white space characters
    text = ' '.join(text.split())
    return text

class MainHandler(webapp2.RequestHandler):
    template_env = jinja2.Environment(loader=jinja2.FileSystemLoader("templates"),
                                      autoescape=True)
    def get(self):
        user = users.get_current_user()
        username = user.nickname()

        results = BlobDataset.query()
        items = [result for result in results]

        for item in items:
            item.ds_key = item.key.urlsafe()
        length = len(items)

        upload_url = create_upload_url("upload_blob")

        self.response.out.write(self.template_env.get_template("blobs.html").render(
            {"username": username,
             "items": items,
             "length": length,
             "upload_url": upload_url}))

    def post(self):
        filename = self.request.get("filename")
        blob_key = self.request.get("blobkey")
        ds_key   = self.request.get("ds_key")

        map_reduce_pipeline = self.get_pipeline(blob_key)

        pipeline = LshBlobPipeline(filename, blob_key, ds_key, map_reduce_pipeline)
        pipeline.start()

        self.redirect(pipeline.base_path + "/status?root=" + pipeline.pipeline_id)

    def get_pipeline(self, blob_key):

        return MapReducePipelineFactory("locality_sensitive_hashing",
            "peer_belt_driver.PeerLshMap.map",
            "lsh_map_reduce.lsh_reduce_base.LshReduceBase.reduce",
            'mapreduce.input_readers.BlobstoreZipLineInputReader',
            "mapreduce.output_writers.BlobstoreOutputWriter",
            mapper_params={
                "blob_keys": blob_key,
            },
            reducer_params={
                "mime_type": "text/plain",
            },
            shards=16)

class PeerLshMap(LshMapBase):

    @classmethod
    def pre_process(cls, data):
        (blob_key, file_no, line) = (data[0][0], data[0][1], data[1])
        found_pattern = text_file_pattern.search(line)
        if not found_pattern:
            return
        (_id, text) = (found_pattern.group(1), found_pattern.group(2))

        dataset = BlobDataset.query(BlobDataset.blob_key == get_blob_key(blob_key)).get()
        text = parse_text(text)

        return (dataset, _id, text)

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

urls = [('/blobs', MainHandler),
        ('/upload_blob', UploadHandler),
        ('/serve_blob/([^/]+)?', ServeHandler),
        ('/view/([^/]+)?/([^/]+)?', ViewHandler),
        ]
