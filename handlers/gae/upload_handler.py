import logging
from repositories.gae.blob_dataset import BlobDataset
from google.appengine.ext.webapp import blobstore_handlers

class UploadHandler(blobstore_handlers.BlobstoreUploadHandler):
    def post(self):
        upload_files = self.get_uploads("file")
        blob_info = upload_files[0]
        blob_key = blob_info.key()

        logging.info('(REFACTORED) filename %s key %s', blob_info.filename, blob_key)

        BlobDataset.create(blob_key, filename=blob_info.filename)

        self.redirect("/blobs2")