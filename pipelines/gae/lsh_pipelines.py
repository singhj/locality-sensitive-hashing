from pipelines.gae.store_output import StoreOutput
from mapreduce import base_handler
from google.appengine.ext import ndb
import logging

class LshBlobPipeline(base_handler.PipelineBase):
    """A pipeline to run LSH that reads and writes to blobstore

      :param blobkey: blobkey to process as string. Should be a zip archive with
      text files inside.
    """

    def run(self, filename, blobkey, ds_key, map_reduce_pipeline):
        self.params = "filename %s \tblobkey %s\tds_key %s" % (filename, blobkey, ds_key)

        logging.warning(self.params)

        dataset = ndb.Key(urlsafe=ds_key).get()
        dataset.buckets = []
        dataset.put()
        output = yield map_reduce_pipeline

        yield StoreOutput('OpenLSH', ds_key, output)

    def finalized(self):
        pass
