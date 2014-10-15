from mapreduce import base_handler
from pipelines.gae.map_reduce_pipeline_factory import MapReducePipelineFactory
from pipelines.gae.store_output import StoreOutput
from google.appengine.ext import ndb
import logging

class LshPipeline(base_handler.PipelineBase):
    """A pipeline to run LSH.

    Args:
      blobkey: blobkey to process as string. Should be a zip archive with
        text files inside.
    """

    def run(self, filename, blob_key, ds_key):
        self.params = "filename %s \tblob_key %s\tds_key %s" % (filename, blob_key, ds_key)
        logging.warning(self.params)

        dataset = ndb.Key(urlsafe=ds_key).get()
        dataset.buckets = []
        dataset.put()
        output = yield self.get_pipeline(blob_key)

        yield StoreOutput('OpenLSH', ds_key, output)

    def finalized(self):
        pass

    def get_pipeline(self, blob_key):

        return MapReducePipelineFactory("locality_sensitive_hashing",
            "blobs.lsh_map", #TODO change this when we set up it up
            "blobs.lsh_bucket", #TODO change this when we set up it up
            'mapreduce.input_readers.BlobstoreZipLineInputReader',
            "mapreduce.output_writers.BlobstoreOutputWriter",
            mapper_params={
                "blob_keys": blob_key,
            },
            reducer_params={
                "mime_type": "text/plain",
            },
            shards=16)
