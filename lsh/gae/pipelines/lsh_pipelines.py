import logging
from google.appengine.ext import ndb
from lsh.gae.store_output import StoreOutput
from mapreduce import base_handler
from mapreduce import mapreduce_pipeline


class LshBlobPipeline(base_handler.PipelineBase):
    """A pipeline to run LSH that reads and writes to blobstore

      :param blobkey: blobkey to process as string. Should be a zip archive with
      text files inside.
    """

    def run(self, filename, blobkey, ds_key, map_reduce_pipeline):
        logging.info("OpenLSH > LshBlobPipeline.run() called.")
        self.params = "filename %s \tblobkey %s\tds_key %s" % (filename, blobkey, ds_key)

        logging.warning(self.params)

        dataset = ndb.Key(urlsafe=ds_key).get()
        dataset.buckets = []
        dataset.put()
        output = yield mapreduce_pipeline.MapreducePipeline(
            map_reduce_pipeline.get("job_name"),
            map_reduce_pipeline.get("mapper"),
            map_reduce_pipeline.get("reducer"),
            map_reduce_pipeline.get("input"),
            map_reduce_pipeline.get("output"),
            mapper_params= map_reduce_pipeline.get("mapper_params"),
            reducer_params=map_reduce_pipeline.get("reducer_params"),
            shards=map_reduce_pipeline.get("shards"))

        yield StoreOutput('OpenLSH', ds_key, output)

    def finalized(self):
        pass

