from mapreduce import base_handler
from google.appengine.ext import ndb
import logging

class StoreOutput(base_handler.PipelineBase):
    """A pipeline to store the result of the MapReduce job in the database.

    Args:
      report: Name of the run
      encoded_key: the DB key corresponding to the metadata of this job
      output: the blobstore location where the output of the job is stored
    """
    def run(self, report, ds_key, output):
        logging.debug("output is %s" % str(output))
        dataset = ndb.Key(urlsafe=ds_key).get()
        dataset.output_link = output[0]
        dataset.put()
