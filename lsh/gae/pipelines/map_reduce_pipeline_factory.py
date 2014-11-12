from mapreduce import mapreduce_pipeline

class MapReducePipelineFactory(object):

    def __init__(self, job_name, mapper_spec, reducer_spec,
                 input_reader_spec, output_writer_spec=None,
                 mapper_params=None, reducer_params=None, shards=None):

        self.job_name = job_name
        self.mapper = mapper_spec
        self.reducer = reducer_spec
        self.input = input_reader_spec
        self.output = output_writer_spec
        self.mapper_params = mapper_params
        self.reducer_params = reducer_params
        self.shards = shards

    def create(self):
        return {
            "job_name": self.job_name,
            "mapper": self.mapper,
            "reducer":self.reducer,
            "input":self.input,
            "output":self.output,
            "mapper_params":self.mapper_params,
            "reducer_params":self.reducer_params,
            "shards":self.shards}