import sys, re, math, random, struct, zipfile, json
import urllib, webapp2, hashlib
import logging
import operator, datetime, time
import jinja2

from google.appengine.ext import blobstore
from google.appengine.api import files
from google.appengine.ext.webapp import blobstore_handlers
from google.appengine.api import taskqueue
from google.appengine.ext import ndb
from google.appengine.api import users

from mapreduce import base_handler
from mapreduce import mapreduce_pipeline

from lsh.utils.similarity import compute_positive_hash
from lsh.shingles.shingles import _get_list_of_shingles

sys.path.insert(0, 'libs')
from bs4 import BeautifulSoup

max_bits = int(math.log(sys.maxsize+2, 2))
url_file_pattern = re.compile('^."id":"([^"]*)","url":"([^"]*)".*')
text_file_pattern = re.compile('^{"id":"([^"]*):html","text":"(.*)}', flags=re.DOTALL)
symbols = re.compile('\W+')

def all_blob_zips():
    for blob_info in blobstore.BlobInfo.all():
        blob_key = blob_info.key()
        blob_reader = blobstore.BlobReader(blob_key)
        yield blob_info, blob_reader

def all_matching_files(zip_reader, filename, pattern):
    with zip_reader.open(filename) as file_reader:
        (lno, mno) = (0, 0,)
        for line in file_reader:
            found_pattern = pattern.search(line)
            lno += 1
            if found_pattern:
                mno += 1
                yield lno, mno, found_pattern.group(1), found_pattern.group(2)

class MainHandler(webapp2.RequestHandler):
    template_env = jinja2.Environment(loader=jinja2.FileSystemLoader("templates"),
                                      autoescape=True)
    def get(self):
        user = users.get_current_user()
        username = user.nickname()

        results = Dataset.query()
        items = [result for result in results]
        for item in items:
            item.ds_key = item.key.urlsafe()
        length = len(items)
        upload_url = blobstore.create_upload_url("/upload_blob")

        self.response.out.write(self.template_env.get_template("blobs.html").render(
            {"username": username,
             "items": items,
             "length": length,
             "upload_url": upload_url}))

    def post(self):
        filename = self.request.get("filename")
        blob_key = self.request.get("blobkey")
        ds_key   = self.request.get("ds_key")

        pipeline = LshPipeline(filename, blob_key, ds_key)
        pipeline.start()
        self.redirect(pipeline.base_path + "/status?root=" + pipeline.pipeline_id)

class UploadHandler(blobstore_handlers.BlobstoreUploadHandler):
    def post(self):
        upload_files = self.get_uploads("file")
        blob_info = upload_files[0]
        blob_key = blob_info.key()
        logging.info('filename %s key %s', blob_info.filename, blob_key)
        Dataset.create(blob_info.filename, blob_key)

        self.redirect("/blobs")

class Dataset(ndb.Model):
    filename = ndb.StringProperty()
    blob_key = ndb.BlobKeyProperty()
    output_link = ndb.StringProperty()
    result_link = ndb.StringProperty()
    random_seeds = ndb.IntegerProperty(repeated = True)
    buckets = ndb.IntegerProperty(repeated = True)
    
    # The following parameters can be tuned via the Datastore Admin Interface
    rows = ndb.IntegerProperty()
    bands = ndb.IntegerProperty()
    buckets_per_band = ndb.IntegerProperty()
    shingle_type = ndb.StringProperty(choices=('w', 'c4'))
    minhash_modulo = ndb.IntegerProperty()
    
    @classmethod
    def create(cls, filename, blob_key, 
               rows=5, bands=40, buckets_per_band=100, 
               shingle_type='c4', minhash_modulo=5000):
        max_hashes = rows * bands
        dataset = Dataset.query(cls.blob_key == blob_key).get()
        if not dataset:
            dataset = Dataset(filename = filename, 
                              blob_key = blob_key,
                              random_seeds = [random.getrandbits(max_bits) for _ in xrange(max_hashes)],
                              rows = rows,
                              bands = bands,
                              buckets_per_band = buckets_per_band,
                              shingle_type = shingle_type,
                              minhash_modulo = minhash_modulo,
                              )
        else:
            dataset.filename = filename
        return dataset.put()

class Document(object):
    def __init__(self, _id, text, dataset):
        ### Parse
        self._id = _id
        soup = BeautifulSoup(text.replace('\\n',' '))
        [s.extract() for s in soup(['script', 'style'])]
        text = soup.get_text(separator=' ', strip=True)
        text = symbols.sub(' ', text.lower())
        text = ' '.join(text.split())
        self.text = text
        self.dataset = dataset
        self.rows = dataset.rows
        self.hashes = self.rows * dataset.bands
        self.seeds = list(dataset.random_seeds)
        self.modulo = dataset.minhash_modulo
        self.buckets_per_band = dataset.buckets_per_band
        self.sh_type = dataset.shingle_type
        self.url = ''

    def calc_minhashes(self):
        def minhashes_for_shingles(shingles):
            def calc_onehash(shingle, seed):
                def c4_hash(shingle):
                    h = struct.unpack('<i',shingle)[0]
                    return  h % ((sys.maxsize + 1) * 2)
                if self.sh_type == 'c4':
                    return operator.xor(c4_hash(shingle), long(seed)) % self.modulo
                else:
                    return operator.xor(compute_positive_hash(shingle), long(seed)) % self.modulo

            minhashes = [sys.maxsize for _ in xrange(self.hashes)]
            for shingle in shingles:
                for hno in xrange(self.hashes):
                    h_value = calc_onehash(shingle, self.seeds[hno])
                    minhashes[hno] = min(h_value, minhashes[hno])
            return minhashes
        ##########################################
        shingles = self.shingles()
        minhashes = minhashes_for_shingles(shingles)
        return minhashes
    
    def shingles(self):
        return self.text.split() if self.sh_type=='w' else set(_get_list_of_shingles(self.text))
    
    def bucketize(self):
        buckets = []
        minhashes = self.calc_minhashes()
        for band in xrange(self.dataset.bands):
            minhashes_in_band = [minhashes[band*self.rows + row] for row in xrange(self.rows)]
            if len(set(minhashes_in_band)) <= 1:
                buckets.append( (band * self.buckets_per_band) + hash(minhashes_in_band[0]) % self.buckets_per_band )
        return buckets

def lsh_map(data):
    """
    LSH map function.
    Emit a bucket number and enough about the document to be able to quickly extract it from the blobstore
    Also embed an MD5 hash in it. 
        The MD5 hash is used for quick comparisons of identical documents.
        Avoids having to shingle and minhash two documents if we already know they are identical.
    """
    (blob_key, line) = (data[0][0], data[1])
    found_pattern = text_file_pattern.search(line)
    if not found_pattern:
        return
    dataset = Dataset.query(Dataset.blob_key == blobstore.BlobKey(blob_key)).get()
    document = Document(found_pattern.group(1), found_pattern.group(2), dataset)
    
    start = datetime.datetime.utcnow()
    buckets = document.bucketize()
    end = datetime.datetime.utcnow()
    if 0 == (start.second % 10):
        logging.info('data[0] %s, id %s, length %d, time %d', data[0], document._id, len(document.text), int((end-start).total_seconds()))
    
    for bkt in buckets:
        yield (bkt, '/view/%s/%s/%s/%s/%s' % (data[0][0], data[0][1], data[0][2], hashlib.md5(document.text).hexdigest()[:12], document._id))

def lsh_bucket(key, values):
    """LSH reduce function."""
    yield '%s\n' % str({key: values})

class LshPipeline(base_handler.PipelineBase):
    """A pipeline to run LSH.

    Args:
      blobkey: blobkey to process as string. Should be a zip archive with
        text files inside.
    """

    def run(self, filename, blobkey, ds_key):
        params = "filename %s \tblobkey %s\tds_key %s" % (filename, blobkey, ds_key)
        logging.info(params)

        dataset = ndb.Key(urlsafe=ds_key).get()
        rows = dataset.rows
        hashes = rows * dataset.bands
        if len(dataset.random_seeds) < hashes:
            dataset.random_seeds = [random.getrandbits(max_bits) for _ in xrange(hashes)]
            dataset.put()
    
        dataset.buckets = []
        dataset.put()
        output = yield mapreduce_pipeline.MapreducePipeline(
            "locality_sensitive_hashing",
            "blobs.lsh_map",
            "blobs.lsh_bucket",
            'mapreduce.input_readers.BlobstoreZipLineInputReader', 
            "mapreduce.output_writers.BlobstoreOutputWriter",
            mapper_params={
                "blob_keys": blobkey,
            },
            reducer_params={
                "mime_type": "text/plain",
            },
            shards=16)
        yield StoreLshResults('OpenLSH', blobkey, ds_key, output)
    def finalized(self):
        pass

class StoreLshResults(base_handler.PipelineBase):
    """A pipeline to store the result of the MapReduce job in the database.

    Args:
      report: Name of the run
      encoded_key: the DB key corresponding to the metadata of this job
      output: the blobstore location where the output of the job is stored
    """
    def run(self, report, blobkey, ds_key, output):
        logging.info("blobkey is %s, output is %s", blobkey, str(output))
        dataset = ndb.Key(urlsafe=ds_key).get()
        dataset.output_link = output[0]
        dataset.result_link = ''
        dataset.put()
        return

    def finalized(self):
        logging.info('StoreLshResults finalized')

class ViewHandler(webapp2.RequestHandler):
    def get(self, zip_key, file_no, offset, md5, _id):
        def cleanup(text):
            return text.replace('\\n', ' ').replace('\\"', '"').replace('http://inventures.euhttp://inventures.eu/', 'http://inventures.eu/')
        blob_reader = blobstore.BlobReader(zip_key)
        zip_reader = zipfile.ZipFile(blob_reader)
        infolist = zip_reader.infolist()
        zipinfo = infolist[int(file_no)]
        with zip_reader.open(zipinfo) as f:
            f.read(int(offset))
            text = f.readline()
            found_pattern = text_file_pattern.search(text)
            html = found_pattern.group(2)#.replace('/sites/','http://inventures.eu/sites/' )
            self.response.out.write(cleanup(html))
            return
        message = 'ID %s not found' % _id
        self.response.out.write('<html><body><p>%s</p></body></html>' % message)
        return

class DistanceHandler(blobstore_handlers.BlobstoreDownloadHandler):
    def get(self, resource):
        pipeline = EvalPipeline(resource)
        pipeline.start()
        self.redirect(pipeline.base_path + "/status?root=" + pipeline.pipeline_id)

class EvalPipeline(base_handler.PipelineBase):
    def run(self, resource):
        output = yield mapreduce_pipeline.MapreducePipeline(
            "results-eval",
            "blobs.eval_map",
            "blobs.eval_reduce",
            'mapreduce.input_readers.BlobstoreLineInputReader', 
            "mapreduce.output_writers.BlobstoreOutputWriter",
            mapper_params={
                "blob_keys": resource,
            },
            reducer_params={
                "mime_type": "text/plain",
            },
            shards=2)
        yield StoreEvalResults(resource, output)
    def finalized(self):
        pass

def eval_map(data):
    (offset, line) = data
    start = time.time()
#     logging.info('eval %s',line)
    kv = json.loads(line.replace("'", '"'))
    k = kv.keys()[0]
    vs = kv[k]
    hv = {}
    for v in vs:
        h = v.split('/')[-2]
        if h not in hv:
            hv[h] = [v]
        else:
            hv[h] += [v]
#     logging.info({k: hv})
    if len(hv.keys()) == 1:
        # no pairs will be found
        return
    number_of_pairs_processed = 0
    for h1 in hv:
        v1 = hv[h1][0]
        (zip_key, file_no, offset, h, id1) = tuple(v1[6:].split('/'))
        dataset = Dataset.query(Dataset.blob_key == blobstore.BlobKey(zip_key)).get()
        blob_reader = blobstore.BlobReader(zip_key)
        zip_reader = zipfile.ZipFile(blob_reader)
        infolist = zip_reader.infolist()
        zipinfo = infolist[int(file_no)]
        with zip_reader.open(zipinfo) as f:
            f.read(int(offset))
            text = f.readline()
            found_pattern = text_file_pattern.search(text)
            doc1 = Document(found_pattern.group(1), found_pattern.group(2), dataset)
        shingles1 = set(doc1.shingles())
        minhashes1 = doc1.calc_minhashes()
        for h2 in hv:
            if h1 <= h2: continue
            # find the distances between documents in each pair of hashes
            v2 = hv[h2][0]
            (zip_key, file_no, offset, h, id2) = tuple(v2[6:].split('/'))
            dataset = Dataset.query(Dataset.blob_key == blobstore.BlobKey(zip_key)).get()
            blob_reader = blobstore.BlobReader(zip_key)
            zip_reader = zipfile.ZipFile(blob_reader)
            infolist = zip_reader.infolist()
            zipinfo = infolist[int(file_no)]
            with zip_reader.open(zipinfo) as f:
                f.read(int(offset))
                text = f.readline()
                found_pattern = text_file_pattern.search(text)
                doc2 = Document(found_pattern.group(1), found_pattern.group(2), dataset)
            shingles2 = set(doc2.shingles())
            minhashes2 = doc2.calc_minhashes()
            jac_txt = float(len(shingles1 & shingles2)) / float(len(shingles1 | shingles2)) 
            jac_min = reduce(lambda x, y: x+y, map(lambda a,b: a == b, minhashes1,minhashes2)) / float(len(minhashes1))
            emitting = {'set1': [str(addr.split('/')[-1]) for addr in hv[h1]], 
                        'set2': [str(addr.split('/')[-1]) for addr in hv[h2]], 
                        'mh': jac_min, 
                        'sh': jac_txt,
                        'len1': len(doc1.text),
                        'len2': len(doc2.text)}
#             row = '{set1} {set2} mh: {mh:.3f} sh: {sh:.3f} {len1:} {len2:}'.format(**emitting)
            yield k, (emitting['set1'], emitting['set2'], emitting['mh'], emitting['sh'], emitting['len1'], emitting['len2'])
#             yield k, row
            
            # we will only allocate 5 minutes for this map function. Save what we have by then and move on.
            number_of_pairs_processed += 1
            end = time.time()
            if (end - start) > 5*60:
                total_docs = len(hv.keys())
                total_pairs = total_docs * (total_docs - 1) / 2
                logging.warn('Abandoning map after %d seconds (%d of %d pairs processed)', (end - start), number_of_pairs_processed, total_pairs)
                return

def eval_reduce(key, values):
    yield (key, values)

class StoreEvalResults(base_handler.PipelineBase):
    """A pipeline to store the result of the Analysis job in the database.

    Args:
      encoded_key: the DB key corresponding to the metadata of this job
      output: the blobstore location where the output of the job is stored
    """
    def run(self, resource, output):
        logging.info("resource is %s, output is %s", resource, str(output))
        dataset = Dataset.query(Dataset.output_link == '/blobstore/'+resource).get()
        dataset.result_link = output[0]
        dataset.put()
        return

    def finalized(self):
        logging.info('StoreEvalResults finalized')

urls = [('/blobs', MainHandler),
        ('/upload_blob', UploadHandler),
        ('/view/([^/]+)?/([^/]+)?/([^/]+)?/([^/]+)?/([^/]+)?', ViewHandler),
        ('/calc_dists/blobstore/([^/]+)?', DistanceHandler),
        ]
