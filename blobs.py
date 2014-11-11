import sys, re, math, random, struct, zipfile, json
import webapp2, hashlib
import logging
import operator, datetime, time
import jinja2

from google.appengine.ext import blobstore
from google.appengine.ext.webapp import blobstore_handlers
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

class Datazz(ndb.Model):
    """
    This class is obsolete.  It was created to understand why ndb stopped working one fine day.
    It should be removed at the next git check in.
    """
    filename = ndb.StringProperty()
    output_link = ndb.StringProperty()

    @classmethod
    def all(cls):
        q = cls.query()
        for ds in q.fetch(10):
            logging.info('Datazz all %s %s', ds.filename, ds.output_link)

    @classmethod
    def create(cls, filename, output_link):
        ndb_datazz = cls.query(Datazz.filename == filename).get()
        if not ndb_datazz:
            ndb_datazz = cls(filename = filename, output_link = output_link)
            ndb_datazz.put()
            # Technically not required, but there's a little bug in the sandbox environment
            time.sleep(0.01)
            logging.info('Datazz new %s %s %s', str(ndb_datazz.key), ndb_datazz.filename, ndb_datazz.output_link)
        else:
            logging.info('Datazz old %s %s %s', str(ndb_datazz.key), ndb_datazz.filename, ndb_datazz.output_link)
            ndb_datazz.output_link = output_link
            ndb_datazz.put()
            # Technically not required, but there's a little bug in the sandbox environment
            time.sleep(0.01)
        logging.info('Datazz final %s %s %s', str(ndb_datazz.key), ndb_datazz.filename, ndb_datazz.output_link)
        return ndb_datazz.key

class MainHandler(webapp2.RequestHandler):
    template_env = jinja2.Environment(loader=jinja2.FileSystemLoader("templates"),
                                      autoescape=True)
    def get(self):
        user = users.get_current_user()
        username = user.nickname()

        items = DatasetPB.all()
#         items = [result for result in results.fetch(10)]
#         for item in items:
#             logging.info('fn %s', item.blob_key)
        length = len(items)
        upload_url = blobstore.create_upload_url("/upload_blob")
        
#         Datazz.create(u'fn1', 'ol1')
#         Datazz.all()
#         Datazz.create('fn1', 'ol2')
#         Datazz.create('fn2', 'ol3')
#         Datazz.all()

        self.response.out.write(self.template_env.get_template("blobs.html").render(
            {"username": username,
             "items": items,
             "length": length,
             "upload_url": upload_url}))

    def post(self):
        filename = self.request.get("filename")
        blob_key = self.request.get("blobkey")
        ds_key   = self.request.get("ds_key")
        output_link   = self.request.get("output_link")

        if self.request.get("run_lsh"):
            pipeline = LshPipeline(filename, blob_key, ds_key)
            pipeline.start()
        elif self.request.get("analyze_output"):
            pipeline = EvalPipeline(output_link[11:])
            pipeline.start()
        elif self.request.get("doc_count"):
            pipeline = CountPipeline(output_link[11:])
            pipeline.start()
        else:
            pass

        self.redirect(pipeline.base_path + "/status?root=" + pipeline.pipeline_id)

class UploadHandler(blobstore_handlers.BlobstoreUploadHandler):
    def post(self):
        upload_files = self.get_uploads("file")
        blob_info = upload_files[0]
        blob_key = blob_info.key()
        logging.info('filename %s key %s', blob_info.filename, blob_key)
        DatasetPB.create(blob_info.filename, blob_key)
        time.sleep(1)

        self.redirect("/blobs")

class DatasetPB(ndb.Model):
    filename = ndb.StringProperty()
    blob_key = ndb.BlobKeyProperty()
    output_link = ndb.StringProperty() 
    result_link = ndb.StringProperty()
    count_link = ndb.StringProperty()
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
               shingle_type='c4', minhash_modulo=4999):
        max_hashes = rows * bands
        dataset = DatasetPB.query(cls.blob_key == blob_key).get()
        if not dataset:
            logging.info('filename %s', filename)
            if filename.find('hello.bg') >= 0:
                # the proper way to do this would be to estimate this number from the average size of documents
                # but this is good enough for now.
                minhash_modulo = 997
            dataset = DatasetPB(filename = filename, 
                              blob_key = blob_key,
                              random_seeds = [random.getrandbits(max_bits) for _ in xrange(max_hashes)],
                              rows = rows,
                              bands = bands,
                              buckets_per_band = buckets_per_band,
                              shingle_type = shingle_type,
                              minhash_modulo = minhash_modulo,
                              )
            dataset.put()
            # Technically not required, but there's a little bug in the sandbox environment
            time.sleep(0.01)
            logging.info('filename stored %s, blob_key %s', dataset.filename, dataset.blob_key)
        else:
            dataset.filename = filename
            dataset.put()
            # Technically not required, but there's a little bug in the sandbox environment
            time.sleep(0.01)
        logging.info('in %s, dataset.blob_key %s', blob_key, dataset.blob_key)
        logging.info('filename in %s', filename)
        logging.info('filename stored %s', dataset.filename)
        cls.all()
        return dataset.key
    @classmethod
    def all(cls):
        items = [result for result in cls.query().fetch()]
        for item in items:
            valnames = vars(item)['_values'].keys()
            logging.info('vals %s', valnames)
            attributes = {}
            for name in valnames:
                try:
                    attributes[name] = getattr(item, name)
                except AttributeError:
                    logging.error('%s: %s', name, '...missing. Check memcache -- it may be serving junk')
            logging.info('Dataset %s', attributes)
        return items

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
    dataset = DatasetPB.query(DatasetPB.blob_key == blobstore.BlobKey(blob_key)).get()
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
        if len(dataset.random_seeds) != hashes:
            dataset.random_seeds = [random.getrandbits(max_bits) for _ in xrange(hashes)]
            logging.warning('Recalculated %d random seeds', hashes)
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

class EvalPipeline(base_handler.PipelineBase):
    def run(self, resource):
        pairs = 0
        with blobstore.BlobReader(resource) as blob_reader:
            for line in blob_reader.readlines():
                # {'33700': ['/view/Mla4PRwYe1ZpNJN4hluceA==/0/1060348/59518bb889e6/mpmoIZY6S4Si89wdEyX9IA', etc ]}
                kdocs = json.loads(line.replace("'", '"'))
                k = kdocs.keys()[0]
                docs = kdocs[k]
                pairs += len(docs) * (len(docs) - 1) / 2
        logging.info('Total number of pairs to compute: %d', pairs)
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
            shards=16)
        yield StoreEvalResults(resource, output)
    def finalized(self):
        pass

def eval_map2(data):
    (offset, line) = data
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
    for h1 in hv:
        for h2 in hv:
            if h1 <= h2: continue
            yield {k: [hv[h1], hv[h2]]}, ""

def eval_reduce2(khv, values):
    def retrieve_doc(v):
        (zip_key, file_no, offset, h, id1) = tuple(v[6:].split('/'))
        dataset = DatasetPB.query(DatasetPB.blob_key == blobstore.BlobKey(zip_key)).get()
        blob_reader = blobstore.BlobReader(zip_key)
        zip_reader = zipfile.ZipFile(blob_reader)
        infolist = zip_reader.infolist()
        zipinfo = infolist[int(file_no)]
        with zip_reader.open(zipinfo) as f:
            f.read(int(offset))
            text = f.readline()
            found_pattern = text_file_pattern.search(text)
            doc = Document(found_pattern.group(1), found_pattern.group(2), dataset)
        shingles = set(doc.shingles())
        minhashes = doc.calc_minhashes()
        return shingles, minhashes, len(doc.text)
    khv = khv.replace("{u'", "{'").replace("[u'", "['").replace(" u'", " '").replace("'", '"')
    try:
        khv2 = json.loads(khv)
    except:
        logging.warning('json.loads failure for %s', khv)
        return
    k = khv2.keys()[0]
    hv = khv2[k]
    v1 = hv[0][0]
    v2 = hv[1][0]
    (shingles1, minhashes1, len1) = retrieve_doc(v1)
    (shingles2, minhashes2, len2) = retrieve_doc(v2)
    jac_txt = float(len(shingles1 & shingles2)) / float(len(shingles1 | shingles2)) 
    jac_min = reduce(lambda x, y: x+y, map(lambda a,b: a == b, minhashes1,minhashes2)) / float(len(minhashes1))
    emitting = {'set1': [str(addr.split('/')[-1]) for addr in hv[0]], 
                'set2': [str(addr.split('/')[-1]) for addr in hv[1]], 
                'mh': jac_min, 
                'sh': jac_txt,
                'len1': len1,
                'len2': len2}
    yield k, (emitting['set1'], emitting['set2'], emitting['mh'], emitting['sh'], emitting['len1'], emitting['len2'])

def eval_map(data):
    def retrieve_doc(v):
        (zip_key, file_no, offset, h, id1) = tuple(v[6:].split('/'))
        dataset = DatasetPB.query(DatasetPB.blob_key == blobstore.BlobKey(zip_key)).get()
        blob_reader = blobstore.BlobReader(zip_key)
        zip_reader = zipfile.ZipFile(blob_reader)
        infolist = zip_reader.infolist()
        zipinfo = infolist[int(file_no)]
        with zip_reader.open(zipinfo) as f:
            f.read(int(offset))
            text = f.readline()
            found_pattern = text_file_pattern.search(text)
            doc = Document(found_pattern.group(1), found_pattern.group(2), dataset)
        shingles = set(doc.shingles())
        minhashes = doc.calc_minhashes()
        return shingles, minhashes, len(doc.text)
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
        (shingles1, minhashes1, len1) = retrieve_doc(v1)
        for h2 in hv:
            if h1 <= h2: continue
            # find the distances between documents in each pair of hashes
            v2 = hv[h2][0]
            (shingles2, minhashes2, len2) = retrieve_doc(v2)
            jac_txt = float(len(shingles1 & shingles2)) / float(len(shingles1 | shingles2)) 
            jac_min = reduce(lambda x, y: x+y, map(lambda a,b: a == b, minhashes1,minhashes2)) / float(len(minhashes1))
            emitting = {'set1': [str(addr) for addr in hv[h1]], 
                        'set2': [str(addr) for addr in hv[h2]], 
                        'mh': jac_min, 
                        'sh': jac_txt,
                        'len1': len1,
                        'len2': len2}
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
        dataset = DatasetPB.query(DatasetPB.output_link == '/blobstore/'+resource).get()
        dataset.result_link = output[0]
        dataset.put()
        return

    def finalized(self):
        logging.info('StoreEvalResults finalized')

class CountPipeline(base_handler.PipelineBase):
    def run(self, resource):
        output = yield mapreduce_pipeline.MapreducePipeline(
            "results-count",
            "blobs.count_map",
            "blobs.count_reduce",
            'mapreduce.input_readers.BlobstoreLineInputReader', 
            "mapreduce.output_writers.BlobstoreOutputWriter",
            mapper_params={
                "blob_keys": resource,
            },
            reducer_params={
                "mime_type": "text/plain",
            },
            shards=6)
        yield StoreCountResults(resource, output)
    def finalized(self):
        pass

def count_map(data):
    (offset, line) = data
    kv = json.loads(line.replace("'", '"'))
    k = kv.keys()[0]
    vs = kv[k]
    for v in vs:
        yield (v, "")

def count_reduce(key, values):
    yield "%s: %d\n" % (key, len(values))

class StoreCountResults(base_handler.PipelineBase):
    """A pipeline to store the result of the Analysis job in the database.

    Args:
      encoded_key: the DB key corresponding to the metadata of this job
      output: the blobstore location where the output of the job is stored
    """
    def run(self, resource, output):
        logging.info("resource is %s, output is %s", resource, str(output))
        dataset = DatasetPB.query(DatasetPB.output_link == '/blobstore/'+resource).get()
        dataset.count_link = output[0]
        dataset.put()
        return

    def finalized(self):
        logging.info('StoreCountResults finalized')

urls = [('/blobs', MainHandler),
        ('/upload_blob', UploadHandler),
        ('/view/([^/]+)?/([^/]+)?/([^/]+)?/([^/]+)?/([^/]+)?', ViewHandler),
        ]
