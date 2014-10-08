import sys, re, math, random, struct, zipfile
import urllib, webapp2
import logging
import operator, time
import jinja2

from google.appengine.ext import blobstore
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

        if self.request.get("run_lsh"):
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

class ServeHandler(blobstore_handlers.BlobstoreDownloadHandler):
    def get(self, resource):
        blob_key = str(urllib.unquote(resource))
        blob_reader = blobstore.BlobReader(blob_key)
        zip_reader = zipfile.ZipFile(blob_reader)
        logging.info('contents: %s', zip_reader.namelist())
        
        urls = {}
        for lno, mno, _id, text in all_matching_files(zip_reader, 'url.out', url_file_pattern): 
            urls[_id] = text
            if lno < 3:
                logging.info('    match %d (line %d): %s: %s', mno, lno, _id, text)
        logging.info('url.out: %d ids', len(urls.keys()))
        taskqueue.add(url='/text_worker', params={'key': blob_key})
        self.redirect('/blobs')

class Dataset(ndb.Model):
    filename = ndb.StringProperty()
    blob_key = ndb.BlobKeyProperty()
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

class Document(ndb.Model):
    minhashes = ndb.IntegerProperty(repeated = True)
    buckets = ndb.IntegerProperty(repeated = True)

class TextWorker(webapp2.RequestHandler):
    def post(self): # should run at most 1/s due to entity group limit

        def calc_minhashes(_id, text, mno, ds_key, sh_type, hashes, seeds, modulo, match_count = 0):
            symbols = re.compile('\W+')
            ##########################################
            def parse_text(text):
                soup = BeautifulSoup(text.replace('\\n',' '))
                [s.extract() for s in soup(['script', 'style'])]
                text = soup.get_text(separator=' ', strip=True)
                text = symbols.sub(' ', text.lower())
                # Remove spurious white space characters
                text = ' '.join(text.split())
                return text
            ##########################################
            def minhashes_for_shingles(shingles, sh_type, hashes, seeds, modulo):
                def calc_onehash(sh_type, shingle, seed, modulo):
                    def c4_hash(shingle):
                        h = struct.unpack('<i',shingle)[0]
                        return  h % ((sys.maxsize + 1) * 2)
                    if sh_type == 'c4':
                        return operator.xor(c4_hash(shingle), long(seed)) % modulo
                    else:
                        return operator.xor(compute_positive_hash(shingle), long(seed)) % modulo

                minhashes = [sys.maxsize for _ in xrange(hashes)]
                logged = 0
                for shingle in shingles:
                    for hno in xrange(hashes):
                        h_value = calc_onehash(sh_type, shingle, seeds[hno], modulo)
                        minhashes[hno] = min(h_value, minhashes[hno])
                    logged += 1
                    if logged <= match_count:
                        logging.info('mh (%s) = %s', shingle, [minhashes[i] for i in xrange(min(8, hashes))])
                return minhashes
            ##########################################
            t0 = time.time()
            text = parse_text(text)
    
            t1 = time.time()
            shingles = text.split() if sh_type=='w' else set(_get_list_of_shingles(text))
    
            t2 = time.time()
            doc = Document.get_or_insert(_id, parent = ds_key)
            doc.minhashes = minhashes_for_shingles(shingles, sh_type, hashes, seeds, modulo)
            if mno < match_count:
                logging.info('mh = %s', [doc.minhashes[i] for i in xrange(min(8, hashes))])
            doc.put()
    
            t3 = time.time()
            return t0, t1, t2, t3

        blob_key = self.request.get('key')
        blob_reader = blobstore.BlobReader(blob_key)
        dataset = Dataset.query(Dataset.blob_key == blobstore.BlobKey(blob_key)).get()
        zip_reader = zipfile.ZipFile(blob_reader)

        ndb.delete_multi(Document.query(ancestor=dataset.key).fetch(999999, keys_only=True))
        dataset.buckets = []
        dataset.put()

        t_parsing = 0
        t_shingle = 0
        t_minhash = 0
        start = time.time()
        ds_key = dataset.key
        sh_type = dataset.shingle_type
        hashes = dataset.max_hashes
        modulo = dataset.minhash_modulo
        seeds = list(dataset.random_seeds)

        for lno, mno, _id, text in all_matching_files(zip_reader, 'text.out', text_file_pattern):
#             if lno < 2:
#                 logging.info('text = %s', text)
            (t0, t1, t2, t3) = calc_minhashes(_id, text, mno, ds_key, sh_type, hashes, seeds, modulo, match_count = (3 if mno<3 else 0))
            t_parsing += t1 - t0
            t_shingle += t2 - t1
            t_minhash += t3 - t2
            end = time.time()
            if (end - start) > 9*60:
                break
        logging.info('processed %d ids (%d lines) in %d seconds total, %d secs parsing, %d secs shingling, %d secs minhashing', 
                     mno, lno, (end - start), t_parsing, t_shingle, t_minhash)
        taskqueue.add(url='/bucketize', params={'key': blob_key})

logged = {}
symbols = re.compile('\W+')

def lsh_map(data):
    def calc_minhashes(_id, text, ds_key, sh_type, hashes, seeds, modulo):
        ##########################################
        def parse_text(text):
            soup = BeautifulSoup(text.replace('\\n',' '))
            [s.extract() for s in soup(['script', 'style'])]
            text = soup.get_text(separator=' ', strip=True)
            text = symbols.sub(' ', text.lower())
            # Remove spurious white space characters
            text = ' '.join(text.split())
            return text
        ##########################################
        def minhashes_for_shingles(shingles, sh_type, hashes, seeds, modulo):
            def calc_onehash(sh_type, shingle, seed, modulo):
                def c4_hash(shingle):
                    h = struct.unpack('<i',shingle)[0]
                    return  h % ((sys.maxsize + 1) * 2)
                if sh_type == 'c4':
                    return operator.xor(c4_hash(shingle), long(seed)) % modulo
                else:
                    return operator.xor(compute_positive_hash(shingle), long(seed)) % modulo

            minhashes = [sys.maxsize for _ in xrange(hashes)]
            logged = 0
            for shingle in shingles:
                for hno in xrange(hashes):
                    h_value = calc_onehash(sh_type, shingle, seeds[hno], modulo)
                    minhashes[hno] = min(h_value, minhashes[hno])
                logged += 1
            return minhashes
        ##########################################
        text = parse_text(text)
        shingles = text.split() if sh_type=='w' else set(_get_list_of_shingles(text))
        minhashes = minhashes_for_shingles(shingles, sh_type, hashes, seeds, modulo)
        doc = Document.get_or_insert(_id, parent = ds_key)
        doc.minhashes = minhashes
        doc.put()
        return minhashes
    """LSH map function."""
    
#     logging.warning('LSH Map Input %s ', data)
    (blob_key, file_no, line) = (data[0][0], data[0][1], data[1])
    found_pattern = text_file_pattern.search(line)
    if not found_pattern:
        return
    (_id, text) = (found_pattern.group(1), found_pattern.group(2))
    dataset = Dataset.query(Dataset.blob_key == blobstore.BlobKey(blob_key)).get()
    ds_key = dataset.key
    
    time_now = int(time.time())
    start = time.time()
    
    (rows, bands) = (dataset.rows, dataset.bands)
    hashes = rows * bands
    if len(dataset.random_seeds) < hashes:
        dataset.random_seeds = [random.getrandbits(max_bits) for _ in xrange(hashes)]
        dataset.put()
    
    sh_type = dataset.shingle_type
    modulo = dataset.minhash_modulo
    seeds = list(dataset.random_seeds)

    minhashes = calc_minhashes(_id, text, ds_key, sh_type, hashes, seeds, modulo)

    buckets = []
    buckets_per_band = dataset.buckets_per_band
    for band in xrange(dataset.bands):
        minhashes_in_band = [minhashes[band*rows + row] for row in xrange(rows)]
        if len(set(minhashes_in_band)) <= 1:
            buckets.append( (band * buckets_per_band) + hash(minhashes_in_band[0]) % buckets_per_band )

    end = time.time()
    if 0 == (time_now % 20):
        logging.info('id %s, length %d, time %d', _id, len(text), int(end - start))
    
    for bkt in buckets:
        yield (bkt, '/view/%s/%s' % (dataset.filename, _id))

def lsh_bucket(key, values):
    """LSH reduce function."""
    yield (key, values)

class LshPipeline(base_handler.PipelineBase):
    """A pipeline to run LSH.

    Args:
      blobkey: blobkey to process as string. Should be a zip archive with
        text files inside.
    """

    def run(self, filename, blobkey, ds_key):
        logging.warning("filename is %s \nblobkey is %s\nds_key is %s" % (filename, blobkey, ds_key))
#        dataset = Dataset.query(Dataset.blob_key == blobstore.BlobKey(blobkey)).get()
        dataset = ndb.Key(urlsafe=ds_key).get()
        ndb.delete_multi(Document.query(ancestor=dataset.key).fetch(999999, keys_only=True))
        dataset.buckets = []
        dataset.put()
        output = yield mapreduce_pipeline.MapreducePipeline(
            "locality_sensitive_hashing",
            "blobs.lsh_map",
            "blobs.lsh_bucket",
#             'mapreduce.input_readers.BlobstoreZipInputReader'
            'mapreduce.input_readers.BlobstoreZipLineInputReader', 
#             "blobs.ZipInputReaderWithPayload",
            "mapreduce.output_writers.BlobstoreOutputWriter",
            mapper_params={
                "blob_keys": blobkey,
#                 'payload':  ds_key,
            },
            reducer_params={
                "mime_type": "text/plain",
            },
            shards=16)
        yield StoreOutput("LSH", filename, output)

class StoreOutput(base_handler.PipelineBase):
    """A pipeline to store the result of the MapReduce job in the database.

    Args:
      mr_type: the type of mapreduce job run (e.g., WordCount, Index)
      encoded_key: the DB key corresponding to the metadata of this job
      output: the blobstore location where the output of the job is stored
    """

    def run(self, mr_type, encoded_key, output):
        logging.debug("output is %s" % str(output))

class Bucketize(webapp2.RequestHandler):
    def post(self):
        blob_key = self.request.get('key')
        start = time.time()
        dataset = Dataset.query(Dataset.blob_key == blobstore.BlobKey(blob_key)).get()
        doc_count = 0
        all_buckets = []
        for doc in  Document.query(ancestor=dataset.key):
            doc_count += 1
            bucket_locs = []
            minhashes = doc.minhashes
            for band in xrange(dataset.bands):
                minhashes_in_band = [minhashes[band*dataset.rows + row] for row in xrange(dataset.rows)]
                if len(set(minhashes_in_band)) <= 1:
                    bucket_locs.append( (band * dataset.buckets_per_band) + hash(minhashes_in_band[0]) % dataset.buckets_per_band )
            doc.buckets = bucket_locs
            doc.put()
            all_buckets.extend(bucket_locs)
        end = time.time()
        logging.info('%d documents allocated to %d buckets, in %d seconds', 
                     doc_count, len(set(all_buckets)), (end - start))
        dataset.buckets = list(set(all_buckets))
        dataset.put()

class ViewHandler(webapp2.RequestHandler):
    def get(self, dataset_name, file_id):
        def cleanup(text):
            return text.replace('\\n', ' ')
        for blob_info, blob_reader in all_blob_zips():
            if blob_info.filename == dataset_name:
                zip_reader = zipfile.ZipFile(blob_reader)
                for lno, mno, _id, text in all_matching_files(zip_reader, 'text.out', text_file_pattern):
                    if file_id == _id:
                        self.response.out.write(cleanup(text))
                        return
                message = 'ID %s not found' % file_id
                self.response.out.write('<html><body><p>%s</p></body></html>' % message)
                return
        message = 'Blob %s not found' % dataset_name
        self.response.out.write('<html><body><p>%s</p></body></html>' % message)
        return


urls = [('/blobs', MainHandler),
        ('/upload_blob', UploadHandler),
        ('/serve_blob/([^/]+)?', ServeHandler),
        ('/text_worker', TextWorker),
        ('/bucketize', Bucketize),
        ('/view/([^/]+)?/([^/]+)?', ViewHandler),
        ]
