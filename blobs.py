import sys, re, math, random, struct, zipfile
import urllib, webapp2
import logging
import operator, time

from google.appengine.ext import blobstore
from google.appengine.ext.webapp import blobstore_handlers
from google.appengine.api import taskqueue
from google.appengine.ext import ndb

from lsh.utils.similarity import compute_positive_hash
from lsh.shingles.shingles import _get_list_of_shingles

max_bits = int(math.log(sys.maxsize+2, 2))

class MainHandler(webapp2.RequestHandler):
    def get(self):
        upload_url = blobstore.create_upload_url('/upload_blob')
        self.response.out.write('<html><body>')
        self.response.out.write('<ol>')

        for blob_info in blobstore.BlobInfo.all():
            blob_key = blob_info.key()
            blob_reader = blobstore.BlobReader(blob_key)
            zip_reader = zipfile.ZipFile(blob_reader)
            url_file_reader = zip_reader.open('url.out')
            line_count = 0
            while url_file_reader.readline():
                line_count += 1
            self.response.out.write('<li><a href="/serve_blob/%s">%s</a> %s %d urls</li>' % (blob_info.key(), blob_info.filename, [('%s: %s' % (p, str(getattr(blob_info, p)))) for p in blob_info._all_properties if p not in ('md5_hash','filename',)], line_count))
            Dataset.create(blob_info.filename, blob_key)

        self.response.out.write('</ol>')
        self.response.out.write('<form action="%s" method="POST" enctype="multipart/form-data">' % upload_url)
        self.response.out.write("""Upload File: <input type="file" name="file"><br> <input type="submit"
            name="submit" value="Submit"> </form></body></html>""")

class UploadHandler(blobstore_handlers.BlobstoreUploadHandler):
    def post(self):
        upload_files = self.get_uploads('file')  # 'file' is file upload field in the form
        blob_info = upload_files[0]
        self.redirect('/serve_blob/%s' % blob_info.key())

class ServeHandler(blobstore_handlers.BlobstoreDownloadHandler):
    def get(self, resource):
        blob_key = str(urllib.unquote(resource))
        blob_reader = blobstore.BlobReader(blob_key)
        zip_reader = zipfile.ZipFile(blob_reader)
        logging.info('contents: %s', zip_reader.namelist())
        url_file_pattern = re.compile('^."id":"([^"]*)","url":"([^"]*)".*')
        lno = 0
        urls = {}
        with zip_reader.open('url.out') as url_file_reader:
            for line in url_file_reader:
                lno += 1
                found_pattern  = url_file_pattern.search(line)
                urls[found_pattern.group(1)] = found_pattern.group(2)
                if lno < 3:
                    logging.info('    line %d: %s: %s', lno, found_pattern.group(1), found_pattern.group(2))
        logging.info('url.out: %d ids', len(urls.keys()))
        taskqueue.add(url='/text_worker', params={'key': blob_key})
        self.redirect('/blobs')

class Dataset(ndb.Model):
    filename = ndb.StringProperty()
    blob_key = ndb.BlobKeyProperty()
    random_seeds = ndb.IntegerProperty(repeated = True)
    
    # The following parameters can be tuned via the Datastore Admin Interface
    rows = ndb.IntegerProperty()
    bands = ndb.IntegerProperty()
    buckets_per_band = ndb.IntegerProperty()
    max_hashes = ndb.IntegerProperty()
    minhash_modulo = ndb.IntegerProperty()
    
    @classmethod
    def create(cls, filename, blob_key, rows=5, bands=40, buckets_per_band=200, max_hashes=200, minhash_modulo=5000):
        dataset = Dataset.query(cls.blob_key == blob_key).get()
        if not dataset:
            dataset = Dataset(filename = filename, 
                              blob_key = blob_key,
                              random_seeds = [random.getrandbits(max_bits) for _ in xrange(max_hashes)],
                              rows = rows,
                              bands = bands,
                              buckets_per_band = buckets_per_band,
                              max_hashes = max_hashes,
                              minhash_modulo = minhash_modulo,
                              )
        else:
            dataset.filename = filename
            dataset.random_seeds = [random.getrandbits(max_bits) for _ in xrange(dataset.max_hashes)]
        return dataset.put()
    
def calc_minhashes(max_hashes, minhash_modulo, random_seeds, shingles, log_count = 0):
    def positive_hash(shingle):
        h = struct.unpack('<i',shingle)[0]
        return  h % ((sys.maxsize + 1) * 2)
    minhashes = [sys.maxsize for _ in xrange(max_hashes)]
    logged = 0
    for shingle in shingles:
        for hno in xrange(max_hashes):
#             h_value = operator.xor(compute_positive_hash(shingle), random_seeds[hno]) 
#            h_value = operator.xor(positive_hash(shingle), random_seeds[hno])
            h_value = operator.xor(positive_hash(shingle), random_seeds[hno]) % minhash_modulo
            minhashes[hno] = min(h_value, minhashes[hno])
        logged += 1
        if logged <= log_count:
            logging.info('mh (%s) = %s', shingle, [minhashes[i] for i in xrange(min(8, max_hashes))])
    return minhashes

class Document(ndb.Model):
    minhashes = ndb.IntegerProperty(repeated = True)
    buckets = ndb.IntegerProperty(repeated = True)

class TextWorker(webapp2.RequestHandler):
    def post(self): # should run at most 1/s due to entity group limit
        def preprocess_text(text):
            # Remove stuff between script tags
            pscript = re.compile('<script.*?>.*?</script.*?>')
            text = pscript.sub('', text.lower())
            # Remove non-alphanumeric characters
            symbols = re.compile('\W+')
            text = symbols.sub(' ', text)
            # Remove spurious white space characters
            text = ' '.join(text.split())
            return text
            
        blob_key = self.request.get('key')
        blob_reader = blobstore.BlobReader(blob_key)
        dataset = Dataset.query(Dataset.blob_key == blobstore.BlobKey(blob_key)).get()
        (max_hashes, minhash_modulo, random_seeds) = (dataset.max_hashes, dataset.minhash_modulo, dataset.random_seeds)
        zip_reader = zipfile.ZipFile(blob_reader)
        text_file_pattern = re.compile('^{"id":"([^"]*):html","text":"(.*)}', flags=re.DOTALL)
        lno = 0

        start = time.time()
        t_shingle = 0
        t_minhash = 0
        with zip_reader.open('text.out') as text_file_reader:
            for line in text_file_reader:
                found_pattern = text_file_pattern.search(line)
                if found_pattern:
                    lno += 1
                    t0 = time.time()
                    text = found_pattern.group(2)
                    orig_len = len(text)
                    text = preprocess_text(text)
                    text_len = len(text)
                    shingles = set(_get_list_of_shingles(text))
                    t1 = time.time()
                    if lno < 3:
                        minhashes = calc_minhashes(max_hashes, minhash_modulo, random_seeds, shingles, 3)
                        logging.info('mh = %s', [minhashes[i] for i in xrange(min(8, dataset.max_hashes))])
                    else:
                        minhashes = calc_minhashes(max_hashes, minhash_modulo, random_seeds, shingles)
                    doc = Document.get_or_insert(found_pattern.group(1), parent = dataset.key)
                    doc.minhashes = minhashes
                    doc.put()
                    t2 = time.time()
                    t_shingle += t1 - t0
                    t_minhash += t2 - t1
                    if lno < 3:
                        logging.info('    line %d: %s orig: %d, pruned %d, shingles %d', 
                                     lno, found_pattern.group(1), orig_len, text_len, len(shingles))
                    end = time.time()
                    if (end - start) > 9*60:
                        break
        logging.info('processed %d ids in %d seconds total, %d secs shingling, %d secs minhashing', 
                     lno, (end - start), t_shingle, t_minhash)
        taskqueue.add(url='/bucketize', params={'key': blob_key})

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

urls = [('/blobs', MainHandler),
        ('/upload_blob', UploadHandler),
        ('/serve_blob/([^/]+)?', ServeHandler),
        ('/text_worker', TextWorker),
        ('/bucketize', Bucketize),
        ]
