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
    def get(self):
        upload_url = blobstore.create_upload_url('/upload_blob')
        self.response.out.write('<html><body>')
        self.response.out.write('<ol>')

        for blob_info, blob_reader in all_blob_zips():
            try:
                zip_reader = zipfile.ZipFile(blob_reader)
                for lno, mno, _id, text in all_matching_files(zip_reader, 'url.out', url_file_pattern): 
                    pass
            except zipfile.BadZipfile:
                logging.warning('Bad zip: %s', blob_info.filename)
                continue
            except KeyError:
                logging.warning('Missing url.out file in the archive: %s', blob_info.filename)
                continue
            dataset = Dataset.create(blob_info.filename, blob_info.key())
            self.response.out.write('<li><a href="/serve_blob/%s">%s</a> %s %d urls</li>' % (blob_info.key(), blob_info.filename, [('%s: %s' % (p, str(getattr(blob_info, p)))) for p in blob_info._all_properties if p not in ('md5_hash','filename',)], lno))

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
    max_hashes = ndb.IntegerProperty()
    shingle_type = ndb.StringProperty(choices=('w', 'c4'))
    minhash_modulo = ndb.IntegerProperty()
    
    @classmethod
    def create(cls, filename, blob_key, 
               rows=7, bands=28, buckets_per_band=100, max_hashes=198, 
               shingle_type='c4', minhash_modulo=5000):
        dataset = Dataset.query(cls.blob_key == blob_key).get()
        if not dataset:
            dataset = Dataset(filename = filename, 
                              blob_key = blob_key,
                              random_seeds = [random.getrandbits(max_bits) for _ in xrange(max_hashes)],
                              rows = rows,
                              bands = bands,
                              buckets_per_band = buckets_per_band,
                              max_hashes = max_hashes,
                              shingle_type = shingle_type,
                              minhash_modulo = minhash_modulo,
                              )
        else:
            dataset.filename = filename
            if len(dataset.random_seeds) != dataset.max_hashes:
                dataset.random_seeds = [random.getrandbits(max_bits) for _ in xrange(dataset.max_hashes)]
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
