import sys, os, re, time, math, random, struct, zipfile, operator
sys.path.insert(0, 'libs')

import logging

LOG_FILENAME = '/home/ubuntu/CassDriver.log'
logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG)


from lsh.shingles.shingles import _get_list_of_shingles
from lsh.utils.similarity import compute_positive_hash
from bs4 import BeautifulSoup

from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement, dict_factory
from cassandra import ConsistencyLevel, InvalidRequest

logging.info('CassDriver path %s', sys.path)
max_bits = int(math.log(sys.maxsize+2, 2))
text_file_pattern = re.compile('^{"id":"([^"]*):html","text":"(.*)}', flags=re.DOTALL)
symbols = re.compile('\W+')

class UnableToCreateTable(Exception):
    pass
class UnknownException(Exception):
    pass

class CassandraTable(type):
    """
    A singleton metaclass to ensure that the table exists in Cassandra
    Inspired by http://stackoverflow.com/questions/6760685/creating-a-singleton-in-python
    """
    _instances = {}
    def __call__(cls, *args, **kwds):
        if cls not in cls._instances:
            try:
                rows = session.execute('SELECT COUNT(*) FROM {name}'.format(name = kwds['name']))
                logging.debug('Table %s exists', kwds['name'])
            except InvalidRequest as err:
                remsg = re.compile(r'code=(\d*).*')
                found = remsg.search(err.message)
                code = int('0'+found.group(1))
                if code == 2200:
                    qstring = 'create table {name} ( {attrs} )'.format(name = kwds['name'], attrs = ', '.join(kwds['attrs']))
                    try:
                        session.execute(qstring)
                    except:
                        raise UnableToCreateTable(kwds['name'])
                else:
                    raise UnknownException()
                logging.debug('Table %s was created', kwds['name'])
            cls._instances[cls] = super(CassandraTable, cls).__call__(*args, **{})
        return cls._instances[cls]

class DatasetPB(object):
    __metaclass__ = CassandraTable
    attrs = [
             'ds_key text primary key',
             'source text',
             'filename text',
             'lsh_output text',
             'eval_output text',
             'count_output text',
             'random_seeds list<bigint>',
             'buckets list<int>',
             'rows int',
             'bands int',
             'buckets_per_band int',
             'shingle_type ascii',
             'minhash_modulo int',
             ]

    def __init__(self):
        qry = "SELECT * FROM {name} WHERE ds_key=?".format(name = self.__class__.__name__)
        self.select = session.prepare(qry)
        self.select.consistency_level = ConsistencyLevel.QUORUM
        doc = Document(name = Document.__class__.__name__, attrs = Document.attrs)
        self.doc_query = "SELECT * FROM Document WHERE ds_key=? AND doc_id=?"
        self.doc_select = session.prepare(self.doc_query)
        self.bkt_query = "SELECT buckets FROM Document WHERE ds_key=? AND doc_id=?"
        self.bkt_select = session.prepare(self.bkt_query)
        self.nns_query = "SELECT doc_id, minhashes FROM Document WHERE ds_key=? AND buckets CONTAINS ?"
        self.nns_select = session.prepare(self.nns_query)
        self.doc_ids_query = "SELECT doc_id FROM Document WHERE ds_key=? ALLOW FILTERING"
        self.doc_ids_select = session.prepare(self.doc_ids_query)

    def get(self, ds_key):
        if ds_key:
            ds = session.execute(self.select, [ds_key])
            try:
                if len(ds) == 1:
                    return ds[0]
            except:
                pass
            return ds
    
    @classmethod
    def find(cls, ds_key):
        ds = DatasetPB(name = cls.__name__, attrs = cls.attrs)
        dataset = ds.get(ds_key)
        for k in dataset.keys():
            setattr(ds, k, dataset[k])
        return ds

    @classmethod
    def create(cls, source, filename,  
               rows=5, bands=350, buckets_per_band=100, 
               shingle_type='c4', minhash_modulo=701):

        # Make sure the underlying tables exist
        ds = DatasetPB(name = cls.__name__, attrs = cls.attrs)

        max_iters = 4
        for iter_count in xrange(max_iters):
            ds_key = '%04d' % (abs(hash(source + filename + ' ' * iter_count)) % (10 ** 4))
            try:
                # Does a dataset with this ID already exist?
                this_ds = ds.get(ds_key)
                if not this_ds:
                    break
                if this_ds['filename'] == filename:
                    logging.debug("A dataset with %s already exists, reusing", filename)
                    for k in this_ds.keys():
                        setattr(ds, k, this_ds[k])
                    return ds
            except ValueError:
                raise Exception('WTF?')
        ds.ds_key = ds_key
        if iter_count == max_iters - 1:
            raise Exception("Unable to create Dataset ID")
        max_hashes = rows * bands
        data = {
                'ds_key': "'%s'" % ds_key,
                'source': "'%s'" % source,
                'filename': "'%s'" % filename,
                'random_seeds': str([random.getrandbits(max_bits) for _ in xrange(max_hashes)]).replace('L',''),
                'rows': rows,
                'bands': bands,
                'buckets_per_band': buckets_per_band,
                'shingle_type': "'%s'" % shingle_type,
                'minhash_modulo': minhash_modulo,
                }
        data_keys = data.keys()
        data_vals = ', '.join([str(data[k]) for k in data_keys])
        data_keys = ', '.join(data_keys)
        qstring = 'INSERT INTO %s (%s) VALUES (%s)' % (cls.__name__, data_keys, data_vals)
        query = SimpleStatement(qstring, consistency_level=ConsistencyLevel.QUORUM)
        session.execute(query)
        return cls.find(ds_key)

    def get_else_create_doc(self, doc_id):
        try:
            docs = session.execute(self.doc_select, [self.ds_key, doc_id])
            if len(docs) == 1:
                return True, docs[0]
        except: 
            pass
        doc = Document(name = 'Document', attrs = Document.attrs)
        doc.ds_key = self.ds_key
        doc.doc_id = doc_id
        return False, doc

    def get_doc(self, doc_id):
        try:
            docs = session.execute(self.doc_select, [self.ds_key, doc_id])
            if len(docs) == 1:
                doc = Document(name = 'Document', attrs = Document.attrs)
                doc.ds_key = self.ds_key
                doc.doc_id = doc_id
                ret_dict = docs[0]
                for k in ret_dict.keys():
                    setattr(doc, k, ret_dict[k])
                return doc
        except:
            pass
        return None

    def get_nns(self, doc_id):
        doc = self.get_doc(doc_id)
        if not doc:
            return []
        bkts = doc.buckets
        mhs = {}
        for bkt in bkts:
            bkt_docs = session.execute(self.nns_select, [self.ds_key, bkt])
            for bkt_doc in bkt_docs:
                mhs[bkt_doc['doc_id']] = bkt_doc['minhashes']
        del mhs[doc_id]
        jac = {}
        for doc_id2 in mhs.keys():
            jac_min = reduce(lambda x, y: x+y, map(lambda a,b: a == b, doc.minhashes,mhs[doc_id2])) / float(len(doc.minhashes))
            jac[doc_id2] = 1.0 - jac_min
            if 0 == int(1000*time.time()) % 100:
                logging.info('Sampling (1%%) Jaccard distance %s | %s: %6.2f', doc_id, doc_id2, jac[doc_id2])
        return jac

    def sample_doc_ids(self, ratio):
        doc_ids = session.execute(self.doc_ids_select, [self.ds_key])
        doc_ids = random.sample(doc_ids, int(0.5+ratio*len(doc_ids)))
        return [_['doc_id'] for _ in doc_ids]

    def create_doc(self, _id, text):
        (found, doc) = self.get_else_create_doc(_id)
        if found:
            if 0 == int(1000*time.time()) % 20:
                # print 5% of the documents on average
                logging.info('%s %s %s %s',doc['ds_key'], doc['doc_id'], doc['bucket_count'], doc['buckets'])
            return doc

        ### Parse
        soup = BeautifulSoup(text.replace('\\n',' '))
        [s.extract() for s in soup(['script', 'style'])]
        text = soup.get_text(separator=' ', strip=True)
        text = symbols.sub(' ', text.lower())
        text = ' '.join(text.split())
        doc.text = text
        doc.dataset = self
        doc.rows = self.rows
        doc.hashes = doc.rows * self.bands
        doc.seeds = list(self.random_seeds)
        doc.modulo = self.minhash_modulo
        doc.buckets_per_band = self.buckets_per_band
        doc.sh_type = self.shingle_type

        max_hashes = self.rows * self.bands
        doc.minhashes = doc.calc_minhashes()

        doc.buckets = doc.bucketize()
        doc.bucket_count = len(doc.buckets)
        if 0 == int(1000*time.time()) % 20:
            # print 5% of the documents on average
            logging.info('%s %s %s %s', doc.ds_key, doc.doc_id, doc.bucket_count, doc.buckets)
        data = {
                'ds_key': "'%s'" % doc.ds_key,
                'doc_id': "'%s'" % doc.doc_id,
                'html': "'%s'" % text[:100],
                'minhashes': str(doc.minhashes).replace('L',''),
                'buckets': str(doc.buckets).replace('L',''),
                'bucket_count': str(len(doc.buckets))
                }
        data_keys = data.keys()
        data_vals = ', '.join([str(data[k]) for k in data_keys])
        data_keys = ', '.join(data_keys)
        qstring = 'INSERT INTO %s (%s) VALUES (%s)' % ('Document', data_keys, data_vals)
        document = session.execute(qstring)
        return document

class Document(object):
    __metaclass__ = CassandraTable
    attrs = [
             'ds_key text',
             'doc_id text',
             'html text',
             'buckets list<int>',
             'minhashes list<int>',
             'bucket_count int',
             'PRIMARY KEY (doc_id, ds_key)',
             ]

    @classmethod
    def create(cls):
        # Make sure the underlying tables exist
        doc = Document(name = cls.__name__, attrs = cls.attrs)
        query = 'create index if not exists doc_buckets on %s.Document (buckets)' % keyspace
        session.execute(query)

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

def main():
    """
    Read input zip file, minhash the documents in it and put them in buckets
    The zip file should have been created with data_prep/prepare_blobstore_zips
    """
    try:
        filename = os.path.abspath(sys.argv[1])
    except IndexError:
        print 'filename not provided'
        exit(1)
    try:
        zip_reader = zipfile.ZipFile(filename)
    except IOError:
        print 'unable to read file {file}'.format(file = filename)
        exit(1)
    except zipfile.BadZipfile:
        print 'file {file} is not a zip file'.format(file = filename)
        exit(1)

    infolist = zip_reader.infolist()
    dummydoc = Document.create()            # force the creation of the table
    dataset = DatasetPB.create('bash', filename)    # force the creation of the table and filling it with a row
    logging.debug('%s %s', dataset.ds_key, dataset.filename)
    dataset = DatasetPB.find(dataset.ds_key)
    start = time.time()
    for info in infolist:
        with zip_reader.open(info) as file_reader:
            logging.debug('Reading file %s', info.filename)
            for line in file_reader.readlines():
                found_pattern = text_file_pattern.search(line)
                doc_id = found_pattern.group(1)
                html = found_pattern.group(2)
                udata=html.decode("utf-8")
                html=udata.encode("ascii","ignore")
                html = html.replace('\\n',' ').replace('\\t',' ').replace("'", "''")
                dataset.create_doc(doc_id, html)
            end = time.time()
            logging.info('File %s %d seconds', info.filename, int(0.5+end-start))
            start = end 

cluster = Cluster()
keyspace = 'jkeyspace'
session = cluster.connect(keyspace)
session.row_factory = dict_factory

if __name__ == "__main__":
    main()
