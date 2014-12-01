import sys, os, re, time, math, random, struct, zipfile, operator
sys.path.insert(0, 'libs')
# sys.path.insert(0, 'lsh')
print sys.path
from lsh.shingles.shingles import _get_list_of_shingles
from lsh.utils.similarity import compute_positive_hash
from bs4 import BeautifulSoup

from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement, dict_factory
from cassandra import ConsistencyLevel, InvalidRequest

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
                print 'Table', kwds['name'], 'exists'
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
                print 'Table', kwds['name'], 'was created'
            cls._instances[cls] = super(CassandraTable, cls).__call__(*args, **{})
        return cls._instances[cls]

class DatasetPB(object):
    __metaclass__ = CassandraTable
    attrs = [
             'ds_key text primary key',
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
        self.doc_select.consistency_level = ConsistencyLevel.QUORUM

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
    def create(cls, filename,  
               rows=5, bands=350, buckets_per_band=100, 
               shingle_type='c4', minhash_modulo=701):

        # Make sure the underlying tables exist
        ds = DatasetPB(name = cls.__name__, attrs = cls.attrs)

        max_iters = 4
        for iter_count in xrange(max_iters):
            ds_key = '%04d' % (abs(hash(filename + ' ' * iter_count)) % (10 ** 4))
            try:
                # Does a dataset with this ID already exist?
                this_ds = ds.get(ds_key)
                if not this_ds:
                    break
                if this_ds['filename'] == filename:
                    print "A dataset with filename {filename} already exists, reusing".format(filename = filename)
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

    def create_doc(self, _id, text):
        (found, doc) = self.get_else_create_doc(_id)
        if found:
            if 0 == int(1000*time.time()) % 20:
                # print 5% of the documents on average
                print doc['ds_key'], doc['doc_id'], doc['bucket_count'], doc['buckets']
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
            print doc.ds_key, doc.doc_id, doc.bucket_count, doc.buckets
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
             'PRIMARY KEY (ds_key, doc_id)',
             ]

    @classmethod
    def create(cls):
        # Make sure the underlying tables exist
        doc = Document(name = cls.__name__, attrs = cls.attrs)

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
    dataset = DatasetPB.create(filename)    # force the creation of the table and filling it with a row
    print dataset.ds_key, dataset.filename
#     print dataset['ds_key'], dataset['filename']
    dataset = DatasetPB.find(dataset.ds_key)
    start = time.time()
    for info in infolist:
        with zip_reader.open(info) as file_reader:
            print 'Reading file', info.filename
            for line in file_reader.readlines():
                found_pattern = text_file_pattern.search(line)
                doc_id = found_pattern.group(1)
                html = found_pattern.group(2)
                udata=html.decode("utf-8")
                html=udata.encode("ascii","ignore")
                html = html.replace('\\n',' ').replace('\\t',' ').replace("'", "''")
                dataset.create_doc(doc_id, html)
            end = time.time()
            print 'File', info.filename, int(0.5+end-start), 'seconds'
            start = end 

cluster = Cluster()
session = cluster.connect('jkeyspace')
session.row_factory = dict_factory

if __name__ == "__main__":
    main()
