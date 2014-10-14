from google.appengine.ext import ndb
import random, sys, math

def get_max_bits():
        return int(math.log(sys.maxsize+2, 2))

def calculate_max_hashes(rows, bands):
    return rows * bands

def get_random_bits(max_hashes):
    return [random.getrandbits(get_max_bits()) for _ in xrange(max_hashes)]

class Dataset(ndb.Model):
    dataset_key = ndb.KeyProperty()
    random_seeds = ndb.IntegerProperty(repeated = True)
    buckets = ndb.IntegerProperty(repeated = True)
    
    # The following parameters can be tuned via the Datastore Admin Interface
    rows = ndb.IntegerProperty()
    bands = ndb.IntegerProperty()
    buckets_per_band = ndb.IntegerProperty()
    shingle_type = ndb.StringProperty(choices=('w', 'c4'))
    minhash_modulo = ndb.IntegerProperty()
    
    @classmethod
    def create(cls, dataset_key, **kwargs):

        rows = kwargs.get('rows', 5)
        bands = kwargs.get('bands', 40)
        buckets_per_band = kwargs.get('buckets_per_band', 100)
        shingle_type = kwargs.get('shingle_type', 'c4')
        minhash_modulo = kwargs.get('minhash_modulo', 5000)
        dataset_key = dataset_key

        max_hashes = calculate_max_hashes(rows, bands)
        dataset = cls.get(dataset_key)

        if not dataset:
            dataset = Dataset(
                              dataset_key = dataset_key,
                              random_seeds = get_random_bits(max_hashes),
                              rows = rows,
                              bands = bands,
                              buckets_per_band = buckets_per_band,
                              shingle_type = shingle_type,
                              minhash_modulo = minhash_modulo,
                              )

        return dataset.put()

    @classmethod
    def get(cls, key):
        return Dataset.query(cls.dataset_key == key).get()

