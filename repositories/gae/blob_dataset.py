from google.appengine.ext import ndb
from repositories.gae.dataset import Dataset
from repositories.gae.dataset import calculate_max_hashes, get_random_bits

class BlobDataset(Dataset):

    filename = ndb.StringProperty()
    blob_key = ndb.BlobKeyProperty()

    @classmethod
    def create(cls, blob_key, **kwargs):

        blob_key = blob_key
        filename = kwargs.get('filename')
        rows = kwargs.get('rows', 5)
        bands = kwargs.get('bands', 40)
        buckets_per_band = kwargs.get('buckets_per_band', 100)
        shingle_type = kwargs.get('shingle_type', 'c4')
        minhash_modulo = kwargs.get('minhash_modulo', 5000)

        max_hashes = calculate_max_hashes(rows, bands)
        dataset = cls.get(blob_key)

        if not dataset:
            dataset = BlobDataset(
                              filename = filename,
                              blob_key = blob_key,
                              random_seeds = get_random_bits(max_hashes),
                              rows = rows,
                              bands = bands,
                              buckets_per_band = buckets_per_band,
                              shingle_type = shingle_type,
                              minhash_modulo = minhash_modulo)
        else:
            dataset.filename = filename
            
        return dataset.put()

    @classmethod
    def get(cls, key):
        return Dataset.query(cls.blob_key == key).get()