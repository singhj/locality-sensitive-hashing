from lsh.lsh.base import LshBase
from lsh.utils.similarity import jaccard_similarity
from lsh.models.document import Document

def calculate_num_rows_per_band(self, num_total_rows, num_bands):

    num_rows = 0

    if num_bands > 0:
        num_rows = num_total_rows / num_bands

    return num_rows

def calc_hash(value):
    return hash(value)

class LshJaccard(LshBase):

    def __init__(self, bands=20, rows_per_band=10, num_minhash_signatures=200, threshold=0.0):

        # number of bands (i.e. buckets) to create
        self.num_bands = bands

        # total number of minhash signatures we expected from each document
        self.num_total_rows = num_minhash_signatures

        # this will be the number of signatures in a vector to be hashed
        self.num_rows_per_band = rows_per_band

        #should be thread safe if one instance of this class is used by threads in a pool.
        self.buckets = self._create_band_dicts()

        #note, the default settings give you a threshold ~.74
        if threshold == 0.0:
            self.threshold = self._calculate_threshold()
        else:
            self.threshold = threshold

    def run(self, document):

        #step 1: get document hash signature list
        #TODO Check if this changes order when converting from set to list
        signatures = list(document.get_signatures())
        bucket_idx = 0

        #step 2: get vectors
        for vector in self._get_vector(signatures, self.num_rows_per_band):

            #step 3: for each vector hash it and add to the proper band
            #TODO Check if this changes order when converting from list to set
            #TODO explore potentially more consistent hashing algorithms
            hash = calc_hash(frozenset(vector))

            #step 4: get current bucket (dict)
            curr_bucket_dict = self.buckets[bucket_idx]

            #step 5: get docs from the current bucket (if any exist)
            docs = curr_bucket_dict.get(hash, None)

            if docs == None:
                #if we don't have docs, add the current doc to a new list and update bucket
                curr_bucket_dict[hash] = [document]
            else:
                #step 6: check for similar document pair
                if len(docs) == 1:
                    print "checking potential match..."
                    document_2 = curr_bucket_dict[hash][0]
                    score = jaccard_similarity(document.get_signatures(), document_2.get_signatures())

                    if self.check_similar(score):
                        print score
                        print "match found!"
                        print document.get_original_document()
                        print document.get_signatures()
                        print "-------"
                        print document_2.get_original_document()
                        print document_2.get_signatures()

            bucket_idx += 1

    def _get_vector(self, signatures, n):

        for i in xrange(0, len(signatures), n):
            yield signatures[i:i+n] #yes, technically this is a list slice not a vector

    def calculate_similarity_score(self, document_1, document_2):

        score = 0

        if document_1 and document_2:
            shingles_set_1 = document_1.get_shingles()
            shingles_set_2 = document_2.get_shingles()

            score = jaccard_similarity(shingles_set_1, shingles_set_2)

        return score

    def check_similar(self, score):
        return score >= self.threshold

    def _calculate_threshold(self):

        #(1/b)^(1/r)
        # b = # of bands
        # r = # rows in a band

        _b = float(self.num_bands)
        _r = float(self.num_rows_per_band)

        if _b > 0 and _r > 0:
            return (1.0/_b)**(1.0/_r)

        return 0

    def _create_band_dicts(self):
        """
            Create a list of bands (dicts). Each dict represents one band (bucket).
            :return: list of dicts that represents bands.
        """
        buckets = None

        if self.num_bands > 0:

            buckets = []

            #create x bands (each index represents one band)
            for _ in range(0, self.num_bands):
                buckets.append({})

        return buckets

# for quick testing only...remote and add unit tests instead
# if __name__ == '__main__':
#
#     from lsh.utils.similarity import jaccard_similarity
#     from lsh.shingles.shingles import shingle_generator, ShingleType
#     from lsh.minhash import minhash
#
#     test_docs = list()
#     test_docs.append("abcdefghi jcklmnop qrs! it is a fun..")
#     test_docs.append("abcdefghi jcklmnop qrs! it is a fun!! a")
#
#     def faux_doc_generator():
#         for doc in test_docs:
#             yield doc
#
#     test = LshJaccard(bands=20, rows_per_band=10, num_minhash_signatures=200)
#     print "-- number of bands: %s" % str(test.num_bands)
#     print "-- number of rows per band: %s" % str(test.num_rows_per_band)
#     print "-- default threshold: %s" % str(test._calculate_threshold())
#
#     for shingle_set, doc in shingle_generator(faux_doc_generator()):
#         sorted_shingle_set = sorted(shingle_set)
#         min_hash_signatures = minhash.run(sorted_shingle_set)
#
#         doc = Document(doc, sorted_shingle_set, min_hash_signatures)
#         test.run(doc)
