from lsh.lsh.lsh_base import LshBase
from lsh.utils.similarity import jaccard_similarity
from lsh.utils.similarity import compute_positive_hash
from lsh.models.documents.jaccard_document import JaccardDocument

def _get_document_from_band(hash_code, band_dict):
    if hash_code and band_dict:
        return band_dict.get(hash_code, None)

    return None

class LshJaccard(LshBase):

    def __init__(self, num_bands=20, rows_per_band=10, threshold=0.0):

        # number of bands (i.e. buckets) to create
        self.num_bands = num_bands

        # this will be the number of signatures in a vector to be hashed
        self.num_rows_per_band = rows_per_band

        #should be thread safe if one instance of this class is used by threads in a pool for example
        #we may need to make a centralized resource to implement this...for now this is good enough.
        self.bands = self._create_band_dicts()

        #note, the default settings give you a threshold ~.74
        if threshold == 0.0:
            self.threshold = self._calculate_threshold()
        else:
            self.threshold = threshold

    def run(self, document):

        #step 1: get document hash signature list
        signatures = list(document.get_signatures_list())

        #get current band
        for band_idx in xrange(0, self.num_bands):

            #step 2: get minhash signature vectors for current band (xrange "start" in function is 0)
            for vector in self._get_vector(signatures, self.num_rows_per_band):

                #step 3: for each vector hash it and add to the proper band

                # note, wrapping in tuple as it will maintain order and is immutable
                # immutability enables us to calculate a hash for it
                vector_tuple = tuple(vector)

                vector_hash = self._calculate_hash(vector_tuple)

                #step 4: get current band
                current_band_dict = self._get_band_by_index(band_idx)

                #step 5: get docs from the current band, for current hash
                docs = _get_document_from_band(vector_hash, current_band_dict)

                #if we don't have docs, add the current doc to a new list and update band
                if not docs:
                    current_band_dict[vector_hash] = [document]
                else:
                    #step 6: check similarity of document pair

                    #for now we only store one document per band, so we get the first document in the list
                    #as it's the only one that should be stored
                    candidate_document = current_band_dict[vector_hash][0]
                    score = self._calculate_similarity_score(document, candidate_document)

                    if self._documents_are_similar(score):
                        results_dict = {
                                "score": score,
                                "match_found": True,
                                "document_1": document.get_original_document(),
                                "document_2": candidate_document.get_original_document()
                            }
                        return results_dict
                    else:
                        results_dict = {
                                "score": score,
                                "match_found": False,
                                "document_1": document.get_original_document(),
                                "document_2": candidate_document.get_original_document()
                            }
                        if score > 0.0:
                            return results_dict

    #--- helper functions ----

    #TODO refactor so that users can specify their own hashing function(s)
    def _calculate_hash(self, obj):
        """
            This method computes hash of object using a dynamic hashing function.
            :param obj: object we are computing hash code for.
            :return: hash code (long integer)
        """

        # hash value to positive integer
        h1 = compute_positive_hash(obj)

        #calculate hash code: h(obj) mod 2^b (where b is num_bands)
        return h1 % 2**self.num_bands


    def _documents_are_similar(self, similarity_score):
        """
            Checks if similarity score against threshold.
            :param similarity_score:
            :return: True if similarity score is greater than or equal to threshold, otherwise False
        """
        return similarity_score >= self.threshold

    def _get_band_by_index(self, idx):
        """
         Gets band by idx from bands collection.
            :param idx:
            :return: None if bands None or idx is greater than number of length of bands collection.
        """
        if self.bands and idx < len(self.bands):
            return self.bands[idx]

        return None

    def _get_vector(self, signatures, n):
        """
            Slices signatures into lists of n rows.
            :param signatures: list of minhash signatures
            :param n: number of rows to include in slice
            :return: list (vector)
        """
        for i in xrange(0, len(signatures), n):
            #yes, technically this is a list slice not a vector
            yield signatures[i:i+n]

    def _calculate_similarity_score(self, document_1, document_2):
        """
            Calculate similarity score for givens documents.
            :param document_1:
            :param document_2:
            :return: 0.0 if score can't be calculated otherwise returns calculated value
        """
        score = 0.0

        if document_1 and document_2:
            shingles_set_1 = document_1.get_shingles_as_set()
            shingles_set_2 = document_2.get_shingles_as_set()

            score = jaccard_similarity(shingles_set_1, shingles_set_2)

        return score

    def _calculate_threshold(self):
        """
            Calculate threshold.
            :return: threshold returned, if threshold can't be calculated returns 0.0
        """

        #(1/b)^(1/r)
        # b = # of bands
        # r = # rows in a band

        _b = float(self.num_bands)
        _r = float(self.num_rows_per_band)

        if _b > 0 and _r > 0:
            return (1.0/_b)**(1.0/_r)

        return 0.0

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


# # for quick testing only...remote and add unit tests instead
# if __name__ == '__main__':
#
#     from lsh.utils.similarity import jaccard_similarity
#     from lsh.shingles.shingles import shingle_generator, ShingleType
#     from lsh.minhash import minhash
#
#     test_docs = list()
#     test_docs.append("Mary had a little lamb, His fleece was white as snow, And everywhere that Mary went, The lamb was sure to go.")
#     test_docs.append("Mary had a little lamb, His fleece was wte as snow, And everywhere thot Mary went, The lamb was sure to go.")
#     test_docs.append("Maryhad a little lomb, is fleece was whit as snow, And evewhere thot Mary went, The lomb was sure to go.")
#     test_docs.append("Mary had a little lamb, His fleece wa")
#     test_docs.append("Mary")
#     test_docs.append("Mary had a little lamb, His fleece was white as snow, And everywhere that Mary went, The lamb was sure to go.")
#
#     def faux_doc_generator():
#         for doc in test_docs:
#             yield doc
#
#     # Examples: Lower the number of bands the higher the threshold becomes
#     # num_bands divides evenly into the number
#     #num_bands=100, rows_per_band=2 => 0.1
#     #num_bands=50, rows_per_band=4 => 0.37
#     #num_bands=10, rows_per_band=20 => 0.89
#     #num_bands=20, rows_per_band=10 => 0.74
#     #num_bands=5, rows_per_band=40 => 0.96
#
#     lshj = LshJaccard(num_bands=20, rows_per_band=10)
#     print "-- number of bands: %s" % str(lshj.num_bands)
#     print "-- number of rows per band: %s" % str(lshj.num_rows_per_band)
#     print "-- default threshold: %s" % str(lshj._calculate_threshold())
#     print ""
#
#     for shingles_list, original_document in shingle_generator(faux_doc_generator()):
#         # get minhash signatures for each shingle list
#         min_hash_signatures = minhash.run(shingles_list)
#
#         #create document and run LSH for Jaccard Distance
#         doc_obj = JaccardDocument(original_document, shingles_list, min_hash_signatures)
#
#         results = lshj.run(doc_obj)
#
#         if results:
#             print results
