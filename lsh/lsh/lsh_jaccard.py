from lsh.lsh.lsh_base import LshBase
from lsh.utils.similarity import jaccard_similarity
from lsh.utils.similarity import compute_positive_hash
from lsh.models.documents.jaccard_document import JaccardDocument
import copy

def _get_documents(hash_code, band_dict):
    if hash_code and band_dict:
        return band_dict.get(hash_code, None)
    return None

def _update_bucket(hash_code, band_dict, new_document):
    docs = band_dict.get(hash_code, None)
    if docs:
        updated_docs = copy.deepcopy(docs)
        updated_docs.append(new_document)
        band_dict[hash_code] = updated_docs
        # print "updated!!"
        # for doc in updated_docs:
        #     print doc.get_original_document()

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

    def run(self, document_to_examine):

        #step 1: get document hash signature list
        signatures = list(document_to_examine.get_signatures_list())
        start = 0

        #get current band
        for band_idx in xrange(0, self.num_bands):

            #step 2: get minhash signature vector for current band
            vector = self._get_vector(signatures, self.num_rows_per_band, start)

            #step 3: hash current vector and add to it a new bucket or compare and record the document (and vector)
            # that hashed to an existing bucket
            if vector:
                # note, wrapping in tuple as it will maintain order and is immutable
                # immutability enables us to calculate a hash for it
                vector_tuple = tuple(vector)

                if vector_tuple:
                    vector_hash = self._calculate_hash(vector_tuple)

                    #step 4: get current band
                    current_band_dict = self._get_band_by_index(band_idx)

                    #step 5: get docs from the current band's bucket that has the current vector_hash as it's key
                    docs = _get_documents(vector_hash, current_band_dict)

                    #if we don't have docs, add the current doc to a new list and update band dict
                    if not docs:
                        current_band_dict[vector_hash] = [document_to_examine]
                    else:
                        #step 6: check similarity of document pair

                        #6a: update the current band's bucket that this current document to be examined hashed to
                        _update_bucket(vector_hash, current_band_dict, document_to_examine)

                        #6b: iterate through the current band's bucket of documents, calculate similarity
                        # scores for each pair and yield results
                        for doc in docs:
                            score = self._calculate_similarity_score(document_to_examine, doc)

                            if self._documents_are_similar(score):
                                match_found = True
                            else:
                                match_found = False

                            if score > 0.0:
                                doc1 = document_to_examine.get_original_document()
                                doc2 = doc.get_original_document()
                                yield self._build_results_dict(score, match_found, doc1, doc2, band_idx)

            # update starting value to get next vector
            start = start + self.num_rows_per_band + 1


    #--- helper functions ----

    def _build_results_dict(self, score, match_found, doc_1, doc_2, band_matched):
        """
            Build LSH results dictionary.
            :param score:
            :param match_found:
            :param doc_1:
            :param doc_2:
            :return: results dict that contains candidate documents, threshold and LSH similarity score.
        """
        results_dict = {"score": score,
                        "match_found": match_found,
                        "document_1": doc_1,
                        "document_2": doc_2,
                        "threshold": self.threshold,
                        "band_matched": band_matched}
        return results_dict

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

    def _get_vector(self, signatures, n, start):
        """
            Slices signatures into lists of n rows.
            :param signatures: list of minhash signatures
            :param n: number of rows to include in slice
            :return: list (vector)
        """
        for i in xrange(start, len(signatures), n):
            #yes, technically this is a list slice not a vector
            return signatures[i:i+n]

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


# for quick testing only...remote and add unit tests instead
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
#         for results in lshj.run(doc_obj):
#             if results:
#                 print results

