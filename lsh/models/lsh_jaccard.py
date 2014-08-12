from lsh.models.lsh_base import LshBase
from lsh.utils.similarity import jaccard_similarity

class LshJaccard(LshBase):

    def __init__(self, bands=20, num_signatures=200):

        self.num_bands = bands
        self.num_total_rows = num_signatures
        self.num_rows_per_band = self._calculate_num_rows_per_band()

        #TODO may want to implement a global threadsafe version of these for all threads to use
        #just in case we want to compare documents from multiple threads.
        self.band_dicts = self._create_band_dicts()

        self.threshold = self._calculate_threshold()

    def run(self, document):
        #TODO implement algorithm here, will run on one document at time, assuming the that
        #one instance of this class will calculate LSH against n documents to compare
        pass

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
        pass

    def _create_band_dicts(self):
        """
            Create a list of bands (dicts). Each dict represents one band (bucket).
            :return: list of dicts that represents bands.
        """
        buckets = None

        if self.num_bands > 0:

            buckets = []

            for _ in range(0, self.num_bands):
                buckets.append({})

        return buckets

    def _calculate_num_rows_per_band(self):

        num_rows = 0

        if self._num_bands > 0:
            num_rows = self.num_total_rows / self.num_bands

        return num_rows