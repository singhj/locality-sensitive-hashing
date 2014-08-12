
class LshBase(object):

    def run(self, document):
        raise NotImplementedError("Subclasses should implement run()")

    def calculate_similarity_score(self, doccument_1, document_2):
        raise NotImplementedError("Subclasses should implement calculate_similarity_score()")

    def check_similar(self, score):
        raise NotImplementedError("Subclasses should implement check_similar()")