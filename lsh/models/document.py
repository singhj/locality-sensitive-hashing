


class Document(object):

    def __init__(self, original_doc, shingles_set, minhash_signatures):
        self.__original_doc = original_doc
        self.__shingles = shingles_set
        self.__signatures = minhash_signatures

    def get_signatures(self):
        return self.__signatures

    def get_shingles(self):
        return self.__shingles

    def get_original_document(self):
        return self.__original_doc