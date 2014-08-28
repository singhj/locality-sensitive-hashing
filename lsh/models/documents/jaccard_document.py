from lsh.models.documents.document_base import DocumentBase

class JaccardDocument(DocumentBase):
    """
        This class represents documents used in LSH families for Jaccard Distance
        based implementations and their corresponding components in this platform.
    """

    def __init__(self, original_doc, shingles_list=None, minhash_signatures=None):
        self.__shingles = shingles_list
        self.__signatures = minhash_signatures

        super(JaccardDocument, self).__init__(original_doc)

    def get_signatures_list(self):
        return self.__signatures

    def get_shingles_list(self):
        return self.__shingles

    def set_signatures_list(self, value):
        self.__signatures = value

    def set_shingles_lists(self, value):
        self.__shingles = value

    def get_shingles_as_set(self):
        return set(self.__shingles)