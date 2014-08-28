

class DocumentBase(object):
    """
        This base class represents documents that are processed by LSH implementations and
        their corresponding components in this platform.

        To implement your own document(s) that are compliant with this platform,
        inherit from this class.
    """
    def __init__(self, original_doc=None):
        self.__original_doc = original_doc

    def get_original_document(self):
        return self.__original_doc

    def set_original_document(self, value):
        self.__original_doc = value


