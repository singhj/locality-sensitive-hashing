
class LshReduceBase(object):

    @classmethod
    def reduce(cls, key, values):
        raise NotImplementedError("Subclasses should implement reduce()")
