
class LshReduceBase(object):

    @classmethod
    def reduce(cls, key, values):
        yield (key, values)
