
class LshBase(object):

    def run(self, docuemnt):
        raise NotImplementedError("Subclasses should implement run()")

    def calculate_hash(self, value):
        raise NotImplementedError("Subclasses should implement run()")