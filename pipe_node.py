

class PipeNode(object):
    def open(self):
        pass
    def getNext(self):
        raise NotImplementedError("Subclasses should implement getNext()")
    def close(self):
        pass

class NotFound(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class NotLoggedIn(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)