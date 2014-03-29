

class PipeNode(object):
    def Open(self):
        pass
    def GetNext(self):
        raise NotImplementedError("Subclasses should implement GetNext()")
    def Close(self):
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