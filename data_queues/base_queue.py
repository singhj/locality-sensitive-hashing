import Queue

class BaseQueue(object):
    """
        Subclass this BaseQueue to implement your own data queues for OpenLSH.
    """
    def enqueue(self, value):
        raise NotImplementedError("Subclasses should implement enqueue()")

    def dequeue(self):
        raise NotImplementedError("Subclasses should implement dequeue()")

    def size(self):
        pass

    def empty(self):
        pass