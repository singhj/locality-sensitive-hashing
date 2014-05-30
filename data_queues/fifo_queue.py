from data_queues.base_queue import BaseQueue
import threading
import Queue

class FIFOQueue(BaseQueue):
    """
        Implements a thread safe, in memory FIFO queue. Only one instance will be created so it will be used by
        all threads. It uses the python Queue module which handles all locking and synchronization for adding and
        removing items from the queue.

        Based on tornado.ioloop.IOLoop.instance() approach.
        See https://github.com/facebook/tornado

        See gist fork: https://gist.github.com/tbrooks007/11199689
    """

    __lock = threading.Lock()
    __instance = None
    __queue = None

    @classmethod
    def instance(cls):
        if not cls.__instance and not cls.__queue:
            with cls.__lock:
                if not cls.__instance and not cls.__queue:
                    cls.__instance = FIFOQueue()
                    cls.__queue = Queue.Queue()
        return cls.__instance

    def enqueue(self, value):
        if self.__class__.__queue:
            self.__class__.__queue.put(value)
        else:
            raise Exception("Queue instance does not exist.")

    def dequeue(self):
        if self.__class__.__queue:
            return self.__class__.__queue.get()
        else:
            raise Exception("Queue instance does not exist.")

    def size(self):
        if self.__class__.__queue:
            return self.__class__.__queue.qsize()
        else:
            raise Exception("Queue instance does not exist.")

    def empty(self):
        if self.__class__.__queue:
            return self.__class__.__queue.empty()
        else:
            raise Exception("Queue instance does not exist.")

# for quick testing only...remote and add unit tests instead
# if __name__ == '__main__':
#
#     test = FIFOQueue.instance()
#
#     test.enqueue("hello world")
#     value = test.dequeue()
#     print value
#     print test.empty()
#     print test.size()