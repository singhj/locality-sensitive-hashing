# Use the deferred library in the case of Google App Engine

try:
    from google.appengine.ext import deferred
except ImportError:
    class deferred(object):
        @staticmethod
        def defer(*args, **kwargs):
            args1 = args[1:]
            args[0](*args1, **kwargs)
