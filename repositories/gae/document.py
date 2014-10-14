from google.appengine.ext import ndb

class Document(ndb.Model):
    minhashes = ndb.IntegerProperty(repeated = True)
    buckets = ndb.IntegerProperty(repeated = True)