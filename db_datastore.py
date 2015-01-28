import sys, struct, os, types, re, pdb
import logging, settings
logging.basicConfig(filename=settings.LOG_FILENAME, level=logging.DEBUG)

from google.appengine.ext import ndb

class UnableToCreateTable(Exception):
    pass

class DbInt(object):
    @staticmethod
    def to_db(number):
        signed = struct.unpack('=l', struct.pack('=L', number & settings.max_mask))[0]
        return signed
    @staticmethod
    def fm_db(number):
        return settings.max_mask & number

class Table(ndb.Model):
    """
    A singleton metaclass to ensure that the table exists in the GAE Datastore
    Inspired by http://stackoverflow.com/questions/6760685/creating-a-singleton-in-python
    """
    _instances = {}
    def __call__(cls, *args, **kwds):
        if cls not in cls._instances:
            def datastore_type(typ):
                mapper = {
                    'text': ndb.StringProperty(),
                    'list<bigint>': ndb.IntegerProperty(repeated = True),
                    'list<int>': ndb.IntegerProperty(repeated = True),
                    'int': ndb.IntegerProperty(),
                    'ascii': ndb.StringProperty(indexed = False),
                }
                return mapper[typ]
            attrs = {}
            for attr in kwds['attrs']:
                (name, typ) = tuple(attr.split())
                attrs[name] = datastore_type(typ)

            cls._instances[cls] = super(Table, cls).__call__(*args, **attrs)
            setattr(cls._instances[cls], 'attrs', kwds['attrs'])
            setattr(cls._instances[cls], 'p_keys', kwds['p_keys'])

        return cls._instances[cls]

    def select_row(cls, *args, **kwds):
        query_params = {}
        for k in cls.p_keys:
            query_params[k] = kwds[k]
        retval = cls.query(**query_params).get()
    def insert_row(cls, *args, **kwds):
        def to_db(datum, typ):
            mapper = {
                'text': lambda x: str(x),
                'list<bigint>': lambda x: [int(xi) for xi in x],
                'list<int>': lambda x: [int(xi) for xi in x],
                'int': lambda x: int(x),
                'ascii': lambda x: str(x),
            }
            return mapper[typ](datum)

        data = kwds['data']
        data_keys = [attr.split()[0] for attr in cls.attrs if attr.split()[0] in data.keys()]
        data_typs = [attr.split()[1] for attr in cls.attrs if attr.split()[0] in data.keys()]
        attrs = {}
        for k in xrange(len(data_kays)):
            attrs[data_keys[k]] = to_db(data[data_keys[k]], data_typs[k])
        new_instance = cls(**attrs)
        return new_instance
