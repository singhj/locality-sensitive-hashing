import sys, struct, os, types, re, copy, pdb
import logging, settings

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

class Table(type):
    """
    A singleton metaclass to ensure that the table exists in the GAE Datastore
    Inspired by http://stackoverflow.com/questions/6760685/creating-a-singleton-in-python
    
    We implement it by creating a StorageProxy
    """
    _instances = {}
    def __call__(cls, *args, **kwds):
        if kwds:
            #logging.info('Table called cls = %s, kwds = %s', cls, kwds)
            pass
        if cls not in cls._instances:
            cls._instances[cls] = super(Table, cls).__call__(*args, **{})
            datastore_type = {
                'text': ndb.StringProperty(),
                'list<bigint>': ndb.IntegerProperty(repeated = True),
                'list<int>': ndb.IntegerProperty(repeated = True),
                'int': ndb.IntegerProperty(),
                'ascii': ndb.StringProperty(indexed = False),
            }
            attr_list = kwds['attrs']
            attrs = {}
            for attr in attr_list:
                (name, typ) = tuple(attr.split())
                attrs[name] = copy.copy(datastore_type[typ])
            StorageProxy = type(cls.__name__, (ndb.Model,), attrs)
            logging.info('Creating class %s with attributes %s', cls.__name__, attrs)
            setattr(cls._instances[cls], 'StorageProxy', StorageProxy)

            gql = "SELECT * FROM {name} WHERE {cond}"\
                .format(name = cls.__name__, cond = ' AND '.join([kwds['p_keys'][c]+'=:%d'%(c+1) for c in xrange(len(kwds['p_keys']))]))
            select = ndb.gql(gql)

            setattr(cls._instances[cls], 'select', select)
            setattr(cls._instances[cls], 'attrs', kwds['attrs'])
            setattr(cls._instances[cls], 'p_keys', kwds['p_keys'])

        return cls._instances[cls]

    def select_row(cls, *args, **kwds):
        pks = tuple([kwds[k] for k in cls.p_keys])
        qry = cls._instances[cls].select.bind(*pks)
        retval = qry.get()
        if not retval: return None
#         logging.info('select returns %s', retval)
        this = cls()
        for k in cls._instances[cls].attrs:
            attr_name = k.split()[0]
            setattr(this, attr_name, getattr(retval, attr_name))
        return this

    def insert_row(cls, *args, **kwds):
        def to_db(datum, typ):
            mapper = {
                'text': lambda x: str(x),
                'list<bigint>': lambda x: [int(xi) for xi in x],
                'list<int>': lambda x: [int(xi) for xi in x],
                'int': lambda x: int(x),
                'ascii': lambda x: str(x),
            }
            retval = mapper[typ](datum)
            return retval

        data = kwds['data']
        my_class = cls._instances[cls]
        key_name = '|'.join([data[k] for k in cls.p_keys])
        constructor_args = {}
        for k in cls.p_keys:
            constructor_args[k] = data[k]
        new_instance = my_class.StorageProxy.get_or_insert(key_name, **constructor_args)
        for data_key in my_class.attrs:
            (data_key_name, data_key_type) = data_key.split()
            if data_key_name in cls.p_keys: continue
            if data_key_name not in data: continue
            setattr(new_instance, data_key_name, to_db(data[data_key_name], data_key_type))
        logging.debug('Inserting %s with %s', new_instance, vars(new_instance))
        new_instance.put()

        this = cls()
        for k in cls._instances[cls].attrs:
            attr_name = k.split()[0]
            setattr(this, attr_name, getattr(new_instance, attr_name))
        return this
