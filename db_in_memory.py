import sys, struct, os, types
sys.path.insert(0, 'libs')
import logging, settings
logging.basicConfig(filename=settings.LOG_FILENAME, level=logging.DEBUG)

max_bits = 32
max_mask = 2**max_bits - 1

class DbInt(object):
    @staticmethod
    def to_db(number):
        signed = struct.unpack('=l', struct.pack('=L', number & max_mask))[0]
        return signed
    @staticmethod
    def fm_db(number):
        return max_mask & number
class Table(type):
    """
    A singleton metaclass to ensure that the table exists in the database
    Inspired by http://stackoverflow.com/questions/6760685/creating-a-singleton-in-python
    """
    _instances = {}
    def __call__(cls, *args, **kwds):
        def insert_row(self, *args, **kwds):
            pks = tuple([kwds['data'][k] for k in self.p_keys])
            self._rows[pks] = kwds['data']
            return self._rows[pks]
        def select_row(self, *args, **kwds):
            pks = tuple([kwds[k] for k in self.p_keys])
            try:
                return self._rows[pks]
            except KeyError:
                return None
        if cls not in cls._instances:
            logging.debug('Table %s was created', kwds['name'])
            cls._instances[cls] = super(Table, cls).__call__(*args, **{})
            setattr(cls._instances[cls], 'attrs', kwds['attrs'])
            setattr(cls._instances[cls], 'p_keys', kwds['p_keys'])
            setattr(cls._instances[cls], '_rows', {})
            cls._instances[cls].insert_row = types.MethodType( insert_row, cls._instances[cls] )
            cls._instances[cls].select_row = types.MethodType( select_row, cls._instances[cls] )
        return cls._instances[cls]
    def select_row(cls, *args, **kwds):
        pks = tuple([kwds[k] for k in cls.p_keys])
        try:
            retval = cls._instances[cls]._rows[pks]
            this = cls()
            for k in retval.keys():
                setattr(this, k, retval[k])
            return this
        except KeyError:
            return None
    def insert_row(cls, *args, **kwds):
        pks = tuple([kwds['data'][k] for k in cls.p_keys])
        cls._instances[cls]._rows[pks] = kwds['data']
        return cls._instances[cls]._rows[pks]
