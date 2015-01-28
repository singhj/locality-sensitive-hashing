import sys, struct, os, types, re, pdb
import logging, settings
logging.basicConfig(filename=settings.LOG_FILENAME, level=logging.DEBUG)

max_bits = 32
max_mask = 2**max_bits - 1

from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement, dict_factory
from cassandra import ConsistencyLevel, InvalidRequest

keyspace = settings.DATABASES['default']['KEYSPACE']
cluster = Cluster()
session = cluster.connect(keyspace)
session.row_factory = dict_factory

class UnableToCreateTable(Exception):
    pass

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
    A singleton metaclass to ensure that the table exists in Cassandra
    Inspired by http://stackoverflow.com/questions/6760685/creating-a-singleton-in-python
    """
    _instances = {}
    def __call__(cls, *args, **kwds):
        if cls not in cls._instances:
            cls._instances[cls] = super(Table, cls).__call__(*args, **{})
            try:
                _rows = session.execute('SELECT COUNT(*) FROM {name}'.format(name = kwds['name']))
                logging.debug('Table %s exists', kwds['name'])
            except InvalidRequest as err:
                remsg = re.compile(r'code=(\d*).*')
                found = remsg.search(err.message)
                code = int('0'+found.group(1))
                if code == 2200:
                    qstring = 'create table {name} ( {attrs}, primary key ({p_keys}))'\
                        .format(name = kwds['name'], 
                                attrs  = ', '.join(kwds['attrs']),
                                p_keys = ', '.join(kwds['p_keys']) )
                    try:
                        session.execute(qstring)
                        if 'indexes' in kwds:
                            for index in kwds['indexes']:
                                # shoulld be (index_name, index_attr)
                                query = 'create index if not exists {index_name} on {keyspace}.{cls_name} ({index_attr})'\
                                    .format(index_name = index[0], index_attr = index[1], keyspace = keyspace, cls_name = cls.__name__)
                                session.execute(query)
                    except:
                        raise UnableToCreateTable(kwds['name'])
                    logging.debug('Table %s was created', kwds['name'])
                else:
                    raise UnknownException()
            qry = "SELECT * FROM {name} WHERE {cond}"\
                .format(name = cls.__name__, cond = ' AND '.join([pk+'=?' for pk in kwds['p_keys']]))

            select = session.prepare(qry)
            select.consistency_level = ConsistencyLevel.QUORUM

            setattr(cls._instances[cls], 'select', select)
            setattr(cls._instances[cls], 'attrs', kwds['attrs'])
            setattr(cls._instances[cls], 'p_keys', kwds['p_keys'])

        return cls._instances[cls]

    def select_row(cls, *args, **kwds):
        pks = tuple([kwds[k] for k in cls.p_keys])
        try:
            #pdb.set_trace()
            retval = session.execute(cls._instances[cls].select, pks)
            if len(retval) == 0:
                return None
            if len(retval) == 1:
                this = cls()
                for k in retval[0].keys():
                    setattr(this, k, retval[0][k])
                #pdb.set_trace()
                return this
        except InvalidRequest as err:
            return None
        raise Exception('%d rows returned' % len(retval))
    def insert_row(cls, *args, **kwds):
        def to_db(datum, typ):
            mapper = {
                'text': lambda x: "'%s'" % x,
                'list<bigint>': lambda x: str(x).replace('L', ''),
                'list<int>': lambda x: str(x).replace('L', ''),
                'int': lambda x: str(x),
                'ascii': lambda x: "'%s'" % x,
            }
            return mapper[typ](datum)

        data = kwds['data']
        data_keys = [attr.split()[0] for attr in cls.attrs if attr.split()[0] in data.keys()]
        data_typs = [attr.split()[1] for attr in cls.attrs if attr.split()[0] in data.keys()]
        data_vals = ', '.join([to_db(data[data_keys[k]], data_typs[k]) for k in xrange(len(data_keys))])

        qstring = 'INSERT INTO %s (%s) VALUES (%s)' % (cls.__name__, ', '.join(data_keys), data_vals)
        #pdb.set_trace()
        query = SimpleStatement(qstring, consistency_level=ConsistencyLevel.QUORUM)
        session.execute(query)
