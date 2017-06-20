#
# author    : Riki Hidayat
# email     : riki.hidayat.91@gmail.com
#

import logging
import MySQLdb
from pymongo.errors import *
from pymongo import MongoClient
from collections import defaultdict, OrderedDict


def initiate_logger():
    # create logger
    logger = logging.getLogger('QueryExecutor')
    logger.setLevel(logging.DEBUG)

    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # add ch to logger
    logger.addHandler(ch)

    return logger


class _Config(object):

    db_engines = {'MONGO': 'mongo', 'MYSQL': 'mysql'}

    # configuration for connecting to mysql
    mysql = {
        'HOST': 'host',
        'USER': 'user',
        'PASSWORD': 'password',
        'DB': 'db'
    }

    # configuration for connecting to mongo
    # if collections is empty, then it will use all collections
    mongo = {
        'HOST': 'host',
        'PORT': 27017,
        'DB': 'db',
        'COLLECTIONS': ['collection']
    }

    # maximum server delay
    # ms unit
    max_delay = 30000

    # searching mode for mongo
    # exact: only values with exact match with query will be displayed
    # partial: values with partial match with query will be displayed
    exact_mode = 'exact'
    partial_mode = 'partial'
    search_mode = (exact_mode, partial_mode)

    """
    query logical for mongo
    $or: Joins query clauses with a logical OR returns all documents
        that match the conditions of either clause.
    $and: Joins query clauses with a logical AND returns all documents
        that match the conditions of both clauses.
    """
    logical_or = '$or'
    logical_and = '$and'
    logical = (logical_or, logical_and)


    """
    query mode for mysql
    """
    # cari data yang diawalin dengan kata X
    startswith_mode = 'startswith'
    # cari data yang diakhiri dengan kata X
    endswith_mode = 'endswith'
    # cari data yang mengandung dengan kata X
    contain_mode = 'contain'
    # cari data yang sama dengan kata X
    equal_mode = 'equal'


class QueEx:

    def __init__(self):
        self.logger = initiate_logger()

    def connect(self, db_engine=_Config.db_engines['MONGO']):
        self.logger.info('Trying to connect with %s' % db_engine)

        if db_engine == _Config.db_engines['MONGO']:
            try:
                client = MongoClient(_Config.mongo['HOST'], _Config.mongo['PORT'],
                        serverSelectionTimeoutMS=_Config.max_delay)
                db = client[_Config.mongo['DB']]

                if not _Config.mongo['COLLECTIONS']:
                    _Config.mongo['COLLECTIONS'] = [c for c in db.collection_names() if not c.startswith('system')]

            except ServerSelectionTimeoutError as e:
                raise Exception(str(e))

        elif db_engine == _Config.db_engines['MYSQL']:
            try:
                ct = _Config.max_delay / 1000
                db = MySQLdb.connect(
                    host=_Config.mysql['HOST'],
                    user=_Config.mysql['USER'],
                    passwd=_Config.mysql['PASSWORD'],
                    db=_Config.mysql['DB'],
                    connect_timeout=ct
                )

            except Exception as e:
                raise Exception(str(e))

        self.logger.info('  connected')
        return db

    def mongo_query(self, fields, logical):
        """
        @type fields: dict of tuple
        @param fields: key merupkan kolom dalam tabel, dan value-nya merupakan
            tuple yang bersisi:
            0: nilai yang akan dicari di kolom tersebut
            1: mode pencarian, exact|partial
        @type logical: str
        @param logical: $or|$and
        """
        if logical not in _Config.logical:
            raise Exception('Unknown logical "%s"' % logical)

        db = self.connect()

        self.logger.info('Building query...')
        # name = 'Riki'
        # print {'NAMA_LGKP': {'$regex' : '.*' + name.upper() + '.*'}}
        # {"$or":[ {"vals":1700}, {"vals":100} ]})
        # {"$or":[{'NAMA_LGKP':'ERNAWATI'}, {'ALAMAT':'CIJAMBE'}]}
        query = {logical: []}
        for field, (value, mode) in fields.iteritems():
            if not mode in _Config.search_mode:
                raise Exception('Invalid mode for field "%s"' % field)

            if mode == _Config.partial_mode:
                query_mode = {'$regex' : '.*' + value + '.*', '$options' : 'i'}

            elif mode == _Config.exact_mode:
                query_mode = value

            query[logical].append({field: query_mode})

        self.logger.info('Querying...')
        result = defaultdict(list)
        for coll_name in _Config.mongo['COLLECTIONS']:
            self.logger.info(query)
            coll = db[coll_name]
            rows = coll.find(query)
            rowcount = coll.count(query)
            self.logger.debug('  from "%s" with total rows: %s' % (coll_name, rowcount))
            result[coll_name] = [row for row in rows]

        return result

    def mysql_query(self, select, tablename, *params):
        """
        @type select: str | list
        @param select:
            string * > akan mengambil semua kolom
            string nama_kolom > hanya akan mengambil 1 kolom saja sesuai dengan nama_kolom
            list > sebutkan nama_kolom apa saja yang akan diambil
        @type tablename: str
        @param tablename: sebutkan nama tabel yang akan di-query
        @type params: dict of dict
        @param params: kondisi pencarian data
            dict terdiri dari key:
                - logical (opsional untuk param ke-1): or|and
                - conditions (list of tuple): index ke-1 harus diisi dengan tuple (logical, or|and)
                    index dan ke-2 dst diisi dengan (nama_kolom, (nilai yang akan dicari, mode))
                    mode >> baca query mode untuk mysql di _Config
        """
        cursor = self.connect(db_engine=_Config.db_engines['MYSQL']).cursor()

        # initiate query
        q = ['SELECT']

        # target select statement
        if isinstance(select, str):
            if select == '*':
                q.append('*')
            else:
                q.append('`%s`' % select)
        elif isinstance(select, list):
            temp_select = []
            for sname in select:
                temp_select.append('`%s`' % sname)
            q.append(','.join(temp_select))

        # from statement
        q.append('FROM `%s`' % tablename)

        # where statement
        q.append('WHERE')
        for i, param in enumerate(params):
            logical = param.get('logical', 'or').upper()
            conditions = OrderedDict(param['conditions'])
            temp_q = []

            ins_logical = conditions.pop('logical').upper()
            for ic, (field, (value, mode)) in enumerate(conditions.iteritems()):
                if mode == _Config.startswith_mode:
                    param = '`{}` LIKE \'{}%\''.format(field, value)
                elif mode == _Config.endswith_mode:
                    param = '`{}` LIKE \'%{}\''.format(field, value)
                elif mode == _Config.contain_mode:
                    param = '`{}` LIKE \'%{}%\''.format(field, value)
                elif mode == _Config.equal_mode:
                    param = '`{}` = \'{}\''.format(field, value)
                else:
                    raise Exception('Unknown mode "%s"' % mode)

                if ic > 0:
                    temp_q.append('{} {}'.format(ins_logical, param))
                else:
                    temp_q.append(param)

            if i > 0:
                q.append('{} ({})'.format(logical, ' '.join(temp_q)))
            else:
                q.append('({})'.format(' '.join(temp_q)))

        q = ' '.join(q)
        self.logger.info('QUERY: %s' % q)

        cursor.execute(q)
        return cursor.fetchall()


if __name__ == '__main__':
    from pprint import pprint
    qe = QueEx()
    # result = qe.mongo_query({'name': ('kadek', 'partial')}, '$or')
    # for coll, rows in result.iteritems():
    #     pprint(rows)

    result = qe.mysql_query(['nama', 'jenis_kelamin'], 'contact_person',
        {'conditions': [('logical', 'or'), ('nama', ('riki', 'contain'))]},
        {'logical': 'and', 'conditions': [('logical', 'and'), ('nama', ('hidayat', 'endswith')), ('alamat', ('bandung', 'equal'))]},
    )
    pprint(result)
