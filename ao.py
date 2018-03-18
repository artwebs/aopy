#-*- coding:utf-8 -*-
_db = None


def db_init(type, connstr=None, host=None, user=None, passwd=None, db=None, port=None):
    dbs = {
        'mysql': Mysql(connstr, host, user, passwd, db, port)
    }
    return dbs.get('mysql')


def db(name, perfix=None, _db=None):
    pass


class DBBase(object):
    '''
    classdocs
    '''
    _conn = None
    _cursor = None
    _connstr = None
    _host = None
    _port = None
    _user = None
    _passwd = None
    _db = None

    def __init__(self, connstr=None, host=None, user=None, passwd=None, db=None, port=None):
        self._connstr = connstr
        self._host = host
        self._user = user
        self._passwd = passwd
        self._db = db
        self._port = port

    def conn(self):
        print(self._connstr)
        pass

    def execute(self, sql):
        print(sql)
        rs = False
        self.conn()
        self._cursor.execute(sql)
        rscount = self._cursor.rowcount
        self._conn.commit()
        self.close()
        if rscount > 0:
            rs = True
        return rs

    def query(self, sql):
        print(sql)
        self.conn()
        self._cursor.execute(sql)
        rs = self._cursor.fetchall()
        self.close()
        return rs

    def select(self):
        pass

    def total(self):
        pass

    def insert(self):
        pass

    def update(self):
        pass

    def delete(self):
        pass

    def close(self):
        self._cursor.close()
        self._conn.close()


class Mysql(DBBase):
    import pymysql

    def __init__(self, connstr=None, host=None, user=None, passwd=None, db=None, port=None):
        DBBase.__init__(self, connstr, host, user, passwd, db, port)
