#-*- coding:utf-8 -*-

class DB(object):
    _db=None

def db(type, connstr=None, host=None, user=None, passwd=None, db=None, port=None):
    dbs = {
        'mysql': Mysql(connstr, host, user, passwd, db, port)
    }
    return dbs.get('mysql')


def table(name, perfix=None, todb=None):
    if todb is None:
        todb = DB._db
    return Table(name,perfix,todb)


class Table(object):
    __db = None
    __name = ""
    __perfix = "system_"
    __field = ""
    __where = None
    __join = None
    __order = ""
    __group = ""
    __limit = ""
    __having = ""

    def __init__(self, name, perfix=None, todb=None):
        self.__name = name
        self.__perfix = perfix
        self.__db = todb

    def select(self):
        __sql = "select "
        __args = []
        if self.__field != "":
            __sql += self.__field
        else:
            __sql += "*"
        __sql += " from "+self.__perfix+self.__name+" "

        if self.__join is not None:
            __sql+= " " + self.__join._format
            __args.extend(self.__join._args)
        if self.__where is not None:
            __sql += " where "+self.__where._format
            __args.extend(self.__where._args)
        if self.__group != "":
            __sql += " group by "+self.__group+" "
        if self.__order != "":
            __sql += " order by "+self.__order

        if self.__limit != "":
            __sql += " limit "+self.__limit
        return self.__db.query(__sql, *__args)

    def find(self):
        self.limit('0,1')
        rs=self.select()
        if len(rs)==1:
            return rs[0]
        else:
            return {}

    def total(self):
        __sql = "select count(*) as c "
        __args = []
        __sql += " from "+self.__perfix+self.__name+" "

        if self.__join is not None:
            __sql += " " + self.__join._format
            __args.extend(self.__join._args)
        if self.__where is not None:
            __sql += " where "+self.__where._format
            __args.extend(self.__where._args)
        if self.__group != "":
            __sql += " group by "+self.__group+" "
        rs = self.__db.query(__sql, *__args)
        if self.__group!="":
            return len(rs)
        else:
            return rs[0]['c']

    def insert(self,**args):
        __sql = "insert into "+self.__perfix+self.__name+" "
        __field=""
        __value=""
        __vars=[]
        for key in args:
            if __field!="":
                __field+=","
                __value+=","
            __field+=key
            if args[key] is None:
                __value+='null'
            else:
                __value+="?"
                __vars.append(args[key])

        __sql+= "("+__field+") values ("+__value+")"
        return self.__db.execute(__sql,*__vars)
            

    def update(self,**args):
        __sql = "update "+self.__perfix+self.__name+" set "
        __field = ""
        __vars = []
        for key in args:
            if __field != "":
                __field += ","
            if args[key] is None:
                __field += key+'=null'
            else:
                __field += key+"=?"
                __vars.append(args[key])
        __sql+=__field

        if self.__where is not None:
            __sql += " where "+self.__where._format
            __vars.extend(self.__where._args)
        return self.__db.execute(__sql, *__vars)

    def delete(self):
        pass

    def field(self, sql):
        if self.__field != "":
            self.__field += ","
        self.__field += sql
        return self

    def where(self, sql, *args, split='and'):
        if self.__where is None:
            self.__where = DBParamer(sql, *args)
        else:
            self.__where.append_split(split, sql, *args)
        return self

    def join(self, sql, *args):
        if self.__join is None:
            self.__join = DBParamer(sql, *args)
        else:
            self.__join.append(sql, *args)
        return self

    def order(self, sql):
        self.__order = sql
        return self

    def group(self, sql):
        self.__group = sql
        return

    def limit(self, sql):
        self.__limit = sql
        return self

    def error(self):
        return self.__db._error

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
    _error=None

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

    def execute(self, sql,*args):
        self.format(sql, *args)
        self.conn()
        rscount=0
        try:
            self._cursor.execute(sql, args)
            rscount = self._cursor.rowcount
            self._conn.commit()
        except Exception as e:
            self._conn.rollback()
            self._error=e
        finally:
            self.close()
        return rscount

    def query(self, sql,*args):
        self.format(sql,*args)
        self.conn()
        self._cursor.execute(sql,args)
        rs = self._cursor.fetchall()
        self.close()
        return rs

    def format(self,sql,*args):
        for arg in args:
            if isinstance(arg,int):
                sql = sql.replace("%s", str(arg),1)
            else:
                sql = sql.replace("%s", "'"+arg+"'",1)
        print(sql)

    def close(self):
        self._cursor.close()
        self._conn.close()



class Mysql(DBBase):

    def __init__(self, connstr=None, host=None, user=None, passwd=None, db=None, port=3306):
        DBBase.__init__(self, connstr, host, user, passwd, db, port)

    def conn(self):
        import pymysql
        self._conn = pymysql.connect(
            host=self._host, port=self._port, user=self._user, passwd=self._passwd, db=self._db, charset='utf8')
        self._cursor = self._conn.cursor(cursor=pymysql.cursors.DictCursor)

    def format(self, sql, *args):
        sql.replace('?','%s')
        for arg in args:
            if isinstance(arg, int):
                sql = sql.replace("%s", str(arg), 1)
            elif arg is None:
                sql = sql.replace("%s", 'null', 1)
            else:
                sql = sql.replace("%s", "'"+arg+"'", 1)
        print(sql)

    def execute(self, sql, *args):
        sql = sql.replace('?', '%s')
        return DBBase.execute(self, sql, *args)

    def query(self, sql, *args):
        sql = sql.replace('?', '%s')
        return DBBase.query(self, sql, *args)



class DBParamer(object):
    _index = 0
    _format = ""
    _args = []

    def __init__(self, sql, *args):
        self._format = sql
        self._args.append(*args)
        self._index += 1

    def append(self, sql, *args):
        self._format += " "+sql+" "
        self._args.append(*args)
        self._index += 1

    def append_split(self, split, sql, *args):
        if split in ['and', 'or']:
            if self._index == 1:
                self._format = "("+self._format+")"
            self._format = self._format+" "+split+" (" + sql + ") "
        else:
            self._format += " "+split+" "+sql
        self._args.append(*args)
        self._index += 1
