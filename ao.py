#-*- coding:utf-8 -*-
import traceback
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

    def __init__(self, name=None, perfix=None, todb=None):
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
        __sql = "delete from "+self.__perfix+self.__name+" "
        __vars = []
        if self.__where is not None:
            __sql += " where "+self.__where._format
            __vars.extend(self.__where._args)
        return self.__db.execute(__sql, *__vars)

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
    _auto=True

    def __init__(self, connstr=None, host=None, user=None, passwd=None, db=None, port=None):
        self._connstr = connstr
        self._host = host
        self._user = user
        self._passwd = passwd
        self._db = db
        self._port = port


    def conn(self):
        return self._conn is not None

    def execute(self, sql,*args):
        sql=self.format(sql, *args)
        self.conn()
        rscount=0
        try:
            self._cursor.execute(sql, args)
            rscount = self._cursor.rowcount
            if self._auto:
                self.commit()
        except Exception as e:
            self.rollback()
            self._error=e
            print(traceback.print_exc())
        finally:
            self.close(self._auto)
        return rscount

    def query(self, sql,*args):
        sql=self.format(sql,*args)
        self.conn()
        rs=[]
        try:
            self._cursor.execute(sql,args)
            rs = self._cursor.fetchall()
        except Exception as e:
            self._error = e
            print(traceback.print_exc())
        finally:
            self.close(self._auto)
        return rs
    ##事务
    def transaction(self,fun,**args):
        self.conn()
        self._auto=False
        rs = fun(**args)
        self.commit()
        self._auto = True
        self.close()
        return rs

    def commit(self,is_auto=True):
        if not is_auto:
            return
        self._conn.commit()

    def rollback(self, is_auto=True):
        if not is_auto:
            return
        self._conn.rollback()

    def format(self,sql,*args):
        for arg in args:
            if isinstance(arg,int):
                sql = sql.replace("?", str(arg),1)
            else:
                sql = sql.replace("?", "'"+arg+"'",1)
        print(sql)
        return sql

    def close(self,is_auto=True):
        if self._cursor is not None:
            self._cursor.close()
        self._cursor = None
        if not is_auto:
            return
        if self._conn is not None :
            self._conn.close()
        self._conn=None




class Mysql(DBBase):
    def __init__(self, connstr=None, host=None, user=None, passwd=None, db=None, port=3306):
        DBBase.__init__(self, connstr, host, user, passwd, db, port)

    def conn(self):
        import pymysql
        if not DBBase.conn(self):
            self._conn = pymysql.connect(
                host=self._host, port=self._port, user=self._user, passwd=self._passwd, db=self._db, charset='utf8')
        if self._cursor is None:
            self._cursor = self._conn.cursor(cursor=pymysql.cursors.DictCursor)

    def format(self, sql, *args):
        DBBase.format(self, sql,*args)
        sql=sql.replace('?', '%s')
        return sql

class DBParamer(object):
    _index = 0
    _format = ""
    _args = []

    def __init__(self, sql, *args):
        self._args=[]
        self._format = sql
        self._args.extend(args)
        self._index += 1

    def append(self, sql, *args):
        self._format += " "+sql+" "
        self._args.extend(args)
        self._index += 1

    def append_split(self, split, sql, *args):
        if split in ['and', 'or']:
            if self._index == 1:
                self._format = "("+self._format+")"
            self._format = self._format+" "+split+" (" + sql + ") "
        else:
            self._format += " "+split+" "+sql
        self._args.extend(args)
        self._index += 1
