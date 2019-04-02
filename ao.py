#-*- coding:utf-8 -*-
import traceback,time

'''
数据库
'''
class DB(object):
    db=None

def now(format="%Y-%m-%d %H:%M:%S"):
        return time.strftime(format)


def db(type, connstr=None, host=None, user=None, passwd=None, db=None, port=None,auto=True):
    dbs = {
        'mysql': Mysql(connstr, host, user, passwd, db, port, auto),
        'postgresql': Postgresql(connstr, host, user, passwd, db, port, auto)
    }
    return dbs.get(type)

'''
print(table('test', 'dev_').where('id=?',1).find())
print(table(table('test', 'dev_').field('id').where("id<3")).where('id=?', 5).select())
'''
def table(name, perfix=None, db=None,alias=None):
    if db is None:
        db = DB.db
    if isinstance(name,Table):
        perfix=""
        if alias is None:
            alias='t'
        (sql, args) = name.to_sql(alias)
        return Table(sql, perfix, db, args)
    return Table(name, perfix, db)

class Table(object):
    def __init__(self, name=None, perfix="system_", db=None,args=[]):
        self._field = ""
        self._where = None
        self._join = None
        self._order = ""
        self._group = ""
        self._limit = ""
        self._having = ""
        self._name = name
        self._perfix = perfix
        self._db = db
        self._args = args

    def name(self):
        return self._perfix+self._name

    def to_sql(self, alias=None):
        _sql = "select "
        _args = []
        _args.extend(self._args)
        if self._field != "":
            _sql += self._field
        else:
            _sql += "*"
        _sql += " from "+self._perfix+self._name+" "

        if self._join is not None:
            _sql += " " + self._join._format
            _args.extend(self._join._args)
        if self._where is not None:
            _sql += " where "+self._where._format
            _args.extend(self._where._args)
        if self._group != "":
            _sql += " group by "+self._group+" "
        if self._order != "":
            _sql += " order by "+self._order

        if self._limit != "":
            _sql += " limit "+self._limit
        if alias is None:
            return (_sql, _args)
        else:
            return ("("+_sql+") "+alias, _args)

    def select(self):
        return self._db.query(*self.to_sql())

    def find(self):
        self.limit('0,1')
        rs=self.select()
        if len(rs)==1:
            return rs[0]
        else:
            return {}

    def total(self):
        _sql = "select count(*) as c "
        _args = []
        _sql += " from "+self._perfix+self._name+" "

        if self._join is not None:
            _sql += " " + self._join._format
            _args.extend(self._join._args)
        if self._where is not None:
            _sql += " where "+self._where._format
            _args.extend(self._where._args)
        if self._group != "":
            _sql += " group by "+self._group+" "
        rs = self._db.query(_sql, *_args)
        if self._group!="":
            return len(rs)
        else:
            return rs[0]['c']

    '''
    table('test', 'dev_').insert(id=None,name='历史2')
    '''
    def insert(self,**args):
        _sql = "insert into "+self._perfix+self._name+" "
        _field=""
        _value=""
        _vars=[]
        for key in args:
            if _field!="":
                _field+=","
                _value+=","
            _field+=key
            if args[key] is None:
                _value+='null'
            else:
                _value+="?"
                _vars.append(args[key])

        _sql+= "("+_field+") values ("+_value+")"
        return self._db.execute(_sql,*_vars)
            
    '''
    table('test', 'dev_').where('id=?', 4).update(name='历史111')
    '''
    def update(self,**args):
        _sql = "update "+self._perfix+self._name+" set "
        _field = ""
        _vars = []
        for key in args:
            if _field != "":
                _field += ","
            if args[key] is None:
                _field += key+'=null'
            else:
                _field += key+"=?"
                _vars.append(args[key])
        _sql+=_field

        if self._where is not None:
            _sql += " where "+self._where._format
            _vars.extend(self._where._args)
        return self._db.execute(_sql, *_vars)

    def delete(self):
        _sql = "delete from "+self._perfix+self._name+" "
        _vars = []
        if self._where is not None:
            _sql += " where "+self._where._format
            _vars.extend(self._where._args)
        return self._db.execute(_sql, *_vars)

    def field(self, sql):
        if self._field != "":
            self._field += ","
        self._field += sql
        return self

    def where(self, sql,  split='and', *args):
        if self._where is None:
            self._where = DBParamer(sql, *args)
        else:
            self._where.append_split(split, sql, *args)
        return self

    def join(self, sql, *args):
        if self._join is None:
            self._join = DBParamer(sql, *args)
        else:
            self._join.append(sql, *args)
        return self

    def order(self, sql):
        self._order = sql
        return self

    def group(self, sql):
        self._group = sql
        return

    def limit(self, sql):
        self._limit = sql
        return self

    def error(self):
        return self._db._error

class DBBase(object):

    def __init__(self, connstr=None, host=None, user=None, passwd=None, db=None, port=None,auto=True):
        self._conn = None
        self._cursor = None
        self._connstr = connstr
        self._host = host
        self._user = user
        self._passwd = passwd
        self._db = db
        self._port = port
        self._error = None
        self._auto = auto


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

    '''
    table.query('select * from dev_test')
    '''
    def query(self, sql,*args):
        sql=self.format(sql,*args)
        self.conn()
        rs=[]
        try:
            self._cursor.execute(sql,*args)
            rs = self._cursor.fetchall()
        except Exception as e:
            self._error = e
            print(traceback.print_exc())
        finally:
            self.close(self._auto)
        return rs
    '''
    事务
    def tran():
        try:
            print(table('test', 'dev_').where('id=?', 4).update(name='历史111'))
            print(table('test', 'dev_').where('id=?', 4).select())
            # raise Exception("hello")
        except Exception as e:
            DB.db.rollback()
            print(traceback.print_exc())
       
    DB.db.transaction(tran)
    '''
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
                sql = sql.replace("?", "'"+str(arg)+"'", 1)
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
    def __init__(self, connstr=None, host=None, user=None, passwd=None, db=None, port=None, auto=True):
        if port is None:
            port=3306
        DBBase.__init__(self, connstr, host, user, passwd, db, port, auto)

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

class Postgresql(DBBase):
    def __init__(self, connstr=None, host=None, user=None, passwd=None, db=None, port=None, auto=True):
        if port is None:
            port = 5432
        DBBase.__init__(self, connstr, host, user, passwd, db, port, auto)

    def conn(self):
        import psycopg2
        import psycopg2.extras
        if not DBBase.conn(self):
            self._conn = psycopg2.connect(
                host=self._host, port=self._port, user=self._user, password=self._passwd, database=self._db)
        if self._cursor is None:
            self._cursor = self._conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor)

    def format(self, sql, *args):
        DBBase.format(self, sql, *args)
        sql = sql.replace('?', '%s')
        return sql

class DBParamer(object):

    def __init__(self, sql, *args):
        self._args=[]
        self._format = sql
        self._args.extend(args)
        self._index = 1

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
