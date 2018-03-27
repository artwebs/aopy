#-*- coding:utf-8 -*-
from ao import *
DB._db = db('mysql', host='dev-server', port=3306,
                user='root', passwd='windows123', db='dev_test')
def test_db():
    
    # rs=table('test','dev_',_db)
    # .where('id=?','1')\
    # .where('itime>?','2018-01-01')\
    # .select()

    # print(table('test', 'dev_', _db).where('id=?',1).find())
    # print(table.query('select * from dev_test'))

    # print(table('test', 'dev_', _db).total())
    # print(table('test', 'dev_').select())
    # print(table('test', 'dev_').insert(id=None,name='历史2'))
    tb = table('test', 'dev_').where('id1=?', 4)
    print(tb.update(name='历史'),tb.error())
    
    
if __name__ == '__main__':
    test_db()
