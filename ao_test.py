#-*- coding:utf-8 -*-
from ao import *
import traceback
# DB.db = db('mysql', host='dev-server',
#                 user='root', passwd='windows123', db='dev_test')

DB.db = db('postgresql', host='dev-server', 
           user='postgres', passwd='windows123', db='dev')
def test_db():
    
    # rs=table('test','dev_',_db)
    # .where('id=?','1')\
    # .where('itime>?','2018-01-01')\
    # .select()

    # print(table('test', 'dev_').where('id=?',1).find())
    # print(table(table('test', 'dev_').where("id<3")).where('id=?', 2).find()['itime'])
    # print(table.query('select * from dev_test'))

    # print(table('test', 'dev_', _db).total())
    print(table('test', 'dev_').select())
    # print(table('test', 'dev_').insert(name='历史3'))
    # tb = table('test', 'dev_').where('id1=?', 4)
    # print(tb.update(name='历史'),tb.error())
    # def tran():
    #     try:
    #         print(table('test', 'dev_').where('id=?', 4).update(name='历史111'))
    #         print(table('test', 'dev_').where('id=?', 4).select())
    #         # raise Exception("hello")
    #     except Exception as e:
    #         DB.db.rollback()
    #         print(traceback.print_exc())
       
    # DB.db.transaction(tran)
    # print(table('test', 'dev_').where('id=?', 4).delete())



    
    
if __name__ == '__main__':
    test_db()
