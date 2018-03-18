#-*- coding:utf-8 -*-
from ao import *

def test_db():
    _db = db_init('mysql', connstr="connstr")
    _db.conn()

if __name__ == '__main__':
    test_db()
