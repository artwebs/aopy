#-*- coding:utf-8 -*-
import MySQLdb


def conn(host,user,passwd,db,port=3306):
    cobj= MySQLdb.connect(
        host,
        port,
        user,
        passwd,
        db,
        )
    return cobj

def close(cobj):
    cobj.close()
