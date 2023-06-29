# -*- coding: utf-8 -*-
"""
首次编辑 2023-05-23
将sqlite3中的数据导入到redis数据库中，以进行数据分析的时候满足性能要求
@author: Administrator
"""

import sqlite3
import redis


def trans_string(c, r):
    '''
    c:sqlite的cursor对象
    r:redis的StrictRedis对象
    '''
    count = 0
    result = c.execute('SELECT * FROM list;')
    e = result.fetchone()
    while e is not None:
        r.set(name=e[0], value=e[1])
        # print(e[0], "'s data transed.")
        count += 1
        e = result.fetchone()
    print('已转换 %d 条string类型数据。'%count)

def trans_hash(c, r):
    '''
    c:sqlite的cursor对象
    r:redis的StrictRedis对象
    '''
    count = 0
    result = c.execute('SELECT * FROM hash;')
    e = result.fetchone()
    while e is not None:
        r.hset(name=e[0], key=e[1], value=e[2])
        # print(e[0], "'s ", e[1], "'s data transed.")
        count += 1
        e = result.fetchone()
    print('已转换 %d 条hash类型数据。'%count)

def trans():
    print('开始转换...')
    sq = sqlite3.connect('/workdir/default.db')
    c = sq.cursor()
    pool = redis.ConnectionPool(host='MyRedis')
    r = redis.StrictRedis(connection_pool=pool)
    
    trans_string(c, r)
    trans_hash(c, r)
    r.save()
    print('Redis Server 数据已保存。')
    
    c.connection.close()
    r.connection_pool.disconnect()
    print("转换结束。")


if __name__ == '__main__':

    trans()