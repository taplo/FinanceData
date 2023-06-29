# -*- coding: utf-8 -*-
"""
finance data 模块中的引导加载程序
"""

from datasource import DataSource
from datastore import DataStore
from trade_calendar import TradeCalendar

from stocks import Stocks
from indexes import Indexes
from topindexes import TopIndexes
from funds import Funds

source = DataSource()

def sqlite_args():
    
    if platform=='win32':
        path = 'd:/workdir/default.db'
    elif platform=='linux' or platform=='linux2':
        path = '/workdir/default.db'
    else:
        path = ':memory:'

    store = DataStore('sqlite3', db_path=path)
    return {'data_store':store}

def redis_args():

    redis_dict = {
        'host':'MyRedis',
        'port':'6379',
        }
    store = DataStore('redis', **redis_dict)
    return { 'data_store':store}


