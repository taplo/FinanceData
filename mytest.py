# -*- coding: utf-8 -*-
"""
测试中使用的对象创建方法
"""
from importlib import reload
from commontools import *
from sys import platform
import datasource
import datastore
import trade_calendar

import stocks
import indexes
import funds


def test_prepare():

    if platform == 'win32':
        path = 'd:/workdir/default.db'
    elif platform == 'linux' or platform == 'linux2':
        path = '/workdir/default.db'
    else:
        path = ':memory:'

    store = datastore.DataStore('sqlite3', db_path=path)
    return {'data_store': store}


def analyst_prepare():
    redis_dict = {
        'host': 'MyRedis',
        'port': '6379',
    }
    store = datastore.DataStore('redis', **redis_dict)
    return {'data_store': store}


def test_reload():
    reload(datasource)
    reload(datastore)
    reload(trade_calendar)
    return test_prepare()


def analyst_reload():
    reload(datasource)
    reload(datastore)
    reload(trade_calendar)
    return analyst_prepare()


def test_stocks():
    reload(stocks)
    return stocks.Stocks(**test_reload())


def analyst_stocks():
    reload(stocks)
    return stocks.Stocks(**analyst_reload())


def test_indexes():
    reload(indexes)
    return indexes.Indexes(**test_reload())


def analyst_indexes():
    reload(indexes)
    return indexes.Indexes(**analyst_reload())


def test_funds():
    reload(funds)
    return funds.Funds(**test_reload())


def analyst_funds():
    reload(funds)
    return funds.Funds(**analyst_reload())
