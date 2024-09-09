# -*- coding: utf-8 -*-
"""
finance data 模块中的引导加载程序
"""
from sys import platform
from datasource import DataSource
from datastore import DataStore
from trade_calendar import TradeCalendar
import configparser

from stocks import Stocks
from indexes import Indexes
from topindexes import TopIndexes
from funds import Funds

source = DataSource()


def sqlite_args(path=''):

    if path == '':
        try:
            conf = configparser.ConfigParser()
            conf.read('../config.ini', encoding="utf-8-sig")
            path = conf.get('sqlite', 'path')
        except:
            if platform == 'win32':
                path = 'd:/workdir/default.db'
            elif platform == 'linux' or platform == 'linux2':
                path = '/workdir/default.db'
            else:
                path = ':memory:'

    return {'db_type': 'sqlite3', 'db_path': path}


def redis_args():
    redis_dict = {}

    conf = configparser.ConfigParser()
    conf.read('../config.ini', encoding="utf-8-sig")
    redis_dict['host'] = conf.get('redis', 'host')
    redis_dict['port'] = conf.get('redis', 'port')

    try:
        redis_dict['password'] = conf.get('redis', 'pass')
    except:
        pass

    try:
        redis_dict['db'] = conf.get('redis', 'db')
    except:
        pass

    return {'db_type': 'redis', **redis_dict}


class FinanceData:
    '''
    创建一个管理调度类，
    '''
    _data_stroe = None

    _data_source = source

    # 金融对象字典
    _finance_dict = {
        'stocks': Stocks,
        'indexes': Indexes,
        'topindexes': TopIndexes,
        'funds': Funds,
    }

    def __init__(self, db_type, **kwargs):
        '''
        :param db_type: 数据库类型，可选'redis'或'sqlite3'
        :param kwargs: 数据库连接参数，redis需要host和port以及密码（如果有的话），sqlite3需要db_path
        '''
        self._data_stroe = DataStore(db_type=db_type, **kwargs)

        # self.stocks = self._finance_dict["stocks"](self._data_stroe)
        for key in self._finance_dict.keys():
            cmd = "self." + key + " = " + \
                "self._finance_dict['" + key + "'](self._data_stroe)"
            exec(cmd)
