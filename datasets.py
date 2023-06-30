# -*- coding: utf-8 -*-
"""
Created on 20230427
financedata包中的各个交易种类封装的基类
此类包含各个交易种类都包含的data_source、data_store、index、data属性，
定义了read_index、get_index、update_index、read_data、get_data、update_data等必须实现的接口。
@author: Administrator
"""
import abc
from pandas import Timestamp
from datasource import DataSource
from trade_calendar import TradeCalendar
from commontools import loads, dumps, try_run


class DataSets:
    '''
    financedata包中的各个交易种类封装的基类
    此类包含各个交易种类都包含的data_source、data_store、index、data属性，
    定义了read_index、get_index、save_index、update_index、read_data、get_data、save_data、update_data等必须实现的接口。
    '''

    _index = None
    _data = None
    _calendar = None

    _data_source = None
    _data_store = None

    # 因为tushare系统对于股票数据下载有长度限制，目前限制一次6000条，因此将交易日历按照6000进行时间分段，
    # 便于从网站分段下载完整数据
    _time_table = [
        {'start_date': '19901219', 'end_date': '20070523'},
        {'start_date': '20070524'}
    ]

    def __init__(self, data_store):
        '''
        data_source:有效的DataSource类的对象
        data_store:有效的DataStore类的对象
        calender:有效的TradeCalendar类的对象
        '''
        self._data_source = DataSource()
        self._data_store = data_store
        self._calendar = TradeCalendar(data_store)

    @property
    def index(self):
        return self._index

    @property
    def data(self):
        return self._data

    @abc.abstractmethod
    def read_index(self):
        # implementation
        pass

    @abc.abstractmethod
    def get_index(self):
        # implementation
        pass

    @abc.abstractmethod
    def update_index(self):
        # implementation
        pass

    @abc.abstractmethod
    def read_data(self):
        # implementation
        pass

    @abc.abstractmethod
    def get_data(self):
        # implementation
        pass

    @abc.abstractmethod
    def update_data(self):
        # implementation
        pass

    """
    @try_run
    def _save_hash(self, args):
        '''
        通过redis接口以hash类型保存数据
        批量保存：name, data两个参数，data类型为dict
        单条保存：name, key, data三个参数，data类型为非dict
        '''
        r = self.__data_store.get_cursor()
        if len(args) == 2 and isinstance(args, dict):
            data = {}
            for key in args['data'].keys():
                data[key] = dumps(args['data'][key])
            return r.hmset(args['name'], data)
        elif len(args) == 3:
            return r.hset(args['name'], args['key'],
                            dumps(args['data']))
        else:
            print('Args Error!', 'Args type or struct error!')
            return 'Args Error!', 'Args type or struct error!'

    @try_run
    def _read_hash(self, name, key):
        '''
        通过redis接口读取以hash类型保存的数据
        只能单条读取，暂时不支持批量读取。
        '''
        r = self.__data_store.redis
        return loads(r.hget(name, key))
    
    @try_run
    def _read_hash_keys(self, name):
        '''
        通过redis接口，获得name下的所有keys值列表
        '''
        r = self.__data_store.get_cursor()
        return r.hkeys(name)
    """

    @try_run
    def _check_code(self, code):
        '''
        如果提交的code为非标准ts_code格式，则补全
        '''
        market_table = {
            '0': 'SZ',
            '3': 'SZ',
            '4': 'BJ',
            '6': 'SH',
            '8': 'BJ',
        }
        char = code[0]
        if char in market_table.keys():
            code = code + '.' + market_table[char]
        else:
            raise code + "代码错误！"

        return code

    # 通用方法
    def _transe_date2tu(self, d):
        '''输入Timestamp，输出TuShare需要的时间格式'''
        #assert isinstance(d, pd.Timestamp), u'日期格式错误'
        s = str(d)[:10]
        return s.replace('-', '')

    def _transe_date2pd(self, s):
        '''输入TuShare日期格式， 输出pandas的Timestamp时间格式'''
        return Timestamp(s)
    """
    # ================================== 不想采用的方法 ==================================

    @try_run
    def _query_data(self, key, args):
        '''
        通用获取数据接口
        key ：接口名称
        args为字典，其中为各个参数的指定
        '''
        assert isinstance(args, dict)
        return self._data_source.api.query(key, **args)

    @try_run
    def _bar_data(self, args):
        '''
        通用行情数据获取接口
        args:（字典结构）
            ts_code：代码（必有）
            start_date：开始日期
            end_date：结束日期
            asset：资产类别，默认为E
                E：股票
                I：指数
                C：数字货币
                FT：期货
                FD：基金
                O：期权
            adj：复权类型，默认为None
                None：未复权
                qfq：前复权
                hfq：后复权
            freq：数据频度，默认D
                1MIN：1分钟
                5MIN：5分钟
                15MIN：15分钟
                30MIN：30分钟
                60MIN：分钟
                D：日线
            factors：股票因子
                只对E（股票有效），支持tor换手率，vr量比
        '''
        assert isinstance(args, dict), '参数错误'
        data = self._data_source.tushare.pro_bar(api=self._data_source.api, **args)
        data.index = data.trade_date.map(self._transe_date2pd)
        data = data.drop(['ts_code', 'trade_date'], axis=1)
        return data.sort_index()

    """
