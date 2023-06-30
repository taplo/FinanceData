# -*- coding: utf-8 -*-
"""
Created on 20230515
financedata包中的指数类
继承DataSet类，并在类中实现了read_index、get_index、update_index、read_data、get_data、update_data等方法。
@author: Administrator
"""
import pandas as pd
import numpy as np
from datasets import DataSets
from commontools import try_run, try_analyse, split_list

NullDataFrame = pd.DataFrame()


class Indexes(DataSets):

    def __init__(self, data_store):
        super(Indexes, self).__init__(data_store)
        self._data = self.read_index()
        self._index = self._data.index.tolist()

    @try_run
    def read_index(self):
        # implementation of read_index method
        result = self._data_store.hget('list', 'index')
        if isinstance(result, pd.DataFrame):
            target = result
        elif isinstance(result, list):
            target = NullDataFrame
        else:
            print(result)
            target = NullDataFrame
        return target

    @try_run
    def get_index(self):
        # implementation of get_index method
        '''
        市场代码	说明
        MSCI	MSCI指数
        CSI	中证指数
        SSE	上交所指数
        SZSE	深交所指数
        CICC	中金指数
        SW	申万指数
        OTH	其他指数
        '''
        indexes = []
        # 目前不是所有的市场都有数据，暂时先选择有数据的市场，今后再进行考虑
        #market_list = ['MSCI', 'CSI', 'SSE', 'SZSE', 'CICC', 'SW', 'CNI', 'OTH']
        # market_list = ['SSE', 'CSI', 'SZSE', 'CNI']
        market_list = ['CSI', 'CNI', 'SZSE', 'SSE', 'MSCI']
        for mark in market_list:
            result = self._data_source.api.index_basic(market=mark,
                                                       fields=["ts_code", "name", "market", "publisher", "category", "base_date",
                                                               "base_point", "list_date", "fullname", "index_type", "weight_rule",
                                                               "desc", "exp_date"]).set_index('ts_code', drop=True)
            if len(result) > 0 and isinstance(result, pd.DataFrame):
                indexes.append(result)
        if len(indexes) > 1:
            target = pd.concat(indexes, axis=0)
        else:
            target = pd.DataFrame()
        self._data = target
        self._index = target.index.tolist()
        return target
        # return self._query_data(key, args).set_index('ts_code', drop=True)

    def index_info(self):
        print('''
            名称	类型	描述
            ts_code	str	TS代码
            name	str	简称
            fullname	str	指数全称
            market	str	市场
            publisher	str	发布方
            index_type	str	指数风格
            category	str	指数类别
            base_date	str	基期
            base_point	float	基点
            list_date	str	发布日期
            weight_rule	str	加权方式
            desc	str	描述
            exp_date	str	终止日期
            ''')

    @try_run
    def update_index(self):
        # implementation of update_index method
        remote_data = self.get_index()
        local_data = self.read_index()
        result = 0
        if isinstance(remote_data, pd.DataFrame) and isinstance(local_data, pd.DataFrame):
            if not remote_data.fillna('').equals(local_data.fillna('')):
                result = self._data_store.hset('list', 'index', remote_data)
        elif isinstance(remote_data, pd.DataFrame):
            result = self._data_store.hset('list', 'index', remote_data)
        return result

    @try_run
    def read_index_keys(self):
        '''
        获取本地指数列表
        '''
        result = self._data_store.hkeys('index')
        result = list(
            map(lambda x: x.decode() if isinstance(x, bytes) else x, result))
        return result

    @try_run
    def read_data(self, code):
        '''
        获取指数数据
        '''
        # implementation of read_data method
        res = self._data_store.hget('index', code)
        if isinstance(res, pd.DataFrame):
            result = res
        elif isinstance(res, list):
            result = NullDataFrame
        else:
            print('数据读取错误！')
            result = NullDataFrame

        return result

    @try_run
    def get_data(self, code):
        '''
        获得指数数据。
        '''
        # implementation of get_data method
        data = self._get_data(code)

        return data

    @try_run
    def _get_all_data(self, code):
        '''
        获得全部的完整除权数据
        '''
        # implementation of get_data method
        result = []
        for period in self._time_table:
            fields = ["ts_code", "trade_date", "open", "high", "low", "close", "pre_close",
                      "change", "pct_chg", "vol", "amount"]
            res = self._data_source.api.daily(
                ts_code=code, fields=fields, **period)
            if len(res) > 0:
                result.append(res)

        if len(result) > 1:
            res = pd.concat(result, axis=0).drop_duplicates()
        else:
            res = result[0]

        if len(res) > 0:
            res.trade_date = res.trade_date.map(pd.Timestamp)
            res = res.set_index('trade_date', drop=True).sort_index()
            res = res.drop('ts_code', axis=1)

        return res
    '''
    # ---------------------------------------------------------
    # 测试使用
    @try_run
    def get_new_cq_data(self, code):
        return self._get_new_cq_data(code)
    # ---------------------------------------------------------
    '''

    @try_run
    def _get_new_data(self, code):
        '''
        获得指定股票的最新除权数据
        可能缺少历史数据
        '''
        fields = ["ts_code", "trade_date", "open", "high", "low", "close", "pre_close",
                  "change", "pct_chg", "vol", "amount"]
        res = self._data_source.api.index_daily(ts_code=code, fields=fields)
        if isinstance(res, pd.DataFrame) and len(res) > 0:
            res.trade_date = res.trade_date.map(pd.Timestamp)
            res = res.set_index('trade_date', drop=True).sort_index()
            res = res.drop('ts_code', axis=1)

        return res

    @try_run
    def _get_data(self, code):
        '''
        替代原来的方法，减少网络获取的次数，从而提高效率。
        '''
        local_data = self._data_store.hget('index', code)
        if len(local_data) == 0:
            result = self._get_all_data(code)
        else:
            new_data = self._get_new_data(code)
            if len(new_data) > 0:
                if local_data.index.max() != new_data.index.max():
                    result = pd.concat([local_data, new_data]
                                       ).drop_duplicates().sort_index()
                else:
                    result = local_data
            else:
                result = new_data

        return result

        '''这段代码的方法很神，保留一下
        tmp = new_cq.merge(local_cq, on=new_cq.columns.tolist() + ['trade_date',], how='left', indicator=True)
        tmp = tmp[tmp['_merge'] == 'left_only'].drop(columns='_merge')
        '''

    @try_run
    def update_data(self, code):
        # implementation of update_data method
        result = {}
        end = 0
        local_data = self.read_data(code)
        remote_data = self.get_data(code)
        if isinstance(local_data, pd.DataFrame) and isinstance(remote_data, pd.DataFrame):
            if remote_data.fillna('').equals(local_data.fillna('')):
                end = 0
            else:
                end = self._data_store.hset('index', code, remote_data)
        else:
            end = 0

        return end
