# -*- coding: utf-8 -*-
"""
Created on 20230524
financedata包中的大盘指数类
继承Indexes类，是其中的一个子集，并在类中实现了read_index、get_index、update_index、read_data、get_data、update_data等方法。
@author: Administrator
"""
import pandas as pd
import numpy as np
from indexes import Indexes
from commontools import try_run, try_analyse, split_list

NullDataFrame = pd.DataFrame()


class TopIndexes(Indexes):

    _index = ['000001.SH', '000005.SH', '000006.SH', '000016.SH', '000300.SH', '000905.SH',
              '399001.SZ', '399005.SZ', '399006.SZ', '399016.SZ', '399300.SZ']

    def __init__(self, data_store):
        super(TopIndexes, self).__init__(data_store)
        self._data = self.read_index()

    @try_run
    def read_index(self):
        # implementation of read_index method
        # implementation of read_index method
        result = super().read_index()
        if isinstance(result, pd.DataFrame):
            target = result.loc[self._index]
        else:
            target = result
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
        # 此类中的index数量有限
        market_list = ['SZSE', 'SSE']
        for mark in market_list:
            result = self._data_source.api.index_basic(market=mark,
                                                       fields=["ts_code", "name", "market", "publisher", "category", "base_date",
                                                               "base_point", "list_date", "fullname", "index_type", "weight_rule",
                                                               "desc", "exp_date"]).set_index('ts_code', drop=True)
            if len(result) > 0 and isinstance(result, pd.DataFrame):
                indexes.append(result)
        if len(indexes) > 1:
            target = pd.concat(indexes, axis=0).loc[self._index]
        else:
            target = NullDataFrame
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
    def read_index_keys(self):
        '''
        获取本地指数列表
        '''
        keys = super().read_index_keys()
        top_keys = []
        for code in self._index:
            if code in keys:
                top_keys.append(code)

        return top_keys
