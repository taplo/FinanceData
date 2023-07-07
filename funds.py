# -*- coding: utf-8 -*-
"""
Created on 20230525
financedata包中的公募基金类
继承DataSets类，是其中的一个子集，并在类中实现了read_index、get_index、update_index、read_data、get_data、update_data等方法。
@author: Administrator
"""
import pandas as pd
import numpy as np
from datasets import DataSets
from commontools import try_run, try_analyse, split_list

NullDataFrame = pd.DataFrame()


class Funds(DataSets):

    def __init__(self, data_store):
        super(Funds, self).__init__(data_store)
        self._data = self.read_index()
        if isinstance(self._data, pd.DataFrame):
            self._index = self._data.index.tolist()
        else:
            self._index = []

    @try_run
    def read_index(self):
        # implementation of read_index method
        result = self._data_store.hget('fund', 'index')
        if isinstance(result, pd.DataFrame):
            target = result.loc[self._index]
        else:
            target = result
        return target

    @try_run
    def get_index(self):
        # implementation of get_index method
        # 条件：1、场内基金；2、所有状态
        res = self._data_source.api.fund_basic(market="E", status='L')
        result = res.set_index('ts_code', drop=True).sort_values('list_date')
        if len(result) > 0 and isinstance(result, pd.DataFrame):
            self._data = result
            self._index = self._data.index.tolist()
        else:
            self._data = NullDataFrame
            self._index = []
        return result

    def fund_info(self):

        info = """
            输出参数

            名称	类型	默认显示	描述
            ts_code	str	Y	基金代码
            name	str	Y	简称
            management	str	Y	管理人
            custodian	str	Y	托管人
            fund_type	str	Y	投资类型
            found_date	str	Y	成立日期
            due_date	str	Y	到期日期
            list_date	str	Y	上市时间
            issue_date	str	Y	发行日期
            delist_date	str	Y	退市日期
            issue_amount	float	Y	发行份额(亿)
            m_fee	float	Y	管理费
            c_fee	float	Y	托管费
            duration_year	float	Y	存续期
            p_value	float	Y	面值
            min_amount	float	Y	起点金额(万元)
            exp_return	float	Y	预期收益率
            benchmark	str	Y	业绩比较基准
            status	str	Y	存续状态D摘牌 I发行 L已上市
            invest_type	str	Y	投资风格
            type	str	Y	基金类型
            trustee	str	Y	受托人
            purc_startdate	str	Y	日常申购起始日
            redm_startdate	str	Y	日常赎回起始日
            market	str	Y	E场内O场外
            """
        print(info)

    @try_run
    def update_index(self):
        # implementation of update_index method
        remote_data = self.get_index()
        local_data = self.read_index()
        result = 1
        if isinstance(local_data, pd.DataFrame):
            if remote_data.fillna('').equals(local_data.fillna('')):
                result = 0

        if result > 0:
            result = self._data_store.hset('list', 'fund', remote_data)

        return result

    @try_run
    def read_index_keys(self):
        '''
        获取本地指数列表
        '''
        result = self._data_store.hkeys('fund')
        result = list(
            map(lambda x: x.decode() if isinstance(x, bytes) else x, result))
        return result

    @try_run
    def _read_data(self, code):
        data = self._data_store.hget('fund', 'code')
        if isinstance(data, pd.DataFrame):
            result = data
        else:
            result = NullDataFrame
        return result

    @try_run
    def _read_adj(self, code):
        data = self._data_store.hget('fundadj', 'code')
        if isinstance(data, pd.DataFrame):
            result = data
        else:
            result = NullDataFrame
        return result

    @try_run
    def read_data(self, code):
        result = {}
        result['data'] = self._read_data(code)
        result['adj'] = self._read_adj(code)
        return result

    @try_run
    def _get_new_data(self, code):
        self._data_source.check_times()
        data = self._data_source.api.fund_daily(ts_code=code)
        if len(data) > 0:
            data.trade_date = data.trade_date.map(pd.Timestamp)
            data = data.set_index('trade_date', drop=True).drop(
                'ts_code', axis=1).sort_index()
        return data

    @try_run
    def _get_all_data(self, code):
        res = []
        for period in self._time_table:
            self._data_source.check_times()
            data = self._data_source.api.fund_daily(ts_code=code, **period)
            if len(data) > 0:
                if type(data) == pd.DataFrame:
                    res.append(data)
                else:
                    # 可能收到超频次错误
                    print(data)
        if len(res) > 1:
            result = pd.concat(res)
            result.trade_date = result.trade_date.map(pd.Timestamp)
            result = result.set_index('trade_date', drop=True).drop(
                'ts_code', axis=1).sort_index()
        elif len(res) == 1:
            result = res[0]
        else:
            result = data
        return result

    @try_run
    def _get_data(self, code):
        local_data = self._read_data(code)
        if isinstance(local_data, pd.DataFrame) and len(local_data) > 0:
            new_data = self._get_new_data(code)
            if len(new_data) > 0:
                if local_data.index.max() != new_data.index.max():
                    result = pd.concat([local_data, new_data]
                                       ).drop_duplicates().sort_index()
                else:
                    result = local_data
            else:
                result = new_data
        else:
            result = self._get_all_data(code)

        return result

    @try_run
    def _get_new_adj(self, code):
        self._data_source.check_times()
        adj = self._data_source.api.fund_adj(ts_code=code)
        if len(adj) > 0:
            adj.trade_date = adj.trade_date.map(pd.Timestamp)
            adj = adj.set_index('trade_date', drop=True).drop(
                'ts_code', axis=1).sort_index()
        return adj

    @try_run
    def _get_all_adj(self, code):
        res = []
        for period in self._time_table:
            self._data_source.check_times()
            data = self._data_source.api.fund_adj(ts_code=code, **period)
            if len(data) > 0:
                data.trade_date = data.trade_date.map(pd.Timestamp)
                data = data.set_index('trade_date', drop=True).drop(
                    'ts_code', axis=1).sort_index()
                res.append(data)
        if len(res) > 1:
            result = pd.concat(res)
        elif len(res) == 1:
            result = res[0]
        else:
            result = data
        return result

    @try_run
    def _get_adj(self, code):
        local_data = self._read_adj(code)
        if isinstance(local_data, pd.DataFrame) and len(local_data) > 0:
            new_data = self._get_new_adj(code)
            if len(new_data) > 0:
                if local_data.index.max() != new_data.index.max():
                    result = pd.concat([local_data, new_data]
                                       ).drop_duplicates().sort_index()
                else:
                    result = local_data
            else:
                result = new_data
        else:
            result = self._get_all_adj(code)

        return result

    @try_run
    def get_data(self, code):
        result = {}
        result['data'] = self._get_data(code)
        result['adj'] = self._get_adj(code)
        return result

    @try_run
    def update_data(self, code):
        # implementation of update_data method
        result = {}
        end = 0
        local_data = self.read_data(code)
        remote_data = self.get_data(code)
        for key in remote_data.keys():
            if remote_data[key].fillna('').equals(local_data[key].fillna('')):
                end = [0, 0]
            else:
                result[key] = self._data_store.hset(
                    key, code, remote_data[key])
                end = list(result.values())

        return end
