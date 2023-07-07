# -*- coding: utf-8 -*-
"""
Created on 20230427
financedata包中的股票类
继承DataSet类，并在类中实现了read_index、get_index、update_index、read_data、get_data、update_data等方法。
其中还包括股票的技术指标（qualification）
@author: Administrator
"""
import pandas as pd
import numpy as np
from datasets import DataSets
from commontools import try_run, try_analyse, split_list

NullDataFrame = pd.DataFrame()


class Stocks(DataSets):

    def __init__(self, data_store):
        super(Stocks, self).__init__(data_store)
        self._data = self.read_index()
        self._index = self._data.index.tolist()

    @try_run
    def _cq2qfq(self, data, adj_factor):
        '''
        将除权数据，使用复权因子计算出前复权数据
        '''
        assert isinstance(data, pd.DataFrame), 'data 数据格式错误'
        assert isinstance(adj_factor, pd.DataFrame), 'adj_factor 数据格式错误'

        temp = data.copy()
        temp['adj'] = adj_factor.adj_factor
        temp['last_adj'] = adj_factor.adj_factor[-1]
        temp['factor'] = temp.adj / temp.last_adj
        cols = ['open', 'high', 'low', 'close', 'pre_close']
        for col in cols:
            temp[col] = np.round(temp[col] * temp.factor, 4)

        temp['change'] = temp['close'] - temp['pre_close']

        return temp.drop(['adj', 'last_adj', 'factor'], axis=1)

    @try_run
    def read_index(self):
        # implementation of read_index method
        result = self._data_store.hget('list', 'stock')
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
        self._data_source.check_times()
        result = self._data_source.api.stock_basic(exchange='', list_status='L',
                                                   fields='ts_code, symbol, name, area, industry, fullname, enname, market, exchange,\
            curr_type, list_status, list_date, delist_date, is_hs').set_index(
            'ts_code', drop=True)
        self._data = result
        self._index = result.index.tolist()
        return result
        # return self._query_data(key, args).set_index('ts_code', drop=True)

    @try_run
    def update_index(self):
        # implementation of update_index method
        remote_data = self.get_index()
        local_data = self.read_index()
        result = 0
        if isinstance(remote_data, pd.DataFrame) and isinstance(local_data, pd.DataFrame):
            if not remote_data.fillna('').equals(local_data.fillna('')):
                result = self._data_store.hset('list', 'stock', remote_data)
        elif isinstance(remote_data, pd.DataFrame):
            result = self._data_store.hset('list', 'stock', remote_data)
        return result

    @try_run
    def read_data(self, code):
        '''
        获取个股数据， 一次性将除权、前复权、复权系数同步读入
        返回数据字典，格式如类中的_data数据。
        '''
        # implementation of read_data method
        if len(code) != 9:
            code = self._check_code(code)

        data = {}
        kinds = ['qfq', 'cq', 'adj', 'qual']
        for k in kinds:
            res = self._data_store.hget(k, code)
            if isinstance(res, pd.DataFrame):
                data[k] = res
            elif isinstance(res, list):
                data[k] = pd.DataFrame()

        return data

    @try_run
    def get_data(self, code):
        '''
        获得股票除权数据，复权系数，并计算前复权数据。
        '''
        # implementation of get_data method
        if len(code) != 9:
            code = self._check_code(code)

        data = {}
        data['cq'] = self._get_cq_data(code)
        data['adj'] = self._get_adj_data(code)
        data['qual'] = self._get_qual_data(code)
        data['qfq'] = self._cq2qfq(data['cq'], data['adj'])
        return data

    @try_run
    def _get_qual_data(self, code):
        local_qual = self._data_store.hget('qual', code)
        if isinstance(local_qual, pd.DataFrame) and len(local_qual) > 0:
            new_qual = self._get_new_qual_data(code)
            if len(new_qual) > 0:
                if local_qual.index.max() != new_qual.index.max():
                    result = pd.concat([local_qual, new_qual]
                                       ).drop_duplicates().sort_index()
                else:
                    result = local_qual
            else:
                result = new_qual
        else:
            result = self._get_all_qual_data(code)

        return result

    @try_run
    def _get_all_qual_data(self, code):
        fields = ["ts_code", "trade_date", "close", "turnover_rate", "turnover_rate_f",
                  "volume_ratio", "pe", "pe_ttm", "pb", "ps", "ps_ttm", "dv_ratio", "dv_ttm",
                  "total_share", "float_share", "free_share", "total_mv", "circ_mv", "limit_status"]
        result = []
        for period in self._time_table:
            self._data_source.check_times()
            res = self._data_source.api.daily_basic(
                ts_code=code, **period, fields=fields)
            if len(res) > 0:
                res.trade_date = res.trade_date.apply(pd.Timestamp)
                res = res.set_index('trade_date', drop=True).drop(
                    'ts_code', axis=1).sort_index()
                result.append(res)

        if len(result) > 1:
            result = pd.concat(result)
        else:
            result = result[0]
        return result

    @try_run
    def _get_new_qual_data(self, code):
        fields = ["ts_code", "trade_date", "close", "turnover_rate", "turnover_rate_f",
                  "volume_ratio", "pe", "pe_ttm", "pb", "ps", "ps_ttm", "dv_ratio", "dv_ttm",
                  "total_share", "float_share", "free_share", "total_mv", "circ_mv", "limit_status"]
        self._data_source.check_times()
        res = self._data_source.api.daily_basic(ts_code=code, fields=fields)
        if len(res) > 0:
            res.trade_date = res.trade_date.apply(pd.Timestamp)
            result = res.set_index('trade_date', drop=True).drop(
                'ts_code', axis=1).sort_index()
        else:
            result = res
        return result

    @property
    def qual_info(self):
        '''
        显示技术指标的数据字典。
        '''
        print("""
              输出参数

                名称	类型	描述
                ts_code	str	TS股票代码
                trade_date	str	交易日期
                close	float	当日收盘价
                turnover_rate	float	换手率（%）
                turnover_rate_f	float	换手率（自由流通股）
                volume_ratio	float	量比
                pe	float	市盈率（总市值/净利润， 亏损的PE为空）
                pe_ttm	float	市盈率（TTM，亏损的PE为空）
                pb	float	市净率（总市值/净资产）
                ps	float	市销率
                ps_ttm	float	市销率（TTM）
                dv_ratio	float	股息率 （%）
                dv_ttm	float	股息率（TTM）（%）
                total_share	float	总股本 （万股）
                float_share	float	流通股本 （万股）
                free_share	float	自由流通股本 （万）
                total_mv	float	总市值 （万元）
                circ_mv	float	流通市值（万元）
                limit_status	int	涨跌停状态
              """)

    @try_run
    def _get_all_cq_data(self, code):
        '''
        获得全部的完整除权数据
        '''
        # implementation of get_data method
        result = []
        for period in self._time_table:
            self._data_source.check_times()
            res = self._data_source.api.daily(ts_code=code, **period)
            if len(res) > 0:
                if type(res) == pd.DataFrame:
                    result.append(res)
                else:
                    # 可能收到超频次错误
                    print(res)

        if len(result) > 1:
            res = pd.concat(result, axis=0).drop_duplicates()
        elif len(result)==1:
            res = result[0]

        if len(res) > 0:
            res.trade_date = res.trade_date.map(pd.Timestamp)
            res = res.set_index('trade_date', drop=True).sort_index()
            res = res.drop('ts_code', axis=1)

        return res

    # ---------------------------------------------------------
    # 测试使用
    @try_run
    def get_new_cq_data(self, code):
        return self._get_new_cq_data(code)
    # ---------------------------------------------------------

    @try_run
    def _get_new_cq_data(self, code):
        '''
        获得指定股票的最新除权数据
        可能缺少历史数据
        '''
        self._data_source.check_times()
        res = self._data_source.api.daily(ts_code=code)
        if len(res) > 0:
            res.trade_date = res.trade_date.map(pd.Timestamp)
            res = res.set_index('trade_date', drop=True).sort_index()
            res = res.drop('ts_code', axis=1)

        return res

    @try_run
    def _get_cq_data(self, code):
        '''
        替代原来的方法，减少网络获取的次数，从而提高效率。
        '''
        local_cq = self._data_store.hget('cq', code)
        if len(local_cq) == 0:
            result = self._get_all_cq_data(code)
        else:
            new_cq = self._get_new_cq_data(code)
            if len(new_cq) > 0:
                if local_cq.index.max() != new_cq.index.max():
                    result = pd.concat([local_cq, new_cq]
                                       ).drop_duplicates().sort_index()
                else:
                    result = local_cq
            else:
                result = new_cq

        return result

        '''这段代码的方法很神，保留一下
        tmp = new_cq.merge(local_cq, on=new_cq.columns.tolist() + ['trade_date',], how='left', indicator=True)
        tmp = tmp[tmp['_merge'] == 'left_only'].drop(columns='_merge')
        '''

    @try_run
    def _get_all_adj_data(self, code):
        '''
        获得复权系数数据
        '''
        result = []
        for period in self._time_table:
            self._data_source.check_times()
            res = self._data_source.api.adj_factor(ts_code=code, **period)
            if len(res) > 0:
                result.append(res)

        if len(result) > 1:
            target = pd.concat(result, axis=0).drop_duplicates()
        else:
            target = result[0]

        if len(target) > 0:
            target.trade_date = target.trade_date.map(pd.Timestamp)
            target = target.set_index('trade_date', drop=True).sort_index()
            target = target.drop('ts_code', axis=1)

        return target

    @try_run
    def _get_new_adj_data(self, code):
        '''
        获得新的复权系数数据
        '''
        self._data_source.check_times()
        target = self._data_source.api.adj_factor(ts_code=code)
        if len(target) > 0:
            target.trade_date = target.trade_date.map(pd.Timestamp)
            target = target.set_index('trade_date', drop=True).sort_index()
            target = target.drop('ts_code', axis=1)

        return target

    @try_run
    def _get_adj_data(self, code):
        '''
        替代原来的方法，减少网络获取的次数，从而提高效率。
        '''
        local_adj = self._data_store.hget('adj', code)
        if len(local_adj) == 0:  # 本地无数据
            result = self._get_all_adj_data(code)
        else:  # 本地有数据
            new_adj = self._get_new_adj_data(code)
            if len(new_adj) > 0:
                if local_adj.index.max() != new_adj.index.max():
                    result = pd.concat([local_adj, new_adj])
                    result['time'] = result.index
                    result = result.drop_duplicates().sort_index().drop('time', axis=1)
                else:
                    result = local_adj
            else:
                result = new_adj

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
                end = [0, 0, 0, 0]
            else:
                result[key] = self._data_store.hset(
                    key, code, remote_data[key])
                end = list(result.values())

        return end
