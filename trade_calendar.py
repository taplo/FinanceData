# -*- coding: utf-8 -*-
"""
Created on 20230426
financedata包中的交易日历类
此类制定数据源和数据存储后，可以读取、获得、更新交易日历
@author: Administrator
"""
import datetime
import pandas as pd
from datasource import DataSource
from commontools import *

class TradeCalendar():
    '''
    financedata包中的交易日历类
    1、此类制定数据源和数据存储后，可以读取、获得、保存、更新交易日历
    2、类创建的时候初始化data_source和data_store对象
    3、对象创建的时候先尝试读取日历，如果日历最新日期比今天早，则重新从网络获取日历并保存数据
    4、如果数据源中没有交易日历，则从网络获取最新交易日历并保存数据。
    '''
    __instance = None
    __data_source = None
    __data_store = None
    __calendar = None

    def __new__(cls, data_store):
        if cls.__instance is None:
            cls.__instance = super().__new__(cls)

        return cls.__instance
    
    def __init__(self, data_store):
        '''
        初始化交易日历对象，读取日历，如果日历最新日期比今天早，则重新从网络获取日历并保存数据
        如果数据源中没有交易日历，则从网络获取最新交易日历并保存数据。
        '''
        self.__data_source = DataSource()
        self.__data_store = data_store
        try:
            self.__calendar = self.read()
        except:
            self.update()

        if self.__calendar.index.max().date() < datetime.date.today():
            self.update()
    
    @property
    def calendar(self):
        return self.__calendar
    
    
    def read(self):
        '''
        从数据源中读取交易日历
        '''
        # implementation omitted
        #cur = self.__data_store.get_cursor()
        #return loads(cur.get('calendar'))
        
        #return loads(self.__data_store.redis.get('calendar'))
        
        return self.__data_store.get('calendar')
    
    def save(self):
        '''
        将交易日历保存到数据存储中
        '''
        # implementation omitted
        #cur = self.__data_store.get_cursor()
        #result = cur.set('calendar', dumps(self.calendar))
        #self.__data_store.save()
        #return result
        
        #return self.__data_store.redis.set('calendar', dumps(self.__calendar))
        
        return self.__data_store.set('calendar', self.__calendar)
    
    def update(self):
        '''
        从网络获取最新交易日历并保存数据
        '''
        # implementation omitted
        cal = self.get()
        return self.save()
    
    @try_run
    def get(self):
        '''
        获取交易日历
        '''
        cals = []
        key = 'trade_cal'
        args = {'start_date': '19901219'}
        today = datetime.date.today().isoformat().replace("-", "")
        while args['start_date'] < today:
            cal_p = self.__data_source.api.trade_cal(exchange='', **args)
            cal_p.index = cal_p.cal_date.apply(pd.Timestamp)
            cals.append(cal_p)
            args['start_date'] = cal_p.cal_date.max()

        if len(cals)==1:
            result = cals[0][['exchange', 'is_open', 'pretrade_date']]
        elif len(cals)>1:
            result = pd.concat(cals, axis=0).drop_duplicates()[['exchange', 'is_open', 'pretrade_date']]
        else:
            print("get calendar error: no data return!")
            result = pd.DataFrame()
        self.__calendar = result
        return result
    
    def is_trading_day(self, date=pd.Timestamp.today().floor('d')):
        '''
        判断某一天是否为交易日， 默认值为当日
        '''
        return self.__calendar.loc[pd.Timestamp(date).floor('d')]['is_open']

    def last_trading_day(self):
        '''
        返回今天之前（含）最后一个交易日
        '''
        return pd.Timestamp(self.__calendar.loc[pd.Timestamp.today().floor('d')]['pretrade_date'])
