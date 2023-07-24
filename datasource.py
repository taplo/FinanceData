# -*- coding: utf-8 -*-
"""
Created on 20230426
financedata包中的数据源类，目前基于tushare编写
@author: Administrator
"""
import tushare
import configparser

from ratelimit import limits, sleep_and_retry

'''
if tushare.get_token() is None:
    source.tushare.set_token(token)
'''


class DataSource:
    '''
    创建网络数据源类，基于tushare包，生成网络数据接口类
    1、考虑使用单例模式，从而确保每个应用程序只有一个连接
    2、最常用的方法返回ts_api对象
    '''

    __instance = None
    tushare = tushare
    __ts_api = None
    __token = None

    def __new__(cls):
        if cls.__instance is None:
            cls.__instance = super().__new__(cls)

            conf = configparser.ConfigParser()
            conf.read('../config.ini', encoding="utf-8-sig")
            cls.__token = conf.get('anapro', 'tushare')

            cls.__instance.__ts_api = tushare.pro_api(cls.__token)
        return cls.__instance
    
    @property
    @sleep_and_retry
    @limits(calls=500, period=60) # 每分钟访问不超过500次
    def api(self):
        '''
        操作句柄
        更改为新的访问频率控制方法，经测试可以使用这种方法。
        '''
        return self.__ts_api
