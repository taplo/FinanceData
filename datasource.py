# -*- coding: utf-8 -*-
"""
Created on 20230426
financedata包中的数据源类，目前基于tushare编写
@author: Administrator
"""
import tushare
import configparser

from rhythm import Rhythm


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
    __rhythm = None

    def __new__(cls):
        if cls.__instance is None:
            cls.__instance = super().__new__(cls)

            conf = configparser.ConfigParser()
            conf.read('../config.ini', encoding="utf-8-sig")
            cls.__token = conf.get('anapro', 'tushare')

            cls.__instance.__ts_api = tushare.pro_api(cls.__token)
            cls.__rhythm = Rhythm(475, 60)
            cls.__rhythm.start()
        return cls.__instance

    @property
    def api(self):
        '''
        操作句柄
        '''
        return self.__ts_api

    def check_times(self):
        '''
        控制请求频度，在网络操作前调用。
        '''
        self.__rhythm.checker()