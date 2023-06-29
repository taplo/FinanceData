# -*- coding: utf-8 -*-
"""
Created on 20230426
financedata包中的数据存储类，可以灵活选择redis还是sqlite3
@author: Administrator
"""

import redis as redisDB
import sqlite2kv as sql
from commontools import loads, dumps, try_run



class DataStore():
    '''
    创建数据持久化的存储类，根据输入选择是建立redis连接池还是基于sqlite2kv监理的sqlite3连接池。
    可以通过公有方法返回操作cursor
    '''
    __pool = None
    __StrictRedis = None
    __r = None
    
    
    def __init__(self, db_type, **kwargs):
        '''
        :param db_type: 数据库类型，可选'redis'或'sqlite3'
        :param kwargs: 数据库连接参数，redis需要host和port以及密码（如果有的话），sqlite3需要db_path
        '''
        params = {}
        if db_type == 'redis':
            if 'host' in kwargs.keys():
                params['host'] = kwargs['host']
            else:
                params['host'] = '127.0.0.1'
                
            if 'port' in kwargs.keys():
                params['port'] = kwargs['port']

            if 'password' in kwargs.keys():
                params['password'] = kwargs['password']
                
            if 'db' in kwargs.keys():
                params['db'] = kwargs['db']
                            
            self.__pool = redisDB.ConnectionPool(**params)
            self.__StrictRedis = redisDB.StrictRedis
            self.__r = self.redis
        elif db_type == 'sqlite3':
            self.__pool = sql.ConnectionPool(kwargs['db_path'])
            self.__StrictRedis = sql.StrictRedis
            self.__r = self.redis
        else:
            raise ValueError('Invalid db_type')

    @property
    def connection_pool(self):
        '''
        返回连接池，不推荐使用，推荐直接获得游标
        '''
        return self.__pool

    @property
    def redis(self):
        '''
        返回操作句柄
        '''
        return self.__StrictRedis(connection_pool=self.__pool)

    @try_run
    def get(self, name):
        res = self.__r.get(name)
        if isinstance(res, bytes):
            result = loads(res)
        else:
            result = res
            
        return result        
    
    @try_run
    def set(self, name, data):
        return self.__r.set(name, dumps(data))
    
    @try_run
    def hget(self, name, key):
        res = self.__r.hget(name, key)
        if isinstance(res, bytes):
            result = loads(res)
        else:
            result = res
            
        return result
    
    @try_run
    def hset(self, name, key, data):
        return self.__r.hset(name, key, dumps(data))
    
    @try_run
    def hkeys(self, name):
        return self.__r.hkeys(name)