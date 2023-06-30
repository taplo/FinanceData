# -*- coding: utf-8 -*-
"""
Created on 20230426

@author: Administrator
"""

from numpy import array

import traceback
import colorama

import pickle
import zlib
import lzma

"""
列表分拆方法
"""


def split_list(lst, lenth):
    '''按照固定长度拆分列表，返回分段索引。'''
    if lenth > len(lst):
        return [lst]
    else:
        start = array(list(range(0, len(lst), lenth)))
        end = start + lenth - 1
        end[-1] = len(lst) - 1
        pos = list(zip(start, end))
        result = []
        for p in pos:
            result.append(lst[p[0]:p[1]+1])

        return result


"""
Created on Sat Jul 9 23:41:46 2022
异常处理工具
使用装饰器来对方法进行try-except意外处理，降低代码数量
@author: Administrator
"""


def try_run(func):

    def real_run(*args, **keyargs):
        try:
            return func(*args, **keyargs)
        except Exception as err:
            print(colorama.Fore.RED, 'Error execute: %s, %s' %
                  (func.__name__, ' '.join(map(str, err.args))), colorama.Fore.RESET)

    return real_run


def try_analyse(func):

    def real_run(*args, **keyargs):
        try:
            return func(*args, **keyargs)
        except Exception as err:
            print(colorama.Fore.RED, 'Error execute: %s, %s' %
                  (func.__name__, ' '.join(map(str, err.args))), colorama.Fore.RESET)

            traceback.print_exc()

    return real_run


"""
Created on 2019-02-03 
为了在redis中更节省空间的保存数据，自行用cloudpickle和zlib封装了一个Pickle类
@author: Administrator
这是2023年5月22日的最新版改版
"""
# 新版压缩解压方法


def dumps(data, level=3):
    return lzma.compress(pickle.dumps(data, pickle.HIGHEST_PROTOCOL), preset=level)


def loads(data):
    # 判定是否为新格式
    check_bytes = b'\xfd7zXZ\x00\x00\x04\xe6\xd6\xb4F\x02\x00!'
    if check_bytes == data[:15]:  # 新格式
        result = pickle.loads(lzma.decompress(data))
    else:
        try:
            result = pickle.loads(zlib.decompress(data))
        except:
            result = pickle.loads(zlib.decompress(data), encoding='iso-8859-1')
    return result


"""
调整字符串长度的方法。
输入字符串和一个整数，将输入字符串长度调整为整数指定的长度，用空格补全。
如果字符串长度超过指定长度则截断，并在末尾加入...字符。
"""


def adjust_string_length(string, length):
    if len(string) < length:
        string += ' ' * (length - len(string))
    elif len(string) > length:
        string = string[:length-3] + '...'
    return string
