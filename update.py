# -*- coding: utf-8 -*-
"""
finance data 模块中的数据更新程序
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from progressbar import Percentage, SimpleProgress, Bar, ETA, progressbar, ProgressBar


class UpdateData:
    '''
    数据更新类
    '''
    _data_set = None

    def __init__(self, data_set):
        '''
        data_set 参数为以DataSets为基类的金融数据类，对此类数据进行数据更新。
        '''
        self._data_set = data_set

    def simple_update(self):
        '''
        单进程简单数据更新
        '''
        # 更新金融数据类的索引数据
        self._data_set.update_index()

        # 更新金融数据类的数据
        lst = self._data_set.index
        result = {}
        pbar = ProgressBar(widgets=[Percentage(), ' ', Bar(
        ), ' ', SimpleProgress(), ' | ', ETA()], maxval=len(lst)).start()  # 画进度条
        for code in pbar(lst):
            result[code] = self._data_set.update_data(code)

        return result

    def concurrent_update(self, threads=2):
        '''
        多进程数据更新
        '''
        # 更新金融数据类的索引数据
        self._data_set.update_index()

        # 更新金融数据类的数据
        with ThreadPoolExecutor(max_workers=threads) as pool:
            task_list = []
            lst = self._data_set.index
            for code in lst:
                task_list.append(pool.submit(self._data_set.update_data, code))

            result = []
            i = 0
            pbar = ProgressBar(widgets=[Percentage(), ' ', Bar(
            ), ' ', SimpleProgress(), ' | ', ETA()], maxval=len(lst)).start()  # 画进度条
            for res in as_completed(task_list):
                result.append(res.result())
                i = i + 1
                pbar.update(i)

            return result
