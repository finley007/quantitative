#! /usr/bin/env python
# -*- coding:utf8 -*-
from abc import abstractmethod, ABCMeta
import os
import time
from functools import lru_cache

from common.localio import read_decompress
from common.constants import CONFIG_PATH, STOCK_TICK_ORGANIZED_DATA_PATH, FACTOR_PATH, STOCK_TICK_DATA_PATH, STOCK_TICK_COMBINED_DATA_PATH
from common.aop import timing
from common.stockutils import get_full_stockcode_for_stock

class DataAccess(metaclass=ABCMeta):
    """数据获取接口

    """

    @classmethod
    @abstractmethod
    def access(self, *args):
        pass


class StockDataAccess(DataAccess):
    """股票数据获取接口

    """

    def __init__(self, check_original=True, combined_file_enabled=False):
        self._check_original = check_original
        if check_original:
            self._combined_file_enabled = False
        else:
            self._combined_file_enabled = combined_file_enabled

    def access(self, *args):
        """

        Parameters
        ----------
        args: tuple
            args[0]: date
            args[1]: tscode

        Returns
        -------

        """
        date = args[0]
        stock = args[1]
        file_path = self.create_stock_tick_data_path(date)
        if self._combined_file_enabled:
            data = read_date_combined_file(date, file_path)
            data = data[data['tscode'] == get_full_stockcode_for_stock(stock)]
            return data
        else:
            return read_decompress(file_path + stock + '.pkl')

    def create_stock_tick_data_path(self, date):
        file_prefix = 'stk_tick10_w_'
        date = date.replace('-','')
        year = date[0:4]
        month = date[4:6]
        if self._check_original:
            root_path = STOCK_TICK_DATA_PATH
        else:
            if self._combined_file_enabled:
                root_path = STOCK_TICK_COMBINED_DATA_PATH
            else:
                root_path = STOCK_TICK_ORGANIZED_DATA_PATH
        return root_path + file_prefix + year + os.path.sep + file_prefix + year + month + os.path.sep + date + os.path.sep

class StockDailyDataAccess(StockDataAccess):
    """
    读取股票按天合并文件，只是测试用
    """

    def __init__(self):
        StockDataAccess.__init__(self, False, True)

    def access(self, *args):
        """

        Parameters
        ----------
        args: tuple
            args[0]: date
            args[1]: tscode

        Returns
        -------

        """
        date = args[0]
        file_path = self.create_stock_tick_data_path(date)
        date = date.replace('-', '')
        return read_decompress(file_path + date + '.pkl')

@lru_cache(maxsize=5)
def read_date_combined_file(date, file_path):
    date = date.replace('-', '')
    return read_decompress(file_path + date + '.pkl')


if __name__ == '__main__':
    #测试加载股票原始文件
    # t = time.perf_counter()
    # print(StockDataAccess().access('20171106', '000021'))
    # print(f'cost time: {time.perf_counter() - t:.8f} s')
    #
    # #测试加载股票生成文件
    # t = time.perf_counter()
    # print(StockDataAccess(False).access('20171106', '000021'))
    # print(f'cost time: {time.perf_counter() - t:.8f} s')
    #
    # #测试加载日期文件和缓存
    t = time.perf_counter()
    print(StockDataAccess(False, True).access('20171106', '000021'))
    print(f'cost time: {time.perf_counter() - t:.8f} s')

    t = time.perf_counter()
    print(StockDataAccess(False, True).access('20171106', '000001'))
    print(f'cost time: {time.perf_counter() - t:.8f} s')

    #测试加载日期文件
    # print(len(StockDailyDataAccess().access('2020-09-22')))

