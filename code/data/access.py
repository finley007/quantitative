#! /usr/bin/env python
# -*- coding:utf8 -*-
from abc import abstractmethod, ABCMeta
import os
import time
from functools import lru_cache

from common.localio import read_decompress
from common.constants import CONFIG_PATH, STOCK_TICK_ORGANIZED_DATA_PATH, FACTOR_PATH, STOCK_TICK_DATA_PATH, STOCK_TICK_COMBINED_DATA_PATH, STOCK_FILE_PREFIX, FACTOR_STANDARD_FIELD_TYPE
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
            return self.field_mapping(data)
        else:
            return self.field_mapping(read_decompress(file_path + stock + '.pkl'))

    def create_stock_tick_data_path(self, date):
        file_prefix = STOCK_FILE_PREFIX
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

    def field_mapping(self, data):
        """
        字段类型转换
        amount object -》 float64

        Parameters
        ----------
        data

        Returns
        -------

        """
        if data.dtypes['amount'] != FACTOR_STANDARD_FIELD_TYPE:
            data['amount'] = data['amount'].astype(FACTOR_STANDARD_FIELD_TYPE)
        return data

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

def create_stock_file_path(year, month, day, stock='', is_original=True):
    """
    生成股票数据文件完整路径

    Parameters
    ----------
    year
    month
    day
    stock
    is_original

    Returns
    -------

    """
    file_prefix = STOCK_FILE_PREFIX
    if is_original:
        root_path = STOCK_TICK_DATA_PATH
    else:
        root_path = STOCK_TICK_ORGANIZED_DATA_PATH
    full_path = root_path + file_prefix + year + os.path.sep + file_prefix + year + month + os.path.sep + (year+month+day) + os.path.sep
    if stock != '':
        full_path = full_path + stock
    return full_path


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
    # t = time.perf_counter()
    # print(StockDataAccess(False, True).access('20171106', '000021'))
    # print(f'cost time: {time.perf_counter() - t:.8f} s')
    #
    # t = time.perf_counter()
    # print(StockDataAccess(False, True).access('20171106', '000001'))
    # print(f'cost time: {time.perf_counter() - t:.8f} s')

    #测试加载日期文件
    # print(len(StockDailyDataAccess().access('2020-09-22')))

    #测试股票数据完整路径生成
    print(create_stock_file_path('2022','01','15'))
    print(create_stock_file_path('2022','01','15','000001'))

