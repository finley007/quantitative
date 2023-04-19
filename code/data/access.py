#! /usr/bin/env python
# -*- coding:utf8 -*-
from abc import abstractmethod, ABCMeta
import os
import time
from functools import lru_cache
import pandas as pd

from common.localio import read_decompress, list_files_in_path
from common.constants import CONFIG_PATH, STOCK_TICK_ORGANIZED_DATA_PATH, FACTOR_PATH, STOCK_TICK_DATA_PATH, STOCK_TICK_COMBINED_DATA_PATH, STOCK_FILE_PREFIX, FACTOR_STANDARD_FIELD_TYPE
from common.aop import timing
from common.stockutils import get_full_stockcode_for_stock
from framework.localconcurrent import ProcessRunner, ThreadRunner
from common.log import get_logger

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

class BatchStockDataAccess(StockDataAccess):
    """
    批量加载文件
    """

    def __init__(self, list, concurrent_count = 3):
        self._list = list
        print(self._list)
        self._concurrent_count = concurrent_count
        self._process_runner = ProcessRunner(self._concurrent_count)
        self._thread_runner = ThreadRunner(self._concurrent_count)
        self._data_access = StockDataAccess(False)

    @timing
    def batch_load_data(self):
        dict = {}
        for item in self._list:
            self._process_runner.execute(load_data, args=(self._data_access, item[0], item[1]))
        results = self._process_runner.get_results()
        for result in results:
            stock_data = result.get()
            if stock_data[0] in dict.keys():
                dict[stock_data[0]].update({stock_data[1]: stock_data[2]})
            else:
                dict[stock_data[0]] = {stock_data[1]: stock_data[2]}
        return dict

    @timing
    def batch_load_data_by_multiple_thread(self):
        dict = {}
        results = self._thread_runner.execute(self.thread_load_data, self._list)
        for result in results:
            if result[0] != '':
                dict[result[0]] = result[1]
        return dict

    def thread_load_data(self, *args):
        date = args[0][0]
        tscode = args[0][1]
        try:
            return date + '_' + tscode, self._data_access.access(date, tscode)
        except Exception as e:
            get_logger().warning('Stock data is missing for date: {0} and stock: {1}'.format(date, tscode))
            return date + '_' + tscode, pd.DataFrame()

def load_data(data_access, date, tscode):
    try:
        return date, date + '_' + tscode, data_access.access(date, tscode)
    except Exception as e:
        get_logger().warning('Stock data is missing for date: {0} and stock: {1}'.format(date, tscode))
        return date, date + '_' + tscode, pd.DataFrame()



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
    # print(create_stock_file_path('2022','01','15'))
    # print(create_stock_file_path('2022','01','15','000001'))

    # 测试批量加载
    path = 'E:\\data\\organized\\stock\\tick\\stk_tick10_w_2020\\stk_tick10_w_202011\\20201102'
    file_list = list_files_in_path(path)
    file_list = list(map(lambda item: ('20201102',item[0:6]), file_list))
    batch_access = BatchStockDataAccess(file_list)
    print(batch_access.batch_load_data())
    batch_access.batch_load_data_by_multiple_thread()

