#! /usr/bin/env python
# -*- coding:utf8 -*-
from abc import abstractmethod, ABCMeta
import os

from common.localio import read_decompress
from common.constants import CONFIG_PATH, STOCK_TICK_ORGANIZED_DATA_PATH, FACTOR_PATH, STOCK_TICK_DATA_PATH
from common.aop import timing

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

    def __init__(self, check_original=True):
        self._check_original = check_original

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
        return read_decompress(file_path + stock + '.pkl')

    def create_stock_tick_data_path(self, date):
        file_prefix = 'stk_tick10_w_'
        date = date.replace('-','')
        year = date[0:4]
        month = date[4:6]
        if self._check_original:
            root_path = STOCK_TICK_DATA_PATH
        else:
            root_path = STOCK_TICK_ORGANIZED_DATA_PATH
        return root_path + file_prefix + year + os.path.sep + file_prefix + year + month + os.path.sep + date + os.path.sep

class StockDailyDataAccess(StockDataAccess):

    def __init__(self):
        StockDataAccess.__init__(self, False)

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

if __name__ == '__main__':
    # print(StockDataAccess().access('20171106', '000021'))
    # print(StockDataAccess(False).access('20171106', '000021'))
    print(len(StockDailyDataAccess().access('2020-09-22')))
