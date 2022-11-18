#! /usr/bin/env python
# -*- coding:utf8 -*-
import os
from abc import ABCMeta, abstractmethod
import pandas as pd

from scipy.stats import pearsonr

from common.constants import CONFIG_PATH, STOCK_TICK_ORGANIZED_DATA_PATH
from common.io import read_decompress
from data.process import StockTickDataColumnTransform


class Factor(metaclass=ABCMeta):
    """因子基类

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = ''
    version = '1.0'

    _params = []

    def get_params(self):
        return self._params

    def get_full_name(self):
        return self.factor_code + '-' + self.version + '[' \
               + (','.join(list(map(lambda x: str(x), self._params)))) + ']'

    def get_key(self, param):
        return self.factor_code + '.' + str(param)

    def get_keys(self):
        return list(map(lambda param: self.factor_code + '.' + str(param), self._params))

    def get_category(self):
        return self.factor_code.split('_')[1]

    # 两个因子相关性分析
    def compare(self, factor, data):
        data = self.caculate(data)
        data = factor.caculate(data)
        data = data.dropna()
        return pearsonr(data[self.factor_code], data[factor.factor_code])

    # 全局计算因子值
    @abstractmethod
    def caculate(self, data):
        pass


class StockTickFactor(Factor):
    """股票Tick因子基类，可以加载股票Tick数据

    Parameters
    ----------
    """

    def __init__(self):
        self._stocks_abstract_50 = pd.read_pickle(CONFIG_PATH + os.path.sep + '50_stocks_abstract.pkl')
        self._stocks_abstract_300 = pd.read_pickle(CONFIG_PATH + os.path.sep + '300_stocks_abstract.pkl')
        self._stocks_abstract_500 = pd.read_pickle(CONFIG_PATH + os.path.sep + '500_stocks_abstract.pkl')
        self._stocks_map = {
            'IC' : self._stocks_abstract_500,
            'IH' : self._stocks_abstract_50,
            'IF' : self._stocks_abstract_300
        }

    def get_stock_tick_data(self, product, date):
        """获取相关的股票tick数据，
        因为一次处理一个股指合约文件，所包含的信息：
        日期，品种
        TODO 这部分可以预处理
        Parameters
        ----------
        product ： 品种
        date ： 日期
        """
        stock_list = self.get_stock_list_by_date(product, date)
        file_path = self.create_stock_tick_data_path(date)
        columns = self.get_columns()
        data = pd.DataFrame(columns=columns)
        for stock in stock_list:
            print('Handle stock {0}'.format(stock))
            temp_data = read_decompress(file_path + stock + '.pkl')
            temp_data = temp_data.loc[:, columns]
            data = pd.concat([data, temp_data])
        return data

    def get_columns(self):
        return ['tscode','date','time']

    def create_stock_tick_data_path(self, date):
        file_prefix = 'stk_tick10_w_'
        date = date.replace('-','')
        year = date[0:4]
        month = date[4:6]
        return STOCK_TICK_ORGANIZED_DATA_PATH + file_prefix + year + os.path.sep + file_prefix + year + month + os.path.sep + date + os.path.sep

    def get_stock_list_by_date(self, product, date):
        """获取股票列表
        Parameters
        ----------
        product ： 品种
        date ： 日期
        """
        stock_abstract = self._stocks_map.get(product)
        date = date.replace('-', '')
        for key in stock_abstract.keys():
            start_date = key.split('_')[0]
            end_date = key.split('_')[1]
            if date >= start_date and date <= end_date:
                return stock_abstract[key]


    # 全局计算因子值
    @abstractmethod
    def caculate(self, data):
        pass



