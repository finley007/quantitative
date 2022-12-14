#! /usr/bin/env python
# -*- coding:utf8 -*-
import os
from abc import ABCMeta, abstractmethod
import pandas as pd
from datetime import datetime

from scipy.stats import pearsonr

from common.constants import CONFIG_PATH, STOCK_TICK_ORGANIZED_DATA_PATH, FACTOR_PATH
from common.localio import read_decompress
from data.process import StockTickDataColumnTransform
from data.access import StockDataAccess
from common.persistence.dbutils import create_session


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

    #加载因子文件
    def load(self, product):
        factor_path = FACTOR_PATH + product + '_' + self.get_full_name()
        return read_decompress(factor_path)

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
        self._session = create_session()
        suspend_list = self._session.execute('select distinct date, tscode from index_constituent_config where status = 1').fetchall()
        self._suspend_set = set(list(map(lambda suspend : suspend[0] + suspend[1], suspend_list)))

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
        columns = self.get_columns()
        data = pd.DataFrame(columns=columns)
        stock_data_access = StockDataAccess()
        for stock in stock_list:
            if (date + stock) in self._suspend_set:
                print('The stock {0} is suspended on {1}'.format(stock, date))
                continue
            print('Handle stock {0}'.format(stock))
            temp_data = stock_data_access.access(date, stock)
            temp_data = temp_data.loc[:, columns]
            temp_data = self.enrich_stock_data(date, stock, temp_data)
            data = pd.concat([data, temp_data])
        return data

    def enrich_stock_data(self, date, stock, data):
        """
        时间维度上处理股票数据

        Parameters
        ----------
        data

        Returns
        -------

        """
        return data

    def get_columns(self):
        return ['tscode','date','time']

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

    def merge_with_stock_data(self, data, date, df_stock_data_per_date):
        """
        和股票数据join，获取现货指标
        Parameters
        ----------
        data
        date
        df_stock_data_per_date

        Returns
        -------

        """
        df_stock_data_per_date['second_remainder'] = df_stock_data_per_date.apply(lambda item: self.is_aligned(item),
                                                                                  axis=1)
        df_stock_data_per_date = df_stock_data_per_date[df_stock_data_per_date['second_remainder'] == 0]
        df_stock_data_per_date['datetime'] = date + ' ' + df_stock_data_per_date['time'] + '000000'
        cur_date_data = data[data['date'] == date]
        cur_date_data = cur_date_data.merge(df_stock_data_per_date, on=['datetime'], how='left')
        return cur_date_data

    def is_aligned(self, item):
        '''过滤对齐到3秒线

        Parameters
        ----------
        item

        Returns
        -------

        '''
        cur_time = datetime.strptime(item['time'], '%H:%M:%S.%f')
        return cur_time.second % 3


    # 全局计算因子值
    @abstractmethod
    def caculate(self, data):
        pass



