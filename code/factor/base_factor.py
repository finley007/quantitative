#! /usr/bin/env python
# -*- coding:utf8 -*-
import os
from abc import ABCMeta, abstractmethod
import pandas as pd
from datetime import datetime

from scipy.stats import pearsonr

from common.constants import CONFIG_PATH, STOCK_TICK_ORGANIZED_DATA_PATH, FACTOR_PATH, TEMP_PATH
from common.localio import read_decompress, save_compress
from data.process import StockTickDataColumnTransform
from data.access import StockDataAccess, StockDailyDataAccess
from common.persistence.dbutils import create_session
from common.persistence.po import StockMissingData
from common.persistence.dao import IndexConstituentConfigDao
from common.aop import timing
from framework.localconcurrent import ThreadRunner
from common.log import get_logger
from framework.pagination import Pagination
from framework.localconcurrent import ProcessRunner, ProcessExcecutor


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
    """现货类基类，可以加载股票Tick数据

    Parameters
    ----------
    """

    _stock_filter = []

    def __init__(self):
        self._stocks_abstract_50 = pd.read_pickle(CONFIG_PATH + os.path.sep + '50_stocks_abstract.pkl')
        self._stocks_abstract_300 = pd.read_pickle(CONFIG_PATH + os.path.sep + '300_stocks_abstract.pkl')
        self._stocks_abstract_500 = pd.read_pickle(CONFIG_PATH + os.path.sep + '500_stocks_abstract.pkl')
        self._stocks_map = {
            'IC' : self._stocks_abstract_500,
            'IH' : self._stocks_abstract_50,
            'IF' : self._stocks_abstract_300
        }
        self._data_access = StockDataAccess(False)
        self._daily_data_access = StockDailyDataAccess()
        index_constituent_config_dao = IndexConstituentConfigDao()
        self._suspend_set = index_constituent_config_dao.get_suspend_list()

    def set_stock_filter(self, stock_filter):
        self._stock_filter = stock_filter

    @timing
    def get_stock_tick_data(self, product, instrument, date):
        """获取相关的股票tick数据，
        因为一次处理一个股指合约文件，所包含的信息：
        日期，品种
        TODO 这部分可以预处理
        Parameters
        ----------
        product ： 品种
        instrument ： 合约
        date ： 日期
        """
        stock_list = self.get_stock_list_by_date(product, date)
        if len(self._stock_filter) > 0: #用于生成人工检测文件
            stock_list = self._stock_filter
        columns = self.get_columns()
        data = pd.DataFrame(columns=columns)
        if stock_list and len(stock_list) > 0:
            stock_list = list(filter(lambda stock: (date + stock) not in self._suspend_set, stock_list))
            for stock in stock_list:
                get_logger().debug('Handle stock {0}'.format(stock))
                try:
                    temp_data = self.get_stock_data(date, stock)
                except Exception as e:
                    session = create_session()
                    stock_missing_data = StockMissingData(date, stock)
                    session.add(stock_missing_data)
                    session.commit()
                    continue
                temp_data = temp_data.loc[:, columns]
                temp_data = self.enrich_stock_data(instrument, date, stock, temp_data)
                data = pd.concat([data, temp_data])
        else:
            get_logger().warning('Stock data is missing for product: {0} and date: {1}'.format(product, date))
        return data
        # return self._daily_data_access.access(date)

    # @timing
    # def get_stock_tick_data(self, product, instrument, date):
    #     """获取相关的股票tick数据，
    #     因为一次处理一个股指合约文件，所包含的信息：
    #     日期，品种
    #     TODO 这部分可以预处理
    #     Parameters
    #     ----------
    #     product ： 品种
    #     instrument ： 合约
    #     date ： 日期
    #     """
    #     stock_list = self.get_stock_list_by_date(product, date)
    #     columns = self.get_columns()
    #     data = pd.DataFrame(columns=columns)
    #     if stock_list and len(stock_list) > 0:
    #         stock_list = list(filter(lambda stock: (date + stock) not in self._suspend_set, stock_list))
    #         stock_date_list = list(map(lambda stock: [instrument, date, stock], stock_list))
    #         stock_data_list = ThreadRunner(10).execute(self.get_stock_tick_data_by_stock, stock_date_list)
    #         for stock_data in stock_data_list:
    #             if len(stock_data) > 0:
    #                 data = pd.concat([data, stock_data])
    #     else:
    #         get_logger().warning('Stock data is missing for product: {0} and date: {1}'.format(product, date))
    #     return data
    #
    # def get_stock_tick_data_by_stock(self, info):
    #     instrument = info[0]
    #     date = info[1]
    #     stock = info[2]
    #     columns = self.get_columns()
    #     temp_data = pd.DataFrame(columns=columns)
    #     get_logger().debug('Handle stock {0}'.format(stock))
    #     try:
    #         temp_data = self.get_stock_data(date, stock)
    #     except Exception as e:
    #         session = create_session()
    #         stock_missing_data = StockMissingData(date, stock)
    #         session.add(stock_missing_data)
    #         session.commit()
    #         return temp_data
    #     temp_data = temp_data.loc[:, columns]
    #     temp_data = self.enrich_stock_data(instrument, date, stock, temp_data)
    #     return temp_data

    @timing
    def get_stock_data(self, date, stock):
        temp_data = self._data_access.access(date, stock)
        return temp_data
    @timing
    def enrich_stock_data(self, instrument, date, stock, data):
        """
        时间维度上处理股票数据

        Parameters
        ----------
        data

        Returns
        -------

        """
        return data

    def get_factor_columns(self, data):
        """
        获取因子文件的列
        Returns
        -------

        """
        columns = data.columns.tolist() + [self.get_key(), 'time', 'second_remainder']
        return columns

    def get_columns(self):
        """
        需要获取股票tick数据的列
        Returns
        -------

        """
        return ['tscode','date','time']
    @timing
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

    @timing
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
        cur_date_data = data[data['date'] == date]
        if len(df_stock_data_per_date) > 0:
            df_stock_data_per_date['second_remainder'] = df_stock_data_per_date.apply(lambda item: self.is_aligned(item), axis=1)
            df_stock_data_per_date = df_stock_data_per_date[df_stock_data_per_date['second_remainder'] == 0]
            df_stock_data_per_date['datetime'] = date + ' ' + df_stock_data_per_date['time'] + '000000'
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

    @timing
    def caculate(self, data):
        columns = self.get_factor_columns(data)
        new_data = pd.DataFrame(columns=columns)
        product = data.iloc[0]['product']
        instrument = data.iloc[0]['instrument']
        date_list = list(set(data['date'].tolist()))
        date_list.sort()
        pagination = Pagination(date_list, page_size=20)
        while pagination.has_next():
            date_list = pagination.next()
            params_list = list(map(lambda date: [date, instrument, product], date_list))
            results = ProcessExcecutor(20).execute(self.caculate_by_date, params_list)
            temp_cache = {}
            for result in results:
                cur_date_data = self.merge_with_stock_data(data, result[0], result[1])
                temp_cache[result[0]] = cur_date_data
            for date in date_list:
                new_data = pd.concat([new_data, temp_cache[date]])
        return new_data

    # 全局计算因子值
    @abstractmethod
    def caculate_by_date(self, *args):
        """
        计算因子值，以合约为单位
        Parameters
        ----------
        data

        Returns
        -------

        """
        pass


class TimewindowStockTickFactor(StockTickFactor):
    """用于因子计算需要跨天的情形

    Parameters
    ----------
    """

    def __init__(self):
        StockTickFactor.__init__(self)
        self._instrument_stock_data = {}

    @timing
    def prepare_timewindow_data(self, instrument):
        """
        按合约生成数据，用于需要时间窗计算的因子
        TODO 这部分可以预处理
        Parameters
        ----------
        instrument ： 合约
        """
        # 一级内存缓存 多进程手写缓存会溢出，考虑用框架
        # if instrument in self._instrument_stock_data:
        #     return self._instrument_stock_data[instrument]
        session = create_session()
        result_list = session.execute(
            'select t2.tscode, t2.date from future_instrument_config t1, index_constituent_config t2 '
            'where t1.date = t2.date and t1.product = t2.product and t2.status = 0 and t1.instrument = :instrument '
            'order by t2.product, t2.tscode, t2.date', {'instrument': instrument}).fetchall()
        # stock -> datelist
        stock_date_map = {}
        for item in result_list:
            if item[0] in stock_date_map:
                stock_date_map[item[0]].append(item[1])
            else:
                stock_date_map[item[0]] = [item[1]]
        stock_data = {}
        for stock in stock_date_map.keys():
            #二级磁盘缓存
            file_path = TEMP_PATH + os.path.sep + 'stock' + os.path.sep + instrument + '_' + stock + '.pkl'
            if os.path.exists(file_path):
                data = read_decompress(file_path)
            else:
                instrument_stock_data = pd.DataFrame()
                for date in stock_date_map[stock]:
                    try:
                        temp_data = self._data_access.access(date, stock)
                    except Exception as e:
                        get_logger().error('Access date: {0} and stock: {1} error'.format(date, stock)) #对于有些缺失的数据先忽略，比如2017.12.01，在计算日期跨度也会忽略这天的数据
                        continue
                    instrument_stock_data = pd.concat([instrument_stock_data, temp_data])
                data = instrument_stock_data
                save_compress(data, file_path)
            stock_data[stock] = data
        # self._instrument_stock_data[instrument] = stock_data
        return stock_data

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




