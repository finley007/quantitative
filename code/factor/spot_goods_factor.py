#! /usr/bin/env python
# -*- coding:utf8 -*-
import pandas as pd
import time
from memory_profiler import profile
import numpy as np

from factor.base_factor import Factor, StockTickFactor, TimewindowStockTickFactor, StockTickDifferenceFactor, StockTickMeanFactor, StockTickStdFactor
from common.constants import TEST_PATH, STOCK_TRANSACTION_START_TIME, STOCK_OPEN_CALL_AUACTION_2ND_STAGE_START_TIME, \
    STOCK_OPEN_CALL_AUACTION_2ND_STAGE_END_TIME, STOCK_OPEN_CALL_AUACTION_1ST_STAGE_START_TIME, STOCK_INDEX_INFO, FACTOR_STANDARD_FIELD_TYPE
from common.localio import read_decompress, save_compress
from common.aop import timing
from common.visualization import draw_analysis_curve
from common.timeutils import add_milliseconds_suffix, get_last_or_next_trading_date_list_by_stock
from framework.pagination import Pagination
from data.access import StockDataAccess
from framework.localconcurrent import ProcessRunner, ProcessExcecutor
from common.log import get_logger
from common.stockutils import approximately_equal_to, get_rising_falling_limit
from common.persistence.dao import FutureInstrumentConfigDao
from common.commonutils import local_divide

"""现货类因子
分类编号：02
"""

class AuxiliaryFileGenerationFactor(StockTickFactor):
    """
    现货辅助因子，该因子不用于模型训练，只用于生成其它因子
    """
    factor_code = 'FCT_AUXILIARY_FILE_GENERATION'
    version = '1.0'

    def __init__(self):
        StockTickFactor.__init__(self)

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['10_grade_bid_amount', '10_grade_ask_amount', '5_grade_bid_amount', '5_grade_ask_amount']
        return columns

    def execute_caculation(self, date, stock_data_per_date):
        stock_data_per_date = stock_data_per_date[stock_data_per_date['time'] > add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)]
        stock_data_per_date = stock_data_per_date.rename(columns={'time': 'cur_time'})
        stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')['10_grade_bid_amount', '10_grade_ask_amount', '5_grade_bid_amount', '5_grade_ask_amount'].mean()
        stock_data_per_date_group_by['time'] = stock_data_per_date_group_by.index
        return date, stock_data_per_date_group_by

class TotalCommissionRatioFactor(StockTickFactor):
    """总委比因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_001_TOTAL_COMMISSION_RATIO'
    version = '1.0'

    def __init__(self):
        StockTickFactor.__init__(self)

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['weighted_average_bid_price', 'weighted_average_ask_price', 'total_bid_volume', 'total_ask_volume']
        return columns

    def execute_caculation(self, date, stock_data_per_date):
        stock_data_per_date = stock_data_per_date[stock_data_per_date['time'] > add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)]
        stock_data_per_date['ask_commission_amount'] = stock_data_per_date['total_ask_volume'] * stock_data_per_date['weighted_average_ask_price']
        stock_data_per_date['bid_commission_amount'] = stock_data_per_date['total_bid_volume'] * stock_data_per_date['weighted_average_bid_price']
        stock_data_per_date['total_commission_amount'] = stock_data_per_date['ask_commission_amount'] + stock_data_per_date['bid_commission_amount']
        stock_data_per_date = stock_data_per_date.rename(columns={'time': 'cur_time'})
        stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')['bid_commission_amount', 'total_commission_amount'].sum()
        stock_data_per_date_group_by[self.get_key()] = stock_data_per_date_group_by.apply(
            lambda x: 0 if x['total_commission_amount'] == 0 else x['bid_commission_amount'] / x['total_commission_amount'], axis=1)
        df_stock_data_per_date = pd.DataFrame({self.get_key(): stock_data_per_date_group_by[self.get_key()], 'time': stock_data_per_date_group_by.index})
        # 过滤对齐在3秒线的数据
        return date, df_stock_data_per_date



class TenGradeCommissionRatioFactor(StockTickFactor):
    """10档委比因子
    2.0 直接计算出股票数据里的10档委买委卖额
    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_002_10_GRADE_COMMISSION_RATIO'
    version = '2.0'

    def __init__(self):
        StockTickFactor.__init__(self)

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['10_grade_bid_amount', '10_grade_ask_amount']
        return columns

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    def execute_caculation(self, date, stock_data_per_date):
        stock_data_per_date = stock_data_per_date[stock_data_per_date['time'] > add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)]
        stock_data_per_date = stock_data_per_date.rename(columns={'time': 'cur_time'})
        stock_data_per_date['10_grade_total_amount'] = stock_data_per_date['10_grade_bid_amount'] + stock_data_per_date['10_grade_ask_amount']
        stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')['10_grade_bid_amount', '10_grade_total_amount'].sum()
        stock_data_per_date_group_by[self.get_key()] = stock_data_per_date_group_by.apply(
            lambda x: 0 if x['10_grade_total_amount'] == 0 else x['10_grade_bid_amount'] / x['10_grade_total_amount'], axis=1)
        df_stock_data_per_date = pd.DataFrame(
            {self.get_key(): stock_data_per_date_group_by[self.get_key()], 'time': stock_data_per_date_group_by.index})
        return date, df_stock_data_per_date


class TenGradeWeightedCommissionRatioFactor(TenGradeCommissionRatioFactor):
    """10档加权委比因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_003_10_GRADE_WEIGHTED_COMMISSION_RATIO'
    version = '1.0'

    def __init__(self):
        StockTickFactor.__init__(self)

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['bid_price1', 'bid_price2', 'bid_price3', 'bid_price4', 'bid_price5', 'bid_price6',
                             'bid_price7', 'bid_price8', 'bid_price9', 'bid_price10', 'bid_volume1', 'bid_volume2',
                             'bid_volume3', 'bid_volume4', 'bid_volume5', 'bid_volume6', 'bid_volume7', 'bid_volume8',
                             'bid_volume9', 'bid_volume10', 'ask_price1', 'ask_price2', 'ask_price3', 'ask_price4', 'ask_price5', 'ask_price6',
                             'ask_price7', 'ask_price8', 'ask_price9', 'ask_price10', 'ask_volume1', 'ask_volume2',
                             'ask_volume3', 'ask_volume4', 'ask_volume5', 'ask_volume6', 'ask_volume7', 'ask_volume8',
                             'ask_volume9', 'ask_volume10']
        return columns

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    def execute_caculation(self, date, stock_data_per_date):
        stock_data_per_date = stock_data_per_date[stock_data_per_date['time'] > add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)]
        stock_data_per_date = stock_data_per_date.rename(columns={'time': 'cur_time'})
        stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')[
            '10_grade_bid_commission_amount', '10_grade_total_commission_amount'].sum()
        stock_data_per_date_group_by[self.get_key()] = stock_data_per_date_group_by.apply(
            lambda x: 0 if x['10_grade_total_commission_amount'] == 0 else x['10_grade_bid_commission_amount'] / x[
                '10_grade_total_commission_amount'], axis=1)
        df_stock_data_per_date = pd.DataFrame(
            {self.get_key(): stock_data_per_date_group_by[self.get_key()], 'time': stock_data_per_date_group_by.index})
        return date, df_stock_data_per_date

    def amount_sum(self, item):
        weighted_list = [10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
        return np.dot(item[3:13] * item[13:23], weighted_list), np.dot(item[23:33] * item[33:43], weighted_list)

    def enrich_stock_data(self, instrument, date, stock, data):
        data = data[data['time'] > add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)]
        data[['10_grade_bid_commission_amount', '10_grade_ask_commission_amount']] = np.apply_along_axis(
            lambda item: self.amount_sum(item), axis=1, arr=data.values)
        data['10_grade_total_commission_amount'] = data['10_grade_ask_commission_amount'] + data['10_grade_bid_commission_amount']
        return data

class FiveGradeCommissionRatioFactor(StockTickFactor):
    """5档委比因子
    2.0 直接计算出股票数据里的5档委买委卖额
    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_004_5_GRADE_COMMISSION_RATIO'
    version = '2.0'

    def __init__(self):
        StockTickFactor.__init__(self)

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['5_grade_bid_amount', '5_grade_ask_amount']
        return columns

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    def execute_caculation(self, date, stock_data_per_date):
        stock_data_per_date = stock_data_per_date[stock_data_per_date['time'] > add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)]
        stock_data_per_date['5_grade_total_amount'] = stock_data_per_date['5_grade_bid_amount'] + stock_data_per_date['5_grade_ask_amount']
        stock_data_per_date = stock_data_per_date.rename(columns={'time': 'cur_time'})
        stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')['5_grade_bid_amount', '5_grade_total_amount'].sum()
        stock_data_per_date_group_by[self.get_key()] = stock_data_per_date_group_by.apply(
            lambda x: 0 if x['5_grade_total_amount'] == 0 else x['5_grade_bid_amount'] / x['5_grade_total_amount'], axis=1)
        df_stock_data_per_date = pd.DataFrame(
            {self.get_key(): stock_data_per_date_group_by[self.get_key()], 'time': stock_data_per_date_group_by.index})
        return date, df_stock_data_per_date


class FiveGradeWeightedCommissionRatioFactor(FiveGradeCommissionRatioFactor):
    """5档加权委比因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_005_5_GRADE_WEIGHTED_COMMISSION_RATIO'
    version = '1.0'

    def __init__(self):
        StockTickFactor.__init__(self)

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['bid_price1', 'bid_price2', 'bid_price3', 'bid_price4', 'bid_price5', 'bid_volume1', 'bid_volume2', 'bid_volume3', 'bid_volume4', 'bid_volume5',
                             'ask_price1', 'ask_price2', 'ask_price3', 'ask_price4', 'ask_price5', 'ask_volume1', 'ask_volume2', 'ask_volume3','ask_volume4','ask_volume5']
        return columns

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    def execute_caculation(self, date, stock_data_per_date):
        stock_data_per_date = stock_data_per_date[stock_data_per_date['time'] > add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)]
        stock_data_per_date = stock_data_per_date.rename(columns={'time': 'cur_time'})
        stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')[
            '5_grade_bid_commission_amount', '5_grade_total_commission_amount'].sum()
        stock_data_per_date_group_by[self.get_key()] = stock_data_per_date_group_by.apply(
            lambda x: 0 if x['5_grade_total_commission_amount'] == 0 else x['5_grade_bid_commission_amount'] / x[
                '5_grade_total_commission_amount'], axis=1)
        df_stock_data_per_date = pd.DataFrame(
            {self.get_key(): stock_data_per_date_group_by[self.get_key()], 'time': stock_data_per_date_group_by.index})
        return date, df_stock_data_per_date

    def amount_sum(self, item):
        weighted_list = [5, 4, 3 ,2 ,1]
        return np.dot(item[3:8] * item[8:13], weighted_list), np.dot(item[13:18] * item[18:23], weighted_list)

    def enrich_stock_data(self, instrument, date, stock, data):
        data = data[data['time'] > add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)]
        data[['5_grade_bid_commission_amount', '5_grade_ask_commission_amount']] = np.apply_along_axis(
            lambda item: self.amount_sum(item), axis=1, arr=data.values)
        data['5_grade_total_commission_amount'] = data['5_grade_ask_commission_amount'] + data[
            '5_grade_bid_commission_amount']
        return data


class RisingStockRatioFactor(StockTickFactor):
    """上涨股票比例因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_006_RISING_STOCK_RATIO'
    version = '1.0'

    def __init__(self, params=[20, 50 ,100 ,200]):
        StockTickFactor.__init__(self)
        self._params = params

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['price', 'close']
        return columns

    def get_factor_columns(self, data):
        key_columns = []
        for param in self._params:
            key_columns = key_columns + [self.get_key(param)]
        columns = data.columns.tolist() + key_columns + ['time', 'second_remainder']
        return columns

    def execute_caculation(self, date, stock_data_per_date):
        stock_data_per_date = stock_data_per_date[stock_data_per_date['time'] > add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)]
        change_columns = []
        columns_replace = {}
        for param in self._params:
            change_columns = change_columns + ['change.' + str(param)]
            columns_replace['change.' + str(param)] = self.get_key(param)
        stock_data_per_date = stock_data_per_date.rename(columns={'time': 'cur_time'})
        if len(stock_data_per_date) == 0:
            return date, stock_data_per_date
        stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')[change_columns].apply(
            lambda item: self.get_rise_ratio(item))
        stock_data_per_date_group_by['time'] = stock_data_per_date_group_by.index
        # 替换列名
        stock_data_per_date_group_by = stock_data_per_date_group_by.rename(columns=columns_replace)
        return date, stock_data_per_date_group_by

    def enrich_stock_data(self, instrument, date, stock, data):
        get_logger().debug('Current date: {} and stock: {}'.format(date, stock))
        data = data.reset_index(drop=True)
        temp_data = data[data['time'] >= add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)]
        for param in self._params:
            #盘前
            data['change.' + str(param)] = 0
            #交易时间
            temp_data.loc[:, 'change.' + str(param)] = temp_data['price'] - temp_data['price'].shift(param)
            # 不足param长度的用昨日收盘价计算
            temp_data.loc[np.isnan(temp_data['change.' + str(param)]), 'change.' + str(param)] = temp_data['price'] - temp_data['close']
        pre_data = data[data['time'] < add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)]
        data = pd.concat([pre_data, temp_data])
        return data

    def get_rise_ratio(self, item):
        rise_ratio = item[item > 0].count() / item.count()
        # rise_ratio = np.sum(item > 0) / len(item)
        return rise_ratio

class SpreadFactor(StockTickFactor):
    """价差因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_007_SPREAD'
    version = '1.0'

    def __init__(self):
        StockTickFactor.__init__(self)

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['bid_price1', 'ask_price1']
        return columns

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    def execute_caculation(self, date, stock_data_per_date):
        stock_data_per_date = stock_data_per_date[stock_data_per_date['time'] > add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)]
        stock_data_per_date[self.get_key()] = (stock_data_per_date['bid_price1'] - stock_data_per_date[
            'ask_price1']) / (stock_data_per_date['bid_price1'] + stock_data_per_date['ask_price1'])
        stock_data_per_date = stock_data_per_date.rename(columns={'time': 'cur_time'})
        stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')[self.get_key()].mean()
        df_stock_data_per_date = pd.DataFrame(
            {self.get_key(): stock_data_per_date_group_by, 'time': stock_data_per_date_group_by.index})
        return date, df_stock_data_per_date


class CallAuctionSecondStageIncreaseFactor(StockTickFactor):
    """集合竞价二阶段涨幅因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_008_CALL_AUCTION_SECOND_STAGE_INCREASE'
    version = '1.0'

    def __init__(self):
        StockTickFactor.__init__(self)

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['price', 'ask_price1', 'volume']
        return columns

    def get_factor_columns(self, data):
        columns = data.columns.tolist() + [self.get_key()]
        return columns

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    def execute_caculation(self, date, stock_data_per_date):
        stock_data_per_date = stock_data_per_date.rename(columns={'time': 'cur_time'})
        stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')['increase_rate'].mean()
        df_stock_data_per_date = pd.DataFrame(
            {self.get_key(): stock_data_per_date_group_by,
             'time': stock_data_per_date_group_by.index})
        return date, df_stock_data_per_date

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
        get_logger().debug('Current date: {} and stock: {}'.format(date, stock))
        start_time_data = data[(data['time'] >= add_milliseconds_suffix(STOCK_OPEN_CALL_AUACTION_2ND_STAGE_START_TIME)) & (data['time'] <= add_milliseconds_suffix(STOCK_OPEN_CALL_AUACTION_2ND_STAGE_END_TIME))]
        # 这里结束时间不能用时间点卡，因为很多情况二阶段成交会有延时，不是正好在09:25:00
        end_time_data = data[(data['time'] <= add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)) & (data['volume'] > 0)]
        # 有些数据没有集合竞价信息，如：'2017-12-18', '002311'，这时能不能用开盘涨幅？
        data['increase_rate'] = 0
        if len(start_time_data) > 0 and len(end_time_data) > 0:
            start_price = start_time_data.iloc[0]['ask_price1']
            end_price = end_time_data.iloc[0]['price']
            data.loc[data['time'] >= add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME), 'increase_rate'] = (end_price - start_price) / start_price
        return data

class TwoCallAuctionStageDifferenceFactor(StockTickFactor):
    """集合竞价二阶段涨幅减去集合竞价一阶段涨幅因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_009_TWO_CALL_AUCTION_STAGE_DIFFERENCE'
    version = '1.0'

    def __init__(self):
        StockTickFactor.__init__(self)

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['ask_price1', 'price', 'volume']
        return columns

    def get_factor_columns(self, data):
        columns = data.columns.tolist() + [self.get_key()]
        return columns

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]
    def execute_caculation(self, date, stock_data_per_date):
        stock_data_per_date = stock_data_per_date.rename(columns={'time': 'cur_time'})
        stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')['rate_difference'].mean()
        df_stock_data_per_date = pd.DataFrame(
            {self.get_key(): stock_data_per_date_group_by,
             'time': stock_data_per_date_group_by.index})
        return date, df_stock_data_per_date

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
        get_logger().debug('Current date: {} and stock: {}'.format(date, stock))
        stage_one_data = data[(data['time'] >= add_milliseconds_suffix(STOCK_OPEN_CALL_AUACTION_1ST_STAGE_START_TIME)) & (data['time'] <= add_milliseconds_suffix(STOCK_OPEN_CALL_AUACTION_2ND_STAGE_START_TIME))]
        stage_two_start_time_data = data[(data['time'] >= add_milliseconds_suffix(STOCK_OPEN_CALL_AUACTION_2ND_STAGE_START_TIME)) & (data['time'] <= add_milliseconds_suffix(STOCK_OPEN_CALL_AUACTION_2ND_STAGE_END_TIME))]
        stage_two_end_time_data = data[(data['time'] <= add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)) & (data['volume'] > 0)]
        data['stage_one_increase_rate'] = 0
        data['stage_two_increase_rate'] = 0
        if len(stage_one_data) > 0 and len(stage_two_start_time_data) > 0 and len(stage_two_end_time_data) > 0:
            stage_one_end_price = stage_one_data.iloc[-1]['ask_price1']
            stage_one_start_price = stage_one_data.iloc[0]['ask_price1']
            data.loc[data['time'] >= add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME), 'stage_one_increase_rate'] = (stage_one_end_price - stage_one_start_price) / stage_one_start_price
            stage_two_start_price = stage_two_start_time_data.iloc[0]['ask_price1']
            stage_two_end_price = stage_two_end_time_data.iloc[0]['price']
            data.loc[data['time'] >= add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME), 'stage_two_increase_rate'] = (stage_two_end_price - stage_two_start_price) / stage_two_start_price
        data['rate_difference'] = data['stage_two_increase_rate'] - data['stage_one_increase_rate']
        return data


class CallAuctionSecondStageReturnVolatilityFactor(StockTickFactor):
    """集合竞价二阶段收益率波动率因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_010_CALL_AUCTION_SECOND_STAGE_RETURN_VOLATILITY'
    version = '1.0'

    def __init__(self):
        StockTickFactor.__init__(self)

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['bid_price1', 'ask_price1', 'volume']
        return columns

    def get_factor_columns(self, data):
        columns = data.columns.tolist() + [self.get_key()]
        return columns

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    def execute_caculation(self, date, stock_data_per_date):
        stock_data_per_date = stock_data_per_date.rename(columns={'time': 'cur_time'})
        stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')['mean_price_return_std'].mean()
        df_stock_data_per_date = pd.DataFrame(
            {self.get_key(): stock_data_per_date_group_by,
             'time': stock_data_per_date_group_by.index})
        return date, df_stock_data_per_date

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
        get_logger().debug('Current date: {} and stock: {}'.format(date, stock))
        data['mean_price_return_std'] = 0
        temp_data = data[(data['time'] <= add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)) & (data['volume'] > 0)]
        if len(temp_data) > 0:
            end_time = temp_data.iloc[0]['time']
            temp_data = data[(data['time'] >= add_milliseconds_suffix(STOCK_OPEN_CALL_AUACTION_2ND_STAGE_START_TIME)) & (data['time'] <= end_time)]
            if len(temp_data) > 0:
                temp_data['mean_price'] = (temp_data['ask_price1'] + temp_data['bid_price1']) / 2
                # 这里不会有未来函数，因为实际已经开盘
                temp_data['mean_price_return'] = (temp_data['mean_price'].shift(-1) - temp_data['mean_price']) / temp_data['mean_price']
                data.loc[data['time'] >= add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME), 'mean_price_return_std'] = temp_data['mean_price_return'].std()
        return data


class FirstStageCommissionRatioFactor(StockTickFactor):
    """集合竞价一阶段委比因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_011_FIRST_STAGE_COMMISSION_RATIO'
    version = '1.0'

    def __init__(self):
        StockTickFactor.__init__(self)

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['bid_price1', 'bid_volume1', 'bid_volume2',
                             'ask_price1', 'ask_volume1', 'ask_volume2']
        return columns

    def get_factor_columns(self, data):
        columns = data.columns.tolist() + [self.get_key()]
        return columns

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    @timing
    def caculate(self, data):
        product = data.iloc[0]['product']
        instrument = data.iloc[0]['instrument']
        date_list = list(set(data['date'].tolist()))
        date_list.sort()
        for date in date_list:
            stock_data_per_date = self.get_stock_tick_data(product, instrument, date)
            if len(stock_data_per_date) == 0:
                get_logger().warning('The data on date: {0} and instrument: {1} is missing'.format(date, instrument))
                continue
            #截取一阶段数据
            stock_data_per_date = stock_data_per_date[
                (stock_data_per_date['time'] >= add_milliseconds_suffix(STOCK_OPEN_CALL_AUACTION_1ST_STAGE_START_TIME)) & (
                            stock_data_per_date['time'] <= add_milliseconds_suffix(STOCK_OPEN_CALL_AUACTION_2ND_STAGE_START_TIME))]
            stock_data_per_date['ask_commission_amount'] = stock_data_per_date.apply(lambda item: self.amount_sum(item, 'ask'), axis=1)
            stock_data_per_date['bid_commission_amount'] = stock_data_per_date.apply(lambda item: self.amount_sum(item, 'bid'), axis=1)
            #两个维度求和
            total_ask_commission_amount = stock_data_per_date['ask_commission_amount'].sum()
            total_bid_commission_amount = stock_data_per_date['bid_commission_amount'].sum()
            data.loc[data['date'] == date, self.get_key()] = total_bid_commission_amount/(total_ask_commission_amount + total_bid_commission_amount)
        return data

    def amount_sum(self, item, type):
        sum = item[type + '_price1'] * item[type + '_volume1'] + item[type + '_price1'] * item[type + '_volume2']
        return sum

class SecondStageCommissionRatioFactor(StockTickFactor):
    """集合竞价二阶段委比因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_012_SECOND_STAGE_COMMISSION_RATIO'
    version = '1.0'

    def __init__(self):
        StockTickFactor.__init__(self)

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['bid_price1', 'bid_volume1', 'bid_price2', 'bid_volume2','bid_price3', 'bid_volume3','bid_price4', 'bid_volume4','bid_price5', 'bid_volume5','bid_price6', 'bid_volume6','bid_price7', 'bid_volume7','bid_price8', 'bid_volume8','bid_price9', 'bid_volume9','bid_price10', 'bid_volume10',
                             'ask_price1', 'ask_volume1', 'ask_price2', 'ask_volume2', 'ask_price3', 'ask_volume3','ask_price4', 'ask_volume4','ask_price5', 'ask_volume5','ask_price6', 'ask_volume6','ask_price7', 'ask_volume7','ask_price8', 'ask_volume8','ask_price9', 'ask_volume9','ask_price10', 'ask_volume10', 'volume']
        return columns

    def get_factor_columns(self, data):
        columns = data.columns.tolist() + [self.get_key()]
        return columns

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    @timing
    def caculate(self, data):
        product = data.iloc[0]['product']
        instrument = data.iloc[0]['instrument']
        date_list = list(set(data['date'].tolist()))
        date_list.sort()
        for date in date_list:
            stock_data_per_date = self.get_stock_tick_data(product, instrument, date)
            if len(stock_data_per_date) == 0:
                get_logger().warning('The data on date: {0} and instrument: {1} is missing'.format(date, instrument))
                continue
            total_ask_commission_amount = stock_data_per_date['ask_commission_amount'].sum()
            total_bid_commission_amount = stock_data_per_date['bid_commission_amount'].sum()
            data.loc[data['date'] == date, self.get_key()] = total_bid_commission_amount/(total_ask_commission_amount + total_bid_commission_amount)
        return data

    def amount_sum(self, item, type):
        sum = 0
        if item['bid_price2'] == 0:
            sum = item[type + '_price1'] * item[type + '_volume1'] + item[type + '_price1'] * item[type + '_volume2']
        else: #对应有成交量的一条记录
            for i in range(1, 11):
                sum = sum + ((item[type + '_price' + str(i)]) * (item[type + '_volume' + str(i)]))
        return sum

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
        get_logger().debug('Current date: {} and stock: {}'.format(date, stock))
        data = data.reset_index()
        # 这里结束时间不能用时间点卡，因为很多情况二阶段成交会有延时，不是正好在09:25:00
        end_time_data = data[(data['time'] <= add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)) & (data['volume'] > 0)]
        end_time = end_time_data.iloc[0]['time']
        data['ask_commission_amount'] = 0
        data['bid_commission_amount'] = 0
        data.loc[(data['time'] >= add_milliseconds_suffix(STOCK_OPEN_CALL_AUACTION_2ND_STAGE_START_TIME)) & (data['time'] <= end_time), 'ask_commission_amount'] = data.apply(lambda item: self.amount_sum(item, 'ask'), axis=1)
        data.loc[(data['time'] >= add_milliseconds_suffix(STOCK_OPEN_CALL_AUACTION_2ND_STAGE_START_TIME)) & (data['time'] <= end_time), 'bid_commission_amount'] = data.apply(lambda item: self.amount_sum(item, 'bid'), axis=1)
        return data

class BidLargeAmountBillFactor(TimewindowStockTickFactor):
    """委买大额挂单因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_013_BID_LARGE_AMOUNT_BILL'
    version = '1.0'

    def __init__(self):
        TimewindowStockTickFactor.__init__(self)
        self._auxiliary_file_generation_factor = AuxiliaryFileGenerationFactor()
        # 初始化已加载数据，并按日求平均
        self._auxiliary_data = {
            'IC' : self._auxiliary_file_generation_factor.load('IC')[['date','5_grade_bid_amount']].groupby('date')['5_grade_bid_amount'].mean(),
            'IH' : self._auxiliary_file_generation_factor.load('IH')[['date','5_grade_bid_amount']].groupby('date')['5_grade_bid_amount'].mean(),
            'IF' : self._auxiliary_file_generation_factor.load('IF')[['date','5_grade_bid_amount']].groupby('date')['5_grade_bid_amount'].mean(),
        }

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['5_grade_bid_amount']
        return columns

    def get_factor_columns(self, data):
        columns = data.columns.tolist() + [self.get_key()]
        return columns

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    @timing
    def caculate(self, data):
        columns = self.get_factor_columns(data)
        future_instrument_config_dao = FutureInstrumentConfigDao()
        new_data = pd.DataFrame(columns=columns)
        product = data.iloc[0]['product']
        instrument = data.iloc[0]['instrument']
        date_list = list(set(data['date'].tolist()))
        date_list.sort()
        pagination = Pagination(date_list, page_size=20)
        while pagination.has_next():
            date_list = pagination.next()
            params_list = list(map(lambda date: [date, instrument, product], date_list))
            results = ProcessExcecutor(10).execute(self.caculate_by_date, params_list)
            temp_cache = {}
            for result in results:
                cur_date_data = self.merge_with_stock_data(data, result[0], result[1])
                temp_cache[result[0]] = cur_date_data
            for date in date_list:
                try:
                    cur_date_data = temp_cache[date]
                    auxiliary_data = self.get_auxiliary_data(product)
                    three_days_before_list = future_instrument_config_dao.get_last_n_transaction_date_list(date, 3)
                    five_grade_bid_amount_3days_mean = auxiliary_data[three_days_before_list].mean()
                    cur_date_data[self.get_key()] = 0
                    cur_date_data.loc[cur_date_data['5_grade_bid_amount_mean'] > five_grade_bid_amount_3days_mean * 1.5, self.get_key()] = 1
                except Exception as e:
                    get_logger().warning('The data is missing for date: {0}'.format(date))
                new_data = pd.concat([new_data, cur_date_data])
        return new_data

    def caculate_by_date(self, *args):
        date = args[0][0]
        instrument = args[0][1]
        product = args[0][2]
        get_logger().debug(f'Caculate by date params {date}, {instrument}, {product}')
        stock_data_per_date = self.get_stock_tick_data(product, instrument, date)
        if len(stock_data_per_date) == 0:
            get_logger().warning('The data on date: {0} and instrument: {1} is missing'.format(date, instrument))
            return date, stock_data_per_date
        stock_data_per_date = stock_data_per_date.rename(columns={'time': 'cur_time'})
        stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')['5_grade_bid_amount'].mean()
        df_stock_data_per_date = pd.DataFrame(
            {'5_grade_bid_amount_mean': stock_data_per_date_group_by,
             'time': stock_data_per_date_group_by.index})
        return date, df_stock_data_per_date

    def get_auxiliary_data(self, product):
        return self._auxiliary_data[product]


class AskLargeAmountBillFactor(TimewindowStockTickFactor):
    """委卖大额挂单因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_014_ASK_LARGE_AMOUNT_BILL'
    version = '1.0'

    def __init__(self):
        TimewindowStockTickFactor.__init__(self)
        self._auxiliary_file_generation_factor = AuxiliaryFileGenerationFactor()
        # 初始化已加载数据，并按日求平均
        self._auxiliary_data = {
            'IC': self._auxiliary_file_generation_factor.load('IC')[['date', '5_grade_ask_amount']].groupby('date')['5_grade_ask_amount'].mean(),
            'IH': self._auxiliary_file_generation_factor.load('IH')[['date', '5_grade_ask_amount']].groupby('date')['5_grade_ask_amount'].mean(),
            'IF': self._auxiliary_file_generation_factor.load('IF')[['date', '5_grade_ask_amount']].groupby('date')['5_grade_ask_amount'].mean(),
        }

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['5_grade_ask_amount']
        return columns

    def get_factor_columns(self, data):
        columns = data.columns.tolist() + [self.get_key()]
        return columns

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    @timing
    def caculate(self, data):
        columns = self.get_factor_columns(data)
        future_instrument_config_dao = FutureInstrumentConfigDao()
        new_data = pd.DataFrame(columns=columns)
        product = data.iloc[0]['product']
        instrument = data.iloc[0]['instrument']
        date_list = list(set(data['date'].tolist()))
        date_list.sort()
        pagination = Pagination(date_list, page_size=20)
        while pagination.has_next():
            date_list = pagination.next()
            params_list = list(map(lambda date: [date, instrument, product], date_list))
            results = ProcessExcecutor(10).execute(self.caculate_by_date, params_list)
            temp_cache = {}
            for result in results:
                cur_date_data = self.merge_with_stock_data(data, result[0], result[1])
                temp_cache[result[0]] = cur_date_data
            for date in date_list:
                try:
                    cur_date_data = temp_cache[date]
                    auxiliary_data = self.get_auxiliary_data(product)
                    three_days_before_list = future_instrument_config_dao.get_last_n_transaction_date_list(date, 3)
                    five_grade_ask_amount_3days_mean = auxiliary_data[three_days_before_list].mean()
                    cur_date_data[self.get_key()] = 0
                    cur_date_data.loc[cur_date_data['5_grade_ask_amount_mean'] > five_grade_ask_amount_3days_mean * 1.5, self.get_key()] = 1
                except Exception as e:
                    get_logger().warning('The data is missing for date: {0}'.format(date))
                new_data = pd.concat([new_data, cur_date_data])
        return new_data

    def caculate_by_date(self, *args):
        date = args[0][0]
        instrument = args[0][1]
        product = args[0][2]
        get_logger().debug(f'Caculate by date params {date}, {instrument}, {product}')
        stock_data_per_date = self.get_stock_tick_data(product, instrument, date)
        if len(stock_data_per_date) == 0:
            get_logger().warning('The data on date: {0} and instrument: {1} is missing'.format(date, instrument))
            return date, stock_data_per_date
        stock_data_per_date = stock_data_per_date.rename(columns={'time': 'cur_time'})
        stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')['5_grade_ask_amount'].mean()
        df_stock_data_per_date = pd.DataFrame(
            {'5_grade_ask_amount_mean': stock_data_per_date_group_by,
             'time': stock_data_per_date_group_by.index})
        return date, df_stock_data_per_date

    def get_auxiliary_data(self, product):
        return self._auxiliary_data[product]

class TotalCommissionRatioChangeRateFactor(StockTickFactor):
    """总委比因子变化率，今天和昨天同一时间的总委比因子比值

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_015_TOTAL_COMMISSION_RATIO_CHANGE_RATE'
    version = '1.0'

    def __init__(self):
        StockTickFactor.__init__(self)
        self._total_commission_ratio_factor = TotalCommissionRatioFactor()

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    @timing
    def caculate(self, data):
        data = self._total_commission_ratio_factor.caculate(data)
        data[self.get_key()] = data.apply(lambda item: self.caculate_change_rate(item, data), axis=1)
        return data

    def caculate_change_rate(self, item, data):
        date = item['date']
        time = item['time']
        total_commission_ratio = item[self._total_commission_ratio_factor.get_key()]
        date_list = list(set(data['date'].tolist()))
        date_list.sort()
        if date in date_list:
            index = date_list.index(date)
            if index - 1 >= 0:
                last_date = date_list[index - 1]
                total_commission_ratio_list = data[(data['date'] == last_date) & (data['time'] == time)][
                    self._total_commission_ratio_factor.get_key()].tolist()
                if len(total_commission_ratio_list) > 0:
                    last_total_commission_ratio = total_commission_ratio_list[0]
                    if last_total_commission_ratio != 0:
                        return total_commission_ratio/last_total_commission_ratio
                else:
                    get_logger().warning('Data missing for date: {0} and time: {1}'.format(last_date, time))
        return 0

    def caculate_by_date(self, *args):
        return

class AmountAndCommissionRatioFactor(StockTickFactor):
    """委比加成交额因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_016_AMOUNT_AND_COMMISSION_RATIO'
    version = '2.0'

    def __init__(self):
        StockTickFactor.__init__(self)

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['10_grade_bid_amount', '10_grade_ask_amount', 'amount', 'price', 'delta_price']
        return columns

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    def execute_caculation(self, date, stock_data_per_date):
        stock_data_per_date = stock_data_per_date[stock_data_per_date['time'] > add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)]
        stock_data_per_date['amount_and_ask_commission_amount'] = stock_data_per_date[self.get_key_by_type('ask')]
        stock_data_per_date['amount_and_bid_commission_amount'] = stock_data_per_date[self.get_key_by_type('bid')]
        stock_data_per_date.loc[stock_data_per_date['delta_price'] > 0, 'amount_and_bid_commission_amount'] = stock_data_per_date['amount_and_bid_commission_amount'] + stock_data_per_date['amount']
        stock_data_per_date.loc[stock_data_per_date['delta_price'] < 0, 'amount_and_ask_commission_amount'] = stock_data_per_date['amount_and_ask_commission_amount'] + stock_data_per_date['amount']
        stock_data_per_date['total_amount_and_commission_amount'] = stock_data_per_date['amount_and_ask_commission_amount'] + stock_data_per_date['amount_and_bid_commission_amount']
        stock_data_per_date = stock_data_per_date.rename(columns={'time': 'cur_time'})
        stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')['amount_and_bid_commission_amount', 'total_amount_and_commission_amount'].sum()
        stock_data_per_date_group_by[self.get_key()] = stock_data_per_date_group_by.apply(
            lambda x: 0 if x['total_amount_and_commission_amount'] == 0 else x['amount_and_bid_commission_amount'] / x[
                'total_amount_and_commission_amount'], axis=1)
        df_stock_data_per_date = pd.DataFrame(
            {self.get_key(): stock_data_per_date_group_by[self.get_key()], 'time': stock_data_per_date_group_by.index})
        return date, df_stock_data_per_date

    def get_stock_data(self, date, stock):
        """
        delta_price要在这里算，不然无法处理跨股票的情形
        Parameters
        ----------
        date
        stock

        Returns
        -------

        """
        temp_data = self._data_access.access(date, stock)
        temp_data['delta_price'] = temp_data['price'] - temp_data['price'].shift(1)
        temp_data.loc[temp_data[np.isnan(temp_data['delta_price'])].index, 'delta_price'] = 0
        return temp_data

    def get_key_by_type(self, type):
        return '10_grade_' + type + '_amount'


class RisingFallingAmountRatioFactor(StockTickFactor):
    """上涨下跌成交量比例因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_017_RISING_FALLING_AMOUNT_RATIO'
    version = '1.0'

    def __init__(self):
        StockTickFactor.__init__(self)

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['amount', 'price', 'delta_price']
        return columns

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    def execute_caculation(self, date, stock_data_per_date):
        stock_data_per_date = stock_data_per_date[stock_data_per_date['time'] > add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)]
        stock_data_per_date['rising_amount'] = 0
        stock_data_per_date['falling_amount'] = 0
        stock_data_per_date.loc[stock_data_per_date['delta_price'] > 0, 'rising_amount'] = stock_data_per_date['amount']
        stock_data_per_date.loc[stock_data_per_date['delta_price'] < 0, 'falling_amount'] = stock_data_per_date['amount']
        stock_data_per_date['total_amount'] = stock_data_per_date['amount']
        stock_data_per_date['diff_amount'] = stock_data_per_date['rising_amount'] - stock_data_per_date['falling_amount']
        stock_data_per_date = stock_data_per_date.rename(columns={'time': 'cur_time'})
        stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')['diff_amount', 'total_amount'].sum()
        stock_data_per_date_group_by[self.get_key()] = stock_data_per_date_group_by.apply(lambda x: 0 if x['total_amount'] == 0 else x['diff_amount'] / x['total_amount'], axis=1)
        df_stock_data_per_date = pd.DataFrame({self.get_key(): stock_data_per_date_group_by[self.get_key()], 'time': stock_data_per_date_group_by.index})
        return date, df_stock_data_per_date

    def get_stock_data(self, date, stock):
        """
        delta_price要在这里算，不然无法处理跨股票的情形
        Parameters
        ----------
        date
        stock

        Returns
        -------

        """
        data = self._data_access.access(date, stock)
        data['delta_price'] = data['price'] - data['price'].shift(1)
        data.loc[data[np.isnan(data['delta_price'])].index, 'delta_price'] = 0
        return data


class UntradedStockRatioFactor(StockTickFactor):
    """未成交股票占比因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_018_UNTRADED_STOCK_RATIO'
    version = '1.0'

    def __init__(self):
        StockTickFactor.__init__(self)

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['volume']
        return columns

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    def execute_caculation(self, date, stock_data_per_date):
        stock_data_per_date = stock_data_per_date[stock_data_per_date['time'] > add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)]
        stock_data_per_date['total_count'] = 1
        stock_data_per_date['untraded_count'] = 0
        stock_data_per_date.loc[stock_data_per_date['volume'] == 0, 'untraded_count'] = 1
        stock_data_per_date = stock_data_per_date.rename(columns={'time': 'cur_time'})
        stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')['untraded_count', 'total_count'].sum()
        stock_data_per_date_group_by[self.get_key()] = stock_data_per_date_group_by.apply(
            lambda x: 0 if x['total_count'] == 0 else x['untraded_count'] / x['total_count'], axis=1)
        df_stock_data_per_date = pd.DataFrame(
            {self.get_key(): stock_data_per_date_group_by[self.get_key()], 'time': stock_data_per_date_group_by.index})
        return date, df_stock_data_per_date


class DailyAccumulatedLargeOrderRatioFactor(StockTickFactor):
    """日累计大单占比占比因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_019_DAILY_ACCUMULATED_LARGE_ORDER_RATIO'
    version = '1.0'

    def __init__(self):
        StockTickFactor.__init__(self)

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['amount','transaction_number','price']
        return columns

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    def execute_caculation(self, date, stock_data_per_date):
        stock_data_per_date = stock_data_per_date[stock_data_per_date['time'] > add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)]
        stock_data_per_date = stock_data_per_date.rename(columns={'time': 'cur_time'})
        stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')['total_rising_amount_per_transaction', 'total_amount_per_transaction'].sum()
        stock_data_per_date_group_by[self.get_key()] = np.apply_along_axis(lambda item: local_divide(item[0], item[1]), axis=1, arr=stock_data_per_date_group_by.values)
        df_stock_data_per_date = pd.DataFrame({self.get_key(): stock_data_per_date_group_by[self.get_key()], 'time': stock_data_per_date_group_by.index})
        return date, df_stock_data_per_date


    def enrich_stock_data(self, instrument, date, stock, data):
        data = data.reset_index()
        data['delta_transaction_number'] = data['transaction_number'] - data['transaction_number'].shift(1)
        data.loc[data[pd.isnull(data['delta_transaction_number'])].index, 'delta_transaction_number'] = 0
        data['amount_per_transaction'] = np.apply_along_axis(lambda item: local_divide(item[4], item[7]), axis=1, arr=data.values)
        data['delta_price'] = data['price'] - data['price'].shift(1)
        data.loc[data[pd.isnull(data['delta_price'])].index, 'delta_price'] = 0
        data['rising_amount_per_transaction'] = 0
        data['falling_amount_per_transaction'] = 0
        data.loc[data['delta_price'] > 0, 'rising_amount_per_transaction'] = data['amount_per_transaction']
        data.loc[data['delta_price'] < 0, 'falling_amount_per_transaction'] = data['amount_per_transaction']
        # 日累加
        data['total_rising_amount_per_transaction'] = data['rising_amount_per_transaction'].cumsum()
        data['total_falling_amount_per_transaction'] = data['falling_amount_per_transaction'].cumsum()
        data['total_amount_per_transaction'] = data['total_rising_amount_per_transaction'] + data['total_falling_amount_per_transaction']
        data.to_csv('E:\\data\\temp\\' + stock + '.csv')
        print(data[data['time'] == '14:02:12.000'][['total_rising_amount_per_transaction','total_amount_per_transaction']])
        return data

    def test(self, a, b):
        c = 748880
        d = 50
        e = c/d
        print(type(e))
        print(e)
        return e

class RollingAccumulatedLargeOrderRatioFactor(StockTickFactor):
    """时间窗大单占比占比因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_020_ROLLING_ACCUMULATED_LARGE_ORDER_RATIO'
    version = '1.0'

    def __init__(self, params):
        StockTickFactor.__init__(self)
        self._params = params

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['amount','transaction_number','price']
        param_keys = []
        for param in self._params:
            param_keys.append('total_rising_amount_per_transaction.' + str(param))
            param_keys.append('total_falling_amount_per_transaction.' + str(param))
            param_keys.append('total_amount_per_transaction.' + str(param))
        columns = columns + param_keys
        return columns

    def execute_caculation(self, date, stock_data_per_date):
        stock_data_per_date = stock_data_per_date[stock_data_per_date['time'] > add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)]
        stock_data_per_date = stock_data_per_date.rename(columns={'time': 'cur_time'})
        for param in self._params:
            stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')[
                'total_rising_amount_per_transaction.' + str(param), 'total_amount_per_transaction.' + str(param)].sum()
            stock_data_per_date_group_by[self.get_key(param)] = stock_data_per_date_group_by.apply(
                lambda x: 0 if x['total_amount_per_transaction.' + str(param)] == 0 else x[
                                                                                             'total_rising_amount_per_transaction.' + str(
                                                                                                 param)] / x[
                                                                                             'total_amount_per_transaction.' + str(
                                                                                                 param)], axis=1)
            df_stock_data_per_date = pd.DataFrame(
                {self.get_key(param): stock_data_per_date_group_by[self.get_key(param)],
                 'time': stock_data_per_date_group_by.index})
        return date, df_stock_data_per_date

    def get_stock_data(self, date, stock):
        """
        所有日内要rolling计算都必须在这里实现
        Parameters
        ----------
        date
        stock

        Returns
        -------

        """
        temp_data = self._data_access.access(date, stock)
        temp_data['delta_transaction_number'] = temp_data['transaction_number'] - temp_data['transaction_number'].shift(1)
        temp_data['amount_per_transaction'] = temp_data.apply(lambda x: 0 if x['delta_transaction_number'] == 0 else x['amount'] / x['delta_transaction_number'], axis=1)
        temp_data['delta_price'] = temp_data['price'] - temp_data['price'].shift(1)
        temp_data.loc[temp_data[np.isnan(temp_data['delta_price'])].index, 'delta_price'] = 0
        temp_data['rising_amount_per_transaction'] = 0
        temp_data['falling_amount_per_transaction'] = 0
        temp_data.loc[temp_data['delta_price'] > 0, 'rising_amount_per_transaction'] = temp_data['amount_per_transaction']
        temp_data.loc[temp_data['delta_price'] < 0, 'falling_amount_per_transaction'] = temp_data['amount_per_transaction']
        # 日累加
        for param in self._params:
            temp_data['total_rising_amount_per_transaction.' + str(param)] = temp_data['rising_amount_per_transaction'].rolling(param).sum()
            temp_data['total_falling_amount_per_transaction.' + str(param)] = temp_data['falling_amount_per_transaction'].rolling(param).sum()
            temp_data['total_amount_per_transaction.' + str(param)] = temp_data['total_rising_amount_per_transaction.' + str(param)] + temp_data['total_falling_amount_per_transaction.' + str(param)]
        return temp_data

class DeltaTotalCommissionRatioFactor(StockTickFactor):
    """总委比一阶差分因子
    不跨天
    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_021_DELTA_TOTAL_COMMISSION_RATIO'
    version = '1.0'

    def __init__(self, params):
        StockTickFactor.__init__(self)
        self._params = params
        self._total_commission_ratio_factor = TotalCommissionRatioFactor()

    def caculate_by_date(self, *args):
        date, df_stock_data_per_date = self._total_commission_ratio_factor.caculate_by_date(*args)
        if len(df_stock_data_per_date) == 0:
            return date, df_stock_data_per_date
        for param in self._params:
            df_stock_data_per_date[self.get_key(param)] = df_stock_data_per_date[self._total_commission_ratio_factor.get_key()] \
                                                      - df_stock_data_per_date[self._total_commission_ratio_factor.get_key()].shift(param)
        return date, df_stock_data_per_date

class OverNightYieldFactor(StockTickFactor):
    """隔夜收益率
    直接从数据中获取close(昨收)和bid_price(今日集合竞价)，bid_price/close
    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_022_OVER_NIGHT_YIELD'
    version = '1.0'

    def __init__(self):
        StockTickFactor.__init__(self)

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['close', 'bid_price1', self.get_key()]
        return columns

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    def execute_caculation(self, date, stock_data_per_date):
        stock_data_per_date = stock_data_per_date.rename(columns={'time': 'cur_time'})
        stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')[self.get_key()].mean()
        df_stock_data_per_date = pd.DataFrame(
            {self.get_key(): stock_data_per_date_group_by, 'time': stock_data_per_date_group_by.index})
        return date, df_stock_data_per_date

    def get_stock_data(self, date, stock):
        data = self._data_access.access(date, stock)
        # 一阶段集合竞价
        temp_data = data[(data['time'] >= add_milliseconds_suffix(STOCK_START_TIME)) & (data['time'] < add_milliseconds_suffix(STOCK_OPEN_CALL_AUACTION_2ND_STAGE_START_TIME)) & (data['bid_price1'] != 0) & (data['close'] != 0)]
        if len(temp_data) == 0:
            get_logger().error('Invalid 1st stage call auction data for {0} and {1}'.format(date, stock))
        last_close = temp_data.iloc[0]['close']
        auction_price = temp_data.iloc[0]['bid_price1']
        data[self.get_key()] = auction_price / last_close
        return data

class AmountAnd1stGradeCommissionRatioFactor(AmountAndCommissionRatioFactor):
    """1档委比加成交额因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_023_AMOUNT_AND_1ST_COMMISSION_RATIO'
    version = '1.0'

    def __init__(self):
        StockTickFactor.__init__(self)

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['bid_price1', 'bid_volume1',
                             'ask_price1', 'ask_volume1', 'amount', 'price', 'delta_price']
        return columns

    def amount_sum(self, item, type):
        return item[type + '_price1'] * item[type + '_volume1']

class RisingLimitStockProportionFactor(StockTickFactor):
    """涨停比例因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """

    factor_code = 'FCT_02_024_RISING_LIMIT_STOCK_PROPORTION'
    version = '1.0'

    def __init__(self):
        StockTickFactor.__init__(self)

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['close', 'price', 'daily_return', 'limit_sign']
        return columns

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    def caculate_by_date(self, *args):
        date = args[0][0]
        instrument = args[0][1]
        product = args[0][2]
        get_logger().debug(f'Caculate by date params {date}, {instrument}, {product}')
        stock_data_per_date = self.get_stock_tick_data(product, instrument, date)
        if len(stock_data_per_date) == 0:
            get_logger().warning('The data on date: {0} and instrument: {1} is missing'.format(date, instrument))
            return date, stock_data_per_date
        return self.execute_caculation(date, product, stock_data_per_date)

    def execute_caculation(self, date, product, stock_data_per_date):
        stock_data_per_date = stock_data_per_date.rename(columns={'time': 'cur_time'})
        stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')['limit_sign'].sum()
        stock_data_per_date_group_by = stock_data_per_date_group_by / STOCK_INDEX_INFO[product]['STOCK_COUNT']
        df_stock_data_per_date = pd.DataFrame({self.get_key(): stock_data_per_date_group_by, 'time': stock_data_per_date_group_by.index})
        return date, df_stock_data_per_date

    def get_stock_data(self, date, stock):
        data = self._data_access.access(date, stock)
        data['daily_return'] = (data['price'] - data['close'])/data['close']
        data['limit_sign'] = data.apply(lambda item: self.check_limit(item, date, stock), axis=1)
        return data

    def check_limit(self, item, date, stock):
        if item['daily_return'] > 0 and approximately_equal_to(item['daily_return'], get_rising_falling_limit(date, stock)):
            return 1
        else:
            return 0

class FallingLimitStockProportionFactor(RisingLimitStockProportionFactor):
    """跌停比例因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """

    factor_code = 'FCT_02_025_FALLING_LIMIT_STOCK_PROPORTION'
    version = '1.0'

    def check_limit(self, item, date, stock):
        if item['daily_return'] < 0 and approximately_equal_to(item['daily_return'], get_rising_falling_limit(date, stock)):
            return 1
        else:
            return 0


class AmountBidTotalCommissionRatioFactor(StockTickFactor):
    """成交额总委买比例因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_026_AMOUNT_BID_TOTAL_COMMISSION_RATIO'
    version = '1.0'

    def __init__(self, params=[20, 50 ,100 ,200]):
        StockTickFactor.__init__(self)
        self._params = params

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['weighted_average_bid_price', 'total_bid_volume', 'amount']
        return columns

    def execute_caculation(self, date, stock_data_per_date):
        stock_data_per_date = stock_data_per_date[stock_data_per_date['time'] > add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)]
        stock_data_per_date = stock_data_per_date.rename(columns={'time': 'cur_time'})
        commission_amount_key = self.get_action_type() + '_commission_amount'
        # 股票维度求和
        stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')[commission_amount_key, 'amount'].sum()
        # 按时间点求比例
        stock_data_per_date_group_by['ratio'] = stock_data_per_date_group_by.apply(
            lambda x: 0 if x[commission_amount_key] == 0 else x['amount'] / x[commission_amount_key], axis=1)
        columns = []
        # 时间维度求平均
        for param in self.get_params():
            stock_data_per_date_group_by[self.get_key(param)] = stock_data_per_date_group_by['ratio'].rolling(param).mean()
            # 两种方式补0或者滑动时间窗
            # stock_data_per_date_group_by.loc[np.isnan(stock_data_per_date_group_by[self.get_key(param)]), self.get_key(param)] = 0
            filled_data_arr = np.zeros(len(stock_data_per_date_group_by.loc[np.isnan(stock_data_per_date_group_by[self.get_key(param)])]))
            temp_arr = stock_data_per_date_group_by.loc[np.isnan(stock_data_per_date_group_by[self.get_key(param)])]['ratio'].tolist()
            for i in range(len(temp_arr)):
                if i == 0:
                    filled_data_arr[i] = temp_arr[i]
                else:
                    filled_data_arr[i] = np.mean(temp_arr[:i + 1])
            stock_data_per_date_group_by.loc[np.isnan(stock_data_per_date_group_by[self.get_key(param)]), self.get_key(param)] = filled_data_arr
            columns = columns + [self.get_key(param)]
        df_stock_data_per_date = stock_data_per_date_group_by[columns]
        df_stock_data_per_date['time'] = stock_data_per_date_group_by.index
        # 过滤对齐在3秒线的数据
        return date, df_stock_data_per_date

    def enrich_stock_data(self, instrument, date, stock, data):
        data[self.get_action_type() + '_commission_amount'] = data.apply(lambda item: self.amount_sum(item), axis=1)
        return data

    def amount_sum(self, item):
        return item['total_' + self.get_action_type() + '_volume'] * item['weighted_average_' + self.get_action_type() + '_price']

    def get_action_type(self):
        return 'bid'

class AmountAskTotalCommissionRatioFactor(AmountBidTotalCommissionRatioFactor):
    """成交额总委卖比例因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_027_AMOUNT_ASK_TOTAL_COMMISSION_RATIO'
    version = '1.0'

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['weighted_average_ask_price', 'total_ask_volume', 'amount']
        return columns

    def get_action_type(self):
        return 'ask'


class AmountBid10GradeCommissionRatioFactor(AmountBidTotalCommissionRatioFactor):
    """成交额10档委买比例因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_028_AMOUNT_BID_10_GRADE_COMMISSION_RATIO'
    version = '1.0'

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['10_grade_bid_amount', 'amount']
        return columns

    def amount_sum(self, item):
        return item['10_grade_bid_amount']


class AmountAsk10GradeCommissionRatioFactor(AmountBid10GradeCommissionRatioFactor):
    """成交额10档委卖比例因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_029_AMOUNT_ASK_10_GRADE_COMMISSION_RATIO'
    version = '1.0'

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['10_grade_ask_amount', 'amount']
        return columns

    def amount_sum(self, item):
        return item['10_grade_ask_amount']

    def get_action_type(self):
        return 'ask'


class AmountBid5GradeCommissionRatioFactor(AmountBidTotalCommissionRatioFactor):
    """成交额5档委买比例因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_030_AMOUNT_BID_5_GRADE_COMMISSION_RATIO'
    version = '1.0'

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['5_grade_bid_amount', 'amount']
        return columns

    def amount_sum(self, item):
        return item['5_grade_bid_amount']


class AmountAsk5GradeCommissionRatioFactor(AmountBid5GradeCommissionRatioFactor):
    """成交额5档委卖比例因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_031_AMOUNT_ASK_5_GRADE_COMMISSION_RATIO'
    version = '1.0'

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['5_grade_ask_amount', 'amount']
        return columns

    def get_action_type(self):
        return 'ask'

    def amount_sum(self, item):
        return item['5_grade_ask_amount']

class TotalCommissionVolatilityRatioFactor(StockTickFactor):
    """总委买总委卖波动率比例因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_032_TOTAL_COMMISSION_VOLATILITY_RATIO'
    version = '1.0'

    def __init__(self, params=[20, 50 ,100 ,200]):
        StockTickFactor.__init__(self)
        self._params = params

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['weighted_average_ask_price', 'total_ask_volume', 'weighted_average_bid_price', 'total_bid_volume']
        return columns

    def execute_caculation(self, date, stock_data_per_date):
        stock_data_per_date = stock_data_per_date.rename(columns={'time': 'cur_time'})
        sum_columns = []
        for param in self.get_params():
            sum_columns = sum_columns + ['ask_commission_amount_std_' + str(param), 'bid_commission_amount_std_' + str(param)]
        stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')[sum_columns].sum()
        columns = []
        for param in self.get_params():
            stock_data_per_date_group_by[self.get_key(param)] = stock_data_per_date_group_by.apply(
                lambda x: 0 if x['ask_commission_amount_std_' + str(param)] == 0 else x['bid_commission_amount_std_' + str(param)] / x['ask_commission_amount_std_' + str(param)], axis=1)
            columns = columns + [self.get_key(param)]
        df_stock_data_per_date = stock_data_per_date_group_by[columns]
        df_stock_data_per_date['time'] = df_stock_data_per_date.index
        # 过滤对齐在3秒线的数据
        return date, df_stock_data_per_date

    def enrich_stock_data(self, instrument, date, stock, data):
        data['ask_commission_amount'] = data.apply(lambda item: self.amount_sum(item, 'ask'))
        data['bid_commission_amount'] = data.apply(lambda item: self.amount_sum(item, 'bid'))
        for param in self.get_params():
            data['ask_commission_amount_std_' + str(param)] = data['ask_commission_amount'].rolling(param).std()
            data['bid_commission_amount_std_' + str(param)] = data['bid_commission_amount'].rolling(param).std()
        return data

    def amount_sum(self, item, type):
        return item['total_' + type + '_volume'] * item['weighted_average_' + type + '_price']


class Commission10GradeVolatilityRatioFactor(TotalCommissionVolatilityRatioFactor):
    """10档委买委卖波动率比例因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_033_10_GRADE_COMMISSION_VOLATILITY_RATIO'
    version = '1.0'

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['10_grade_bid_amount', '10_grade_ask_amount']
        return columns

    def enrich_stock_data(self, instrument, date, stock, data):
        for param in self.get_params():
            data['ask_commission_amount_std_' + str(param)] = data['10_grade_ask_amount'].rolling(param).std()
            data['bid_commission_amount_std_' + str(param)] = data['10_grade_bid_amount'].rolling(param).std()
        return data

class Commission5GradeVolatilityRatioFactor(TotalCommissionVolatilityRatioFactor):
    """5档委买委卖波动率比例因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_034_5_GRADE_COMMISSION_VOLATILITY_RATIO'
    version = '1.0'

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['5_grade_bid_amount', '5_grade_ask_amount']
        return columns

    def enrich_stock_data(self, instrument, date, stock, data):
        for param in self.get_params():
            data['ask_commission_amount_std_' + str(param)] = data['5_grade_ask_amount'].rolling(param).std()
            data['bid_commission_amount_std_' + str(param)] = data['5_grade_bid_amount'].rolling(param).std()
        return data

class TotalCommissionRatioDifferenceFactor(StockTickFactor):
    """
    总委比差分因子
    """
    factor_code = 'FCT_02_035_TOTAL_COMMISSION_RATIO_DIFFERENCE'
    version = '1.0'

    def __init__(self, params):
        StockTickFactor.__init__(self)
        self._params = params
        self._total_commission_ratio_factor = TotalCommissionRatioFactor()

    @timing
    def caculate(self, data):
        """
        现货因子计算逻辑，多进程按天计算
        Parameters
        ----------
        data

        Returns
        -------

        """
        columns = self.get_factor_columns(data)
        new_data = pd.DataFrame(columns=columns)
        product = data.iloc[0]['product']
        original_data = self._total_commission_ratio_factor.load(product)
        date_list = list(set(data['date'].tolist()))
        date_list.sort()
        for date in date_list:
            temp_data = original_data[original_data['date'] == date]
            if len(temp_data) > 0: # 因为已经按主力合约交割日进行了截取，所以temp_data有可能是空的，必须做这个处理
                for param in self._params:
                    temp_data.loc[:, self.get_key(param)] = temp_data[self._total_commission_ratio_factor.get_key()] - temp_data[self._total_commission_ratio_factor.get_key()].shift(param)
                    if temp_data.dtypes[self.get_key(param)] != FACTOR_STANDARD_FIELD_TYPE:
                        temp_data[self.get_key(param)] = temp_data[self.get_key(param)].astype(FACTOR_STANDARD_FIELD_TYPE)
                    # 不足param长度的用第一个时间点计算
                    temp_data.loc[np.isnan(temp_data[self.get_key(param)]), self.get_key(param)] = temp_data[self._total_commission_ratio_factor.get_key()] - temp_data.iloc[0][self._total_commission_ratio_factor.get_key()]
                new_data = pd.concat([new_data, temp_data])
        return new_data

class TenGradeCommissionRatioDifferenceFactor(StockTickDifferenceFactor):
    """
    十档委比差分因子
    """
    factor_code = 'FCT_02_036_10_GRADE_COMMISSION_RATIO_DIFFERENCE'
    version = '1.0'

    def __init__(self, params):
        StockTickFactor.__init__(self)
        self._params = params
        self._10_grade_commission_ratio_factor = TenGradeCommissionRatioFactor()

    def get_target_factor(self):
        return self._10_grade_commission_ratio_factor

class FiveGradeCommissionRatioDifferenceFactor(StockTickDifferenceFactor):
    """
    五档委比差分因子
    """
    factor_code = 'FCT_02_037_5_GRADE_COMMISSION_RATIO_DIFFERENCE'
    version = '1.0'

    def __init__(self, params):
        StockTickFactor.__init__(self)
        self._params = params
        self._5_grade_commission_ratio_factor = FiveGradeCommissionRatioFactor()

    def get_target_factor(self):
        return self._5_grade_commission_ratio_factor

class DailyRisingStockRatioFactor(StockTickFactor):
    """
    当日上涨股票比例因子
    """
    factor_code = 'FCT_02_038_DAILY_RISING_STOCK_RATIO'
    version = '1.0'

    def __init__(self):
        StockTickFactor.__init__(self)

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['price', 'close']
        return columns

    def get_factor_columns(self, data):
        key_columns = []
        columns = data.columns.tolist() + key_columns + ['time', 'second_remainder']
        return columns


    @timing
    def execute_caculation(self, date, stock_data_per_date):
        stock_data_per_date = stock_data_per_date[
            stock_data_per_date['time'] > add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)]
        stock_data_per_date = stock_data_per_date.rename(columns={'time': 'cur_time'})
        if len(stock_data_per_date) == 0:
            return date, stock_data_per_date
        stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')[['change']].apply(
            lambda item: self.get_rise_ratio(item))
        stock_data_per_date_group_by['time'] = stock_data_per_date_group_by.index
        # 替换列名
        stock_data_per_date_group_by = stock_data_per_date_group_by.rename(columns={'change':self.get_key()})
        return date, stock_data_per_date_group_by

    def enrich_stock_data(self, instrument, date, stock, data):
        get_logger().debug('Current date: {} and stock: {}'.format(date, stock))
        data = data.reset_index(drop=True)
        temp_data = data[data['time'] >= add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)]
        # 盘前
        data['change'] = 0
        # 交易时间
        temp_data.loc[:, 'change'] = temp_data['price'] - temp_data['close']
        pre_data = data[data['time'] < add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)]
        data = pd.concat([pre_data, temp_data])
        return data

    def get_rise_ratio(self, item):
        # rise_ratio = item[item > 0].count() / item.count()
        rise_ratio = np.sum(item > 0) / len(item)
        return rise_ratio

class LargeOrderBidAskVolumeRatioFactor(TimewindowStockTickFactor):
    """
    大单买卖量比因子
    """
    factor_code = 'FCT_02_039_LARGE_ORDER_BID_ASK_VOLUME_RATIO'
    version = '1.0'

    def __init__(self):
        TimewindowStockTickFactor.__init__(self)

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['amount', 'transaction_number', 'price']
        return columns

    def get_factor_columns(self, data):
        columns = data.columns.tolist() + [self.get_key()]
        return columns

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    @timing
    # @profile
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
            results = ProcessExcecutor(3).execute(self.caculate_by_date, params_list)
            temp_cache = {}
            for result in results:
                cur_date_data = self.merge_with_stock_data(data, result[0], result[1])
                temp_cache[result[0]] = cur_date_data
            for date in date_list:
                cur_date_data = temp_cache[date]
                cur_date_data[self.get_key()] = 0
                cur_date_data.loc[cur_date_data['total_large_amount'] > 0, self.get_key()] = cur_date_data['rising_large_amount']/cur_date_data['total_large_amount']
                new_data = pd.concat([new_data, cur_date_data])
        return new_data

    def caculate_by_date(self, *args):
        date = args[0][0]
        instrument = args[0][1]
        product = args[0][2]
        get_logger().debug(f'Caculate by date params {date}, {instrument}, {product}')
        stock_data_per_date = self.get_stock_tick_data(product, instrument, date)
        if len(stock_data_per_date) == 0:
            get_logger().warning('The data on date: {0} and instrument: {1} is missing'.format(date, instrument))
            return date, stock_data_per_date
        stock_data_per_date = stock_data_per_date.rename(columns={'time': 'cur_time'})
        stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')['rising_large_amount', 'falling_large_amount'].sum()
        df_stock_data_per_date = pd.DataFrame(
            {'rising_large_amount': stock_data_per_date_group_by['rising_large_amount'],
             'falling_large_amount': stock_data_per_date_group_by['falling_large_amount'],
             'total_large_amount': stock_data_per_date_group_by['falling_large_amount'] + stock_data_per_date_group_by['rising_large_amount'],
             'time': stock_data_per_date_group_by.index})
        return date, df_stock_data_per_date

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
        # 计算时间区间，
        # 考虑：
        # 1. 因子是在哪个时间区间计算？所有时间区间：集合竞价和开盘之后
        # 2. 周期？3天
        get_logger().debug('Current date: {} and stock: {}'.format(date, stock))
        instrument_stock_data = self.prepare_timewindow_data(instrument)[stock]
        days_before = get_last_or_next_trading_date(stock, date, self.get_timewindow_size())
        instrument_stock_data = instrument_stock_data[(instrument_stock_data['date'] >= days_before) & (instrument_stock_data['date'] <= date)]
        total_amount = 0
        total_transaction = 0
        for dt in days_before:
            if dt != date:
                total_amount = total_amount + instrument_stock_data[instrument_stock_data['date'] == date].iloc[-1]['daily_amount']
                total_transaction = total_transaction + instrument_stock_data[instrument_stock_data['date'] == date].iloc[-1]['transaction_number']
        amount_per_transaction = total_amount/total_transaction
        data = instrument_stock_data[instrument_stock_data['date'] == date]
        data['amount_per_transaction'] = amount_per_transaction
        data['delta_price'] = data['price'] - data['price'].shift(1)
        data['delta_transaction_number'] = data['transaction_number'] - data['transaction_number'].shift(1)
        data['rising_large_amount'] = 0
        data['falling_large_amount'] = 0
        data.loc[(data['delta_price'] > 0) & (data['delta_transaction_number'] > 0) & (data['amount']/data['delta_transaction_number'] > 2 * data['amount_per_transaction']), 'rising_large_amount'] = data['amount']
        data.loc[(data['delta_price'] < 0) & (data['delta_transaction_number'] > 0) & (data['amount']/data['delta_transaction_number'] > 2 * data['amount_per_transaction']), 'falling_large_amount'] = data['amount']
        return data

    def get_timewindow_size(self):
        """
        获取时间窗大小
        Returns
        -------

        """
        return 4

class TenGradeCommissionRatioMeanFactor(StockTickMeanFactor):
    """
    十档委比均值因子
    """
    factor_code = 'FCT_02_040_10_GRADE_COMMISSION_RATIO_MEAN'
    version = '1.0'

    def __init__(self, params):
        StockTickFactor.__init__(self)
        self._params = params
        self._10_grade_commission_ratio_factor = TenGradeCommissionRatioFactor()

    def get_target_factor(self):
        return self._10_grade_commission_ratio_factor

class TenGradeCommissionRatioStdFactor(StockTickStdFactor):
    """
    十档委比均值因子
    """
    factor_code = 'FCT_02_041_10_GRADE_COMMISSION_RATIO_STD'
    version = '1.0'

    def __init__(self, params):
        StockTickFactor.__init__(self)
        self._params = params
        self._10_grade_commission_ratio_factor = TenGradeCommissionRatioFactor()

    def get_target_factor(self):
        return self._10_grade_commission_ratio_factor

class FiveGradeCommissionRatioMeanFactor(StockTickMeanFactor):
    """
    五档委比均值因子
    """
    factor_code = 'FCT_02_042_5_GRADE_COMMISSION_RATIO_MEAN'
    version = '1.0'

    def __init__(self, params):
        StockTickFactor.__init__(self)
        self._params = params
        self._5_grade_commission_ratio_factor = FiveGradeCommissionRatioFactor()

    def get_target_factor(self):
        return self._5_grade_commission_ratio_factor

class FiveGradeCommissionRatioStdFactor(StockTickStdFactor):
    """
    五档委比标准差因子
    """
    factor_code = 'FCT_02_043_5_GRADE_COMMISSION_RATIO_STD'
    version = '1.0'

    def __init__(self, params):
        StockTickFactor.__init__(self)
        self._params = params
        self._5_grade_commission_ratio_factor = FiveGradeCommissionRatioFactor()

    def get_target_factor(self):
        return self._5_grade_commission_ratio_factor

class TenGradeWeightedCommissionRatioDifferenceFactor(StockTickDifferenceFactor):
    """
    十档加权委比差分因子
    """
    factor_code = 'FCT_02_044_10_GRADE_WEIGHTED_COMMISSION_RATIO_DIFFERENCE'
    version = '1.0'

    def __init__(self, params):
        StockTickFactor.__init__(self)
        self._params = params
        self._10_grade_weighted_commission_ratio_factor = TenGradeWeightedCommissionRatioFactor()

    def get_target_factor(self):
        return self._10_grade_weighted_commission_ratio_factor

class TenGradeWeightedCommissionRatioMeanFactor(StockTickMeanFactor):
    """
    十档加权委比均值因子
    """
    factor_code = 'FCT_02_045_10_GRADE_WEIGHTED_COMMISSION_RATIO_MEAN'
    version = '1.0'

    def __init__(self, params):
        StockTickFactor.__init__(self)
        self._params = params
        self._10_grade_weighted_commission_ratio_factor = TenGradeWeightedCommissionRatioFactor()

    def get_target_factor(self):
        return self._10_grade_weighted_commission_ratio_factor

class TenGradeWeightedCommissionRatioStdFactor(StockTickStdFactor):
    """
    十档加权委比标准差因子
    """
    factor_code = 'FCT_02_046_10_GRADE_WEIGHTED_COMMISSION_RATIO_STD'
    version = '1.0'

    def __init__(self, params):
        StockTickFactor.__init__(self)
        self._params = params
        self._10_grade_weighted_commission_ratio_factor = TenGradeWeightedCommissionRatioFactor()

    def get_target_factor(self):
        return self._10_grade_weighted_commission_ratio_factor


class FiveGradeWeightedCommissionRatioDifferenceFactor(StockTickDifferenceFactor):
    """
    五档加权委比差分因子
    """
    factor_code = 'FCT_02_047_5_GRADE_WEIGHTED_COMMISSION_RATIO_DIFFERENCE'
    version = '1.0'

    def __init__(self, params):
        StockTickFactor.__init__(self)
        self._params = params
        self._5_grade_weighted_commission_ratio_factor = FiveGradeWeightedCommissionRatioFactor()

    def get_target_factor(self):
        return self._5_grade_weighted_commission_ratio_factor

class FiveGradeWeightedCommissionRatioMeanFactor(StockTickMeanFactor):
    """
    五档加权委比均值因子
    """
    factor_code = 'FCT_02_048_5_GRADE_WEIGHTED_COMMISSION_RATIO_MEAN'
    version = '1.0'

    def __init__(self, params):
        StockTickFactor.__init__(self)
        self._params = params
        self._5_grade_weighted_commission_ratio_factor = FiveGradeWeightedCommissionRatioFactor()

    def get_target_factor(self):
        return self._5_grade_weighted_commission_ratio_factor

class FiveGradeWeightedCommissionRatioStdFactor(StockTickStdFactor):
    """
    五档加权委比标准差因子
    """
    factor_code = 'FCT_02_049_5_GRADE_WEIGHTED_COMMISSION_RATIO_STD'
    version = '1.0'

    def __init__(self, params):
        StockTickFactor.__init__(self)
        self._params = params
        self._5_grade_weighted_commission_ratio_factor = FiveGradeWeightedCommissionRatioFactor()

    def get_target_factor(self):
        return self._5_grade_weighted_commission_ratio_factor


class AmountBidTotalCommissionRatioStdFactor(StockTickFactor):
    """成交额总委买比例标准差因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_050_AMOUNT_BID_TOTAL_COMMISSION_RATIO_STD'
    version = '1.0'

    def __init__(self, params=[20, 50 ,100 ,200]):
        StockTickFactor.__init__(self)
        self._params = params

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['weighted_average_bid_price', 'total_bid_volume', 'amount']
        return columns

    def execute_caculation(self, date, stock_data_per_date):
        stock_data_per_date = stock_data_per_date[stock_data_per_date['time'] > add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)]
        stock_data_per_date = stock_data_per_date.rename(columns={'time': 'cur_time'})
        commission_amount_key = self.get_action_type() + '_commission_amount'
        # 股票维度求和
        stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')[commission_amount_key, 'amount'].sum()
        # 按时间点求比例
        stock_data_per_date_group_by['ratio'] = stock_data_per_date_group_by.apply(
            lambda x: 0 if x[commission_amount_key] == 0 else x['amount'] / x[commission_amount_key], axis=1)
        columns = []
        # 时间维度求平均
        for param in self.get_params():
            stock_data_per_date_group_by[self.get_key(param)] = stock_data_per_date_group_by['ratio'].rolling(param).std()
            # 两种方式补0或者滑动时间窗
            # stock_data_per_date_group_by.loc[np.isnan(stock_data_per_date_group_by[self.get_key(param)]), self.get_key(param)] = 0
            filled_data_arr = np.zeros(len(stock_data_per_date_group_by.loc[np.isnan(stock_data_per_date_group_by[self.get_key(param)])]))
            temp_arr = stock_data_per_date_group_by.loc[np.isnan(stock_data_per_date_group_by[self.get_key(param)])]['ratio'].tolist()
            for i in range(len(temp_arr)):
                if i == 0:
                    filled_data_arr[i] = temp_arr[i]
                else:
                    filled_data_arr[i] = np.std(temp_arr[:i + 1])
            stock_data_per_date_group_by.loc[np.isnan(stock_data_per_date_group_by[self.get_key(param)]), self.get_key(param)] = filled_data_arr
            columns = columns + [self.get_key(param)]
        df_stock_data_per_date = stock_data_per_date_group_by[columns]
        df_stock_data_per_date['time'] = stock_data_per_date_group_by.index
        # 过滤对齐在3秒线的数据
        return date, df_stock_data_per_date

    def enrich_stock_data(self, instrument, date, stock, data):
        data[self.get_action_type() + '_commission_amount'] = data.apply(lambda item: self.amount_sum(item), axis=1)
        return data

    def amount_sum(self, item):
        return item['total_' + self.get_action_type() + '_volume'] * item['weighted_average_' + self.get_action_type() + '_price']

    def get_action_type(self):
        return 'bid'

class AmountBid10GradeCommissionRatioStdFactor(AmountBidTotalCommissionRatioStdFactor):
    """成交额10档委买比例标准差因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_051_AMOUNT_BID_10_GRADE_COMMISSION_RATIO_STD'
    version = '1.0'

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['10_grade_bid_amount', 'amount']
        return columns

    def amount_sum(self, item):
        return item['10_grade_bid_amount']

class AmountBid5GradeCommissionRatioStdFactor(AmountBidTotalCommissionRatioStdFactor):
    """成交额5档委买比例标准差因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_052_AMOUNT_BID_5_GRADE_COMMISSION_RATIO_STD'
    version = '1.0'

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['5_grade_bid_amount', 'amount']
        return columns

    def amount_sum(self, item):
        return item['5_grade_bid_amount']

class AmountAskTotalCommissionRatioStdFactor(AmountBidTotalCommissionRatioStdFactor):
    """成交额总委卖比例标准差因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_053_AMOUNT_ASK_TOTAL_COMMISSION_RATIO_STD'
    version = '1.0'

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['weighted_average_ask_price', 'total_ask_volume', 'amount']
        return columns

    def get_action_type(self):
        return 'ask'

class AmountAsk10GradeCommissionRatioStdFactor(AmountBid10GradeCommissionRatioStdFactor):
    """成交额10档委卖比例标准差因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_054_AMOUNT_ASK_10_GRADE_COMMISSION_RATIO_STD'
    version = '1.0'

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['10_grade_ask_amount', 'amount']
        return columns

    def amount_sum(self, item):
        return item['10_grade_ask_amount']

    def get_action_type(self):
        return 'ask'

class AmountAsk5GradeCommissionRatioStdFactor(AmountBid5GradeCommissionRatioStdFactor):
    """成交额5档委卖比例因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_055_AMOUNT_ASK_5_GRADE_COMMISSION_RATIO_STD'
    version = '1.0'

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['5_grade_ask_amount', 'amount']
        return columns

    def get_action_type(self):
        return 'ask'

    def amount_sum(self, item):
        return item['5_grade_ask_amount']

class AmountAndCommissionRatioMeanFactor(StockTickMeanFactor):
    """委比加成交额均值因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_056_AMOUNT_AND_COMMISSION_RATIO_MEAN'
    version = '1.0'

    def __init__(self, params):
        StockTickFactor.__init__(self)
        self._params = params
        self._amount_and_commission_ratio_factor = AmountAndCommissionRatioFactor()

    def get_target_factor(self):
        return self._amount_and_commission_ratio_factor


class AmountAndCommissionRatioStdFactor(StockTickStdFactor):
    """委比加成交额标准差因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_057_AMOUNT_AND_COMMISSION_RATIO_STD'
    version = '1.0'

    def __init__(self, params):
        StockTickFactor.__init__(self)
        self._params = params
        self._amount_and_commission_ratio_factor = AmountAndCommissionRatioFactor()

    def get_target_factor(self):
        return self._amount_and_commission_ratio_factor


class RisingFallingAmountRatioMeanFactor(StockTickMeanFactor):
    """上涨下跌成交量比例均值因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_058_RISING_FALLING_AMOUNT_RATIO_MEAN'
    version = '1.0'

    def __init__(self, params):
        StockTickFactor.__init__(self)
        self._params = params
        self._rising_falling_amount_ratio_factor = RisingFallingAmountRatioFactor()

    def get_target_factor(self):
        return self._rising_falling_amount_ratio_factor


class RisingFallingAmountRatioStdFactor(StockTickStdFactor):
    """上涨下跌成交量比例标准差因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_059_RISING_FALLING_AMOUNT_RATIO_STD'
    version = '1.0'

    def __init__(self, params):
        StockTickFactor.__init__(self)
        self._params = params
        self._rising_falling_amount_ratio_factor = RisingFallingAmountRatioFactor()

    def get_target_factor(self):
        return self._rising_falling_amount_ratio_factor

class AmountAnd1stCommissionRatioMeanFactor(StockTickMeanFactor):
    """1档委比加成交额均值因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_060_AMOUNT_AND_1ST_COMMISSION_RATIO_MEAN'
    version = '1.0'

    def __init__(self, params):
        StockTickFactor.__init__(self)
        self._params = params
        self._amount_and_1st_grade_commission_ratio_factor = AmountAnd1stGradeCommissionRatioFactor()

    def get_target_factor(self):
        return self._amount_and_1st_grade_commission_ratio_factor


class AmountAnd1stCommissionRatioStdFactor(StockTickStdFactor):
    """1档委比加成交额标准差因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_061_AMOUNT_AND_1ST_COMMISSION_RATIO_STD'
    version = '1.0'

    def __init__(self, params):
        StockTickFactor.__init__(self)
        self._params = params
        self._amount_and_1st_grade_commission_ratio_factor = AmountAnd1stGradeCommissionRatioFactor()

    def get_target_factor(self):
        return self._amount_and_1st_grade_commission_ratio_factor

class LargeOrderBidAskVolumeRatioFactor(TimewindowStockTickFactor):
    """
    大单买入比例因子
    计算单个股票三天总成交额除以三天总成交笔数，得到平均每笔成交额，大于平均值2倍以上为大单，均值-2倍之间为中单，0.5-均值之间为小单，小于0.5倍为小小单
    如果当前tick上涨且每笔成交额是大单，则算买入大单，如果下跌则卖出大单，分别计算当前tick点所有股票的买入大单卖出大单，最后计算（买入大单/买入大单+卖出大单）
    """
    factor_code = 'FCT_02_062_LARGE_ORDER_BUYING_RATIO'
    version = '1.0'

    def __init__(self):
        TimewindowStockTickFactor.__init__(self)

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['amount', 'daily_amount', 'transaction_number', 'price']
        return columns

    def get_factor_columns(self, data):
        columns = data.columns.tolist() + [self.get_key()]
        return columns

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    # @profile
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
            results = ProcessExcecutor(10).execute(self.caculate_by_date, params_list)
            temp_cache = {}
            for result in results:
                cur_date_data = self.merge_with_stock_data(data, result[0], result[1])
                temp_cache[result[0]] = cur_date_data
            for date in date_list:
                cur_date_data = temp_cache[date]
                new_data = pd.concat([new_data, cur_date_data])
        return new_data

    def caculate_by_date(self, *args):
        date = args[0][0]
        instrument = args[0][1]
        product = args[0][2]
        get_logger().debug(f'Caculate by date params {date}, {instrument}, {product}')
        stock_data_per_date = self.get_stock_tick_data(product, instrument, date)
        if len(stock_data_per_date) == 0:
            get_logger().warning('The data on date: {0} and instrument: {1} is missing'.format(date, instrument))
            return date, stock_data_per_date
        stock_data_per_date = stock_data_per_date.rename(columns={'time': 'cur_time'})
        # 求每个时刻上涨大单额和下跌大单额在股票维度的累加
        stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')['rising_large_amount', 'falling_large_amount'].sum()
        stock_data_per_date_group_by['total_large_amount'] = stock_data_per_date_group_by['falling_large_amount'] + stock_data_per_date_group_by['rising_large_amount']
        stock_data_per_date_group_by[self.get_key()] = np.apply_along_axis(lambda item: local_divide(item[0], item[2]), axis=1, arr=stock_data_per_date_group_by.values)
        df_stock_data_per_date = pd.DataFrame(
            {self.get_key(): stock_data_per_date_group_by[self.get_key()],
             'time': stock_data_per_date_group_by.index})
        return date, df_stock_data_per_date

    def enrich_stock_data(self, instrument, date, stock, data):
        """
        时间维度上处理股票数据
        对于每个股票维护一个小的滑动时间窗，大小为get_timewindow_size()，每向后处理一天，加载新的数据，删除老的数据

        Parameters
        ----------
        data

        Returns
        -------

        """
        get_logger().debug('Current date: {} and stock: {}'.format(date, stock))
        data = data.reset_index(drop=True)
        days_list = get_last_or_next_trading_date_list_by_stock(stock, date, self.get_timewindow_size())
        instrument_stock_data = self.prepare_timewindow_data(stock, days_list)
        if len(instrument_stock_data) > 0:
            total_amount = 0
            total_transaction = 0
            for dt in days_list:
                if len(instrument_stock_data[instrument_stock_data['date'] == dt]) > 0:
                    total_amount = total_amount + instrument_stock_data[instrument_stock_data['date'] == dt].iloc[-1]['daily_amount']
                    total_transaction = total_transaction + instrument_stock_data[instrument_stock_data['date'] == dt].iloc[-1]['transaction_number']
            amount_per_transaction = total_amount/total_transaction
            data['amount_per_transaction'] = amount_per_transaction
        else: # 没有之前的股票现货数据，取当天当前时刻之前的平均成交额
            data['amount_per_transaction'] = np.apply_along_axis(lambda item: local_divide(item[4], item[5]),  axis=1, arr=data.values)
        data['delta_price'] = data['price'] - data['price'].shift(1)
        data.loc[data[pd.isnull(data['delta_price'])].index, 'delta_price'] = 0
        data['delta_transaction_number'] = data['transaction_number'] - data['transaction_number'].shift(1)
        data.loc[data[pd.isnull(data['delta_transaction_number'])].index, 'delta_transaction_number'] = 0
        data['rising_large_amount'] = 0
        data['falling_large_amount'] = 0
        data['cur_amount_per_transaction'] = np.apply_along_axis(lambda item: local_divide(item[3], item[9]), axis=1, arr=data.values)
        data.loc[(data['delta_price'] > 0) & (data['cur_amount_per_transaction'] > 2 * data['amount_per_transaction']), 'rising_large_amount'] = data['amount']
        data.loc[(data['delta_price'] < 0) & (data['cur_amount_per_transaction'] > 2 * data['amount_per_transaction']), 'falling_large_amount'] = data['amount']
        return data


if __name__ == '__main__':
    data = read_decompress(TEST_PATH + 'IF1810.pkl')
    data['product'] = 'IF'
    data['instrument'] = 'IF1810'
    data['date'] = data['datetime'].str[0:10]
    #
    # print(data)
    # print(data.columns)
    #
    # #总委比因子
    # total_commision = TotalCommissionRatioFactor()
    # print(total_commision.factor_code)
    # print(total_commision.version)
    # print(total_commision.get_params())
    # print(total_commision.get_category())
    # print(total_commision.get_full_name())
    #
    # t = time.perf_counter()
    # data = TotalCommissionRatioFactor().caculate(data)
    # print(f'cost time: {time.perf_counter() - t:.8f} s')
    # save_compress(data, 'E:\\data\\test\\IF1803.daily.concurrent.20.10.refactor.pkl')
    # print(data[['datetime', TotalCommissionRatioFactor.factor_code]])
    # data.index = pd.DatetimeIndex(data['datetime'])
    # data = data[(data['datetime'] >= '2020-09-28 10:00:00') & (data['datetime'] <= '2020-09-28 10:30:00')]
    # draw_analysis_curve(data, show_signal=True, signal_keys=total_commision.get_keys())

    # 10档委比因子
    # ten_grade_total_commision = TenGradeCommissionRatioFactor()
    # print(ten_grade_total_commision.factor_code)
    # print(ten_grade_total_commision.version)
    # print(ten_grade_total_commision.get_params())
    # print(ten_grade_total_commision.get_category())
    # print(ten_grade_total_commision.get_full_name())
    #
    # t = time.perf_counter()
    # data = TenGradeCommissionRatioFactor().caculate(data)
    # print(f'cost time: {time.perf_counter() - t:.8f} s')
    # save_compress(data, 'E:\\data\\test\\IF1803.daily.concurrent.20.10.10_GRADE_COMMISSION_RATIO.pkl')
    # print(data[['datetime', TenGradeCommissionRatioFactor.factor_code]])
    # data.index = pd.DatetimeIndex(data['datetime'])
    # data = data[(data['datetime'] >= '2020-09-28 10:00:00') & (data['datetime'] <= '2020-09-28 10:30:00')]
    # draw_analysis_curve(data, show_signal=True, signal_keys=ten_grade_total_commision.get_keys())

    # 10档加权委比因子
    # ten_grade_weighted_total_commision = TenGradeWeightedCommissionRatioFactor()
    # print(ten_grade_weighted_total_commision.factor_code)
    # print(ten_grade_weighted_total_commision.version)
    # print(ten_grade_weighted_total_commision.get_params())
    # print(ten_grade_weighted_total_commision.get_category())
    # print(ten_grade_weighted_total_commision.get_full_name())
    #
    # data = TenGradeWeightedCommissionRatioFactor().caculate(data)
    # print(data[['datetime', TenGradeWeightedCommissionRatioFactor.factor_code]])
    # data.index = pd.DatetimeIndex(data['datetime'])
    # data = data[(data['datetime'] >= '2020-09-28 10:00:00') & (data['datetime'] <= '2020-09-28 10:30:00')]
    # draw_analysis_curve(data, show_signal=True, signal_keys=ten_grade_weighted_total_commision.get_keys())

    # 5档委比因子
    # five_grade_total_commision = FiveGradeCommissionRatioFactor()
    # print(five_grade_total_commision.factor_code)
    # print(five_grade_total_commision.version)
    # print(five_grade_total_commision.get_params())
    # print(five_grade_total_commision.get_category())
    # print(five_grade_total_commision.get_full_name())
    #
    # data = FiveGradeCommissionRatioFactor().caculate(data)
    # print(data[['datetime', FiveGradeCommissionRatioFactor.factor_code]])
    # data.index = pd.DatetimeIndex(data['datetime'])
    # data = data[(data['datetime'] >= '2020-09-28 10:00:00') & (data['datetime'] <= '2020-09-28 10:30:00')]
    # draw_analysis_curve(data, show_signal=True, signal_keys=five_grade_total_commision.get_keys())

    # 5档加权委比因子
    # five_grade_weighted_total_commision = FiveGradeWeightedCommissionRatioFactor()
    # print(five_grade_weighted_total_commision.factor_code)
    # print(five_grade_weighted_total_commision.version)
    # print(five_grade_weighted_total_commision.get_params())
    # print(five_grade_weighted_total_commision.get_category())
    # print(five_grade_weighted_total_commision.get_full_name())
    #
    # data = FiveGradeWeightedCommissionRatioFactor().caculate(data)
    # print(data[['datetime', FiveGradeWeightedCommissionRatioFactor.factor_code]])
    # data.index = pd.DatetimeIndex(data['datetime'])
    # data = data[(data['datetime'] >= '2020-09-28 10:00:00') & (data['datetime'] <= '2020-09-28 10:30:00')]
    # draw_analysis_curve(data, show_signal=True, signal_keys=five_grade_weighted_total_commision.get_keys())

    # 上涨股票比例因子
    # rising_stock_ratio_factor = RisingStockRatioFactor()
    # print(rising_stock_ratio_factor.factor_code)
    # print(rising_stock_ratio_factor.version)
    # print(rising_stock_ratio_factor.get_params())
    # print(rising_stock_ratio_factor.get_category())
    # print(rising_stock_ratio_factor.get_full_name())
    #
    # data = RisingStockRatioFactor().caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # data = data[(data['datetime'] >= '2020-09-28 10:00:00') & (data['datetime'] <= '2020-09-28 10:30:00')]
    # draw_analysis_curve(data, show_signal=True, signal_keys=rising_stock_ratio_factor.get_keys())

    # 价差因子
    # spread_factor = SpreadFactor()
    # print(spread_factor.factor_code)
    # print(spread_factor.version)
    # print(spread_factor.get_params())
    # print(spread_factor.get_category())
    # print(spread_factor.get_full_name())
    #
    # data = SpreadFactor().caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # data = data[(data['datetime'] >= '2020-09-28 10:00:00') & (data['datetime'] <= '2020-09-28 10:30:00')]
    # draw_analysis_curve(data, show_signal=True, signal_keys=spread_factor.get_keys())

    # 集合竞价二阶段涨幅因子
    # call_auction_second_stage_increase_factor = CallAuctionSecondStageIncreaseFactor()
    # print(call_auction_second_stage_increase_factor.factor_code)
    # print(call_auction_second_stage_increase_factor.version)
    # print(call_auction_second_stage_increase_factor.get_params())
    # print(call_auction_second_stage_increase_factor.get_category())
    # print(call_auction_second_stage_increase_factor.get_full_name())
    #
    # data = CallAuctionSecondStageIncreaseFactor().caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # data = data[(data['datetime'] >= '2020-09-28 10:00:00') & (data['datetime'] <= '2020-09-28 10:30:00')]
    # draw_analysis_curve(data, show_signal=True, signal_keys=call_auction_second_stage_increase_factor.get_keys())

    # 集合竞价二阶段涨幅减去集合竞价一阶段涨幅因子
    # two_call_auction_stage_difference_factor = TwoCallAuctionStageDifferenceFactor()
    # print(two_call_auction_stage_difference_factor.factor_code)
    # print(two_call_auction_stage_difference_factor.version)
    # print(two_call_auction_stage_difference_factor.get_params())
    # print(two_call_auction_stage_difference_factor.get_category())
    # print(two_call_auction_stage_difference_factor.get_full_name())
    #
    # data = TwoCallAuctionStageDifferenceFactor().caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # data = data[(data['datetime'] >= '2020-09-28 10:00:00') & (data['datetime'] <= '2020-09-28 10:30:00')]
    # draw_analysis_curve(data, show_signal=True, signal_keys=two_call_auction_stage_difference_factor.get_keys())

    # 集合竞价二阶段收益率波动率因子
    # call_auction_second_stage_return_volatility_factor = CallAuctionSecondStageReturnVolatilityFactor()
    # print(call_auction_second_stage_return_volatility_factor.factor_code)
    # print(call_auction_second_stage_return_volatility_factor.version)
    # print(call_auction_second_stage_return_volatility_factor.get_params())
    # print(call_auction_second_stage_return_volatility_factor.get_category())
    # print(call_auction_second_stage_return_volatility_factor.get_full_name())
    #
    # data = CallAuctionSecondStageReturnVolatilityFactor().caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # data = data[(data['datetime'] >= '2020-09-28 10:00:00') & (data['datetime'] <= '2020-09-28 10:30:00')]
    # draw_analysis_curve(data, show_signal=True, signal_keys=call_auction_second_stage_return_volatility_factor.get_keys())

    # 集合竞价一阶段委比因子
    # first_stage_comission_ratio_factor = FirstStageCommissionRatioFactor()
    # print(first_stage_comission_ratio_factor.factor_code)
    # print(first_stage_comission_ratio_factor.version)
    # print(first_stage_comission_ratio_factor.get_params())
    # print(first_stage_comission_ratio_factor.get_category())
    # print(first_stage_comission_ratio_factor.get_full_name())
    #
    # data = FirstStageCommissionRatioFactor().caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # data = data[(data['datetime'] >= '2020-09-28 10:00:00') & (data['datetime'] <= '2020-09-28 10:30:00')]
    # draw_analysis_curve(data, show_signal=True,
    #                     signal_keys=first_stage_comission_ratio_factor.get_keys())

    # 集合竞价二阶段委比因子
    # second_stage_comission_ratio_factor = SecondStageCommissionRatioFactor()
    # print(second_stage_comission_ratio_factor.factor_code)
    # print(second_stage_comission_ratio_factor.version)
    # print(second_stage_comission_ratio_factor.get_params())
    # print(second_stage_comission_ratio_factor.get_category())
    # print(second_stage_comission_ratio_factor.get_full_name())
    #
    # data = SecondStageCommissionRatioFactor().caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # data = data[(data['datetime'] >= '2020-09-28 10:00:00') & (data['datetime'] <= '2020-09-28 10:30:00')]
    # draw_analysis_curve(data, show_signal=True,
    #                     signal_keys=second_stage_comission_ratio_factor.get_keys())

    # 委买大额挂单因子
    # ask_large_amount_bill_factor = AskLargeAmountBillFactor()
    # print(ask_large_amount_bill_factor.factor_code)
    # print(ask_large_amount_bill_factor.version)
    # print(ask_large_amount_bill_factor.get_params())
    # print(ask_large_amount_bill_factor.get_category())
    # print(ask_large_amount_bill_factor.get_full_name())
    #
    # data = AskLargeAmountBillFactor().caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # data = data[(data['datetime'] >= '2020-09-28 10:00:00') & (data['datetime'] <= '2020-09-28 10:30:00')]
    # draw_analysis_curve(data, show_signal=True,
    #                     signal_keys=ask_large_amount_bill_factor.get_keys())

    # 委卖大额挂单因子
    # bid_large_amount_bill_factor = BidLargeAmountBillFactor()
    # print(bid_large_amount_bill_factor.factor_code)
    # print(bid_large_amount_bill_factor.version)
    # print(bid_large_amount_bill_factor.get_params())
    # print(bid_large_amount_bill_factor.get_category())
    # print(bid_large_amount_bill_factor.get_full_name())
    #
    # data = BidLargeAmountBillFactor().caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # data = data[(data['datetime'] >= '2020-09-28 10:00:00') & (data['datetime'] <= '2020-09-28 10:30:00')]
    # draw_analysis_curve(data, show_signal=True, signal_keys=bid_large_amount_bill_factor.get_keys())

    # 总委比因子变化率，今天和昨天同一时间的总委比因子比值
    # total_commission_ratio_change_rate_factor = TotalCommissionRatioChangeRateFactor()
    # print(total_commission_ratio_change_rate_factor.factor_code)
    # print(total_commission_ratio_change_rate_factor.version)
    # print(total_commission_ratio_change_rate_factor.get_params())
    # print(total_commission_ratio_change_rate_factor.get_category())
    # print(total_commission_ratio_change_rate_factor.get_full_name())
    #
    # data = TotalCommissionRatioChangeRateFactor().caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # data = data[(data['datetime'] >= '2020-09-28 10:00:00') & (data['datetime'] <= '2020-09-28 10:30:00')]
    # draw_analysis_curve(data, show_signal=True,
    #                     signal_keys=total_commission_ratio_change_rate_factor.get_keys())


    # data_access = StockDataAccess(check_original=False)
    # data = data_access.access('2017-12-18', '002311')
    # temp_data = data[(data['time'] <= STOCK_TRANSACTION_START_TIME) & (data['volume'] > 0)]
    # end_price = temp_data.iloc[0]['ask_price1']
    # print(end_price)

    factor = DailyAccumulatedLargeOrderRatioFactor()
    factor.set_stock_filter(['601678','002194'])
    data = factor.caculate_by_date(['2017-09-13','IC1709','IC'])
    data[1].to_csv('E:\\data\\temp\\IC1709_20170913.csv')




