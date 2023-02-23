#! /usr/bin/env python
# -*- coding:utf8 -*-
import pandas as pd
import time
from memory_profiler import profile
import numpy as np

from factor.base_factor import Factor, StockTickFactor, TimewindowStockTickFactor
from common.constants import TEST_PATH, STOCK_TRANSACTION_START_TIME, STOCK_OPEN_CALL_AUACTION_2ND_STAGE_START_TIME, \
    STOCK_OPEN_CALL_AUACTION_2ND_STAGE_END_TIME, STOCK_OPEN_CALL_AUACTION_1ST_STAGE_START_TIME, STOCK_INDEX_INFO
from common.localio import read_decompress, save_compress
from common.aop import timing
from common.visualization import draw_analysis_curve
from common.timeutils import get_last_or_next_trading_date, add_milliseconds_suffix
from framework.pagination import Pagination
from data.access import StockDataAccess
from framework.localconcurrent import ProcessRunner, ProcessExcecutor
from common.log import get_logger
from common.stockutils import approximately_equal_to, get_rising_falling_limit

"""现货类因子
分类编号：02
"""
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

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['weighted_average_bid_price', 'weighted_average_ask_price', 'total_bid_volume', 'total_ask_volume']
        return columns

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    def execute_caculation(self, date, stock_data_per_date):
        stock_data_per_date = stock_data_per_date[stock_data_per_date['time'] > add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)]
        stock_data_per_date['ask_commission_amount'] = stock_data_per_date['total_ask_volume'] * stock_data_per_date[
            'weighted_average_ask_price']
        stock_data_per_date['bid_commission_amount'] = stock_data_per_date['total_bid_volume'] * stock_data_per_date[
            'weighted_average_bid_price']
        stock_data_per_date['total_commission_amount'] = stock_data_per_date['ask_commission_amount'] + \
                                                         stock_data_per_date['bid_commission_amount']
        stock_data_per_date = stock_data_per_date.rename(columns={'time': 'cur_time'})
        stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')[
            'ask_commission_amount', 'total_commission_amount'].sum()
        stock_data_per_date_group_by[self.get_key()] = stock_data_per_date_group_by.apply(
            lambda x: 0 if x['total_commission_amount'] == 0 else x['ask_commission_amount'] / x[
                'total_commission_amount'], axis=1)
        df_stock_data_per_date = pd.DataFrame(
            {self.get_key(): stock_data_per_date_group_by[self.get_key()], 'time': stock_data_per_date_group_by.index})
        # 过滤对齐在3秒线的数据
        return date, df_stock_data_per_date


class TenGradeCommissionRatioFactor(StockTickFactor):
    """10档委比因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_002_10_GRADE_COMMISSION_RATIO'
    version = '1.0'

    def __init__(self):
        StockTickFactor.__init__(self)

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['bid_price1', 'bid_volume1', 'bid_price2', 'bid_volume2','bid_price3', 'bid_volume3','bid_price4', 'bid_volume4','bid_price5', 'bid_volume5','bid_price6', 'bid_volume6','bid_price7', 'bid_volume7','bid_price8', 'bid_volume8','bid_price9', 'bid_volume9','bid_price10', 'bid_volume10',
                             'ask_price1', 'ask_volume1', 'ask_price2', 'ask_volume2', 'ask_price3', 'ask_volume3','ask_price4', 'ask_volume4','ask_price5', 'ask_volume5','ask_price6', 'ask_volume6','ask_price7', 'ask_volume7','ask_price8', 'ask_volume8','ask_price9', 'ask_volume9','ask_price10', 'ask_volume10']
        return columns

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    def execute_caculation(self, date, stock_data_per_date):
        stock_data_per_date = stock_data_per_date[stock_data_per_date['time'] > add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)]
        stock_data_per_date = stock_data_per_date.rename(columns={'time': 'cur_time'})
        stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')[
            '10_grade_ask_commission_amount', '10_grade_total_commission_amount'].sum()
        stock_data_per_date_group_by[self.get_key()] = stock_data_per_date_group_by.apply(
            lambda x: 0 if x['10_grade_total_commission_amount'] == 0 else x['10_grade_ask_commission_amount'] / x[
                '10_grade_total_commission_amount'], axis=1)
        df_stock_data_per_date = pd.DataFrame(
            {self.get_key(): stock_data_per_date_group_by[self.get_key()], 'time': stock_data_per_date_group_by.index})
        return date, df_stock_data_per_date

    def amount_sum(self, item, type):
        sum = 0
        for i in range(1, 11):
            sum = sum + ((item[type + '_price' + str(i)]) * (item[type + '_volume' + str(i)]))
        return sum

    def enrich_stock_data(self, instrument, date, stock, data):
        data = data[data['time'] > add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)]
        data['10_grade_ask_commission_amount'] = data.apply(lambda item: self.amount_sum(item, 'ask'), axis=1)
        data['10_grade_bid_commission_amount'] = data.apply(lambda item: self.amount_sum(item, 'bid'), axis=1)
        data['10_grade_total_commission_amount'] = data['10_grade_ask_commission_amount'] + data['10_grade_bid_commission_amount']
        return data

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

    def amount_sum(self, item, type):
        weighted_list = [10 ,9 , 8 , 7 , 6 , 5, 4, 3 ,2 ,1]
        sum = 0
        for i in range(1, 11):
            sum = sum + ((item[type + '_price' + str(i)]) * (item[type + '_volume' + str(i)]) * weighted_list[i - 1])
        return sum

class FiveGradeCommissionRatioFactor(StockTickFactor):
    """5档委比因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_004_5_GRADE_COMMISSION_RATIO'
    version = '1.0'

    def __init__(self):
        StockTickFactor.__init__(self)

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['bid_price1', 'bid_volume1', 'bid_price2', 'bid_volume2','bid_price3', 'bid_volume3','bid_price4', 'bid_volume4','bid_price5', 'bid_volume5',
                             'ask_price1', 'ask_volume1', 'ask_price2', 'ask_volume2', 'ask_price3', 'ask_volume3','ask_price4', 'ask_volume4','ask_price5', 'ask_volume5']
        return columns

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    def execute_caculation(self, date, stock_data_per_date):
        stock_data_per_date = stock_data_per_date[stock_data_per_date['time'] > add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)]
        stock_data_per_date['5_grade_ask_commission_amount'] = stock_data_per_date.apply(
            lambda item: self.amount_sum(item, 'ask'), axis=1)
        stock_data_per_date['5_grade_bid_commission_amount'] = stock_data_per_date.apply(
            lambda item: self.amount_sum(item, 'bid'), axis=1)
        stock_data_per_date['5_grade_total_commission_amount'] = stock_data_per_date['5_grade_ask_commission_amount'] + \
                                                                 stock_data_per_date['5_grade_bid_commission_amount']
        stock_data_per_date = stock_data_per_date.rename(columns={'time': 'cur_time'})
        stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')[
            '5_grade_ask_commission_amount', '5_grade_total_commission_amount'].sum()
        stock_data_per_date_group_by[self.get_key()] = stock_data_per_date_group_by.apply(
            lambda x: 0 if x['5_grade_total_commission_amount'] == 0 else x['5_grade_ask_commission_amount'] / x[
                '5_grade_total_commission_amount'], axis=1)
        df_stock_data_per_date = pd.DataFrame(
            {self.get_key(): stock_data_per_date_group_by[self.get_key()], 'time': stock_data_per_date_group_by.index})
        return date, df_stock_data_per_date

    def amount_sum(self, item, type):
        sum = 0
        for i in range(1, 6):
            sum = sum + ((item[type + '_price' + str(i)]) * (item[type + '_volume' + str(i)]))
        return sum

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

    def amount_sum(self, item, type):
        weighted_list = [5, 4, 3 ,2 ,1]
        sum = 0
        for i in range(1, 6):
            sum = sum + ((item[type + '_price' + str(i)]) * (item[type + '_volume' + str(i)]) * weighted_list[i - 1])
        return sum


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
            temp_data['change.' + str(param)] = temp_data['price'] - temp_data['price'].shift(param)
            # 不足param长度的用昨日收盘价计算
            temp_data.loc[np.isnan(temp_data['change.' + str(param)]), 'change.' + str(param)] = temp_data['price'] - temp_data['close']
        pre_data = data[data['time'] < add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)]
        data = pd.concat([pre_data, temp_data])
        return data

    def get_rise_ratio(self, item):
        rise_ratio = item[item > 0].count() / item.count()
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
        stock_data_per_date[self.get_key()] = (stock_data_per_date['ask_price1'] - stock_data_per_date[
            'bid_price1']) / (stock_data_per_date['bid_price1'] + stock_data_per_date['ask_price1'])
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
            data.loc[data['date'] == date, self.get_key()] = total_ask_commission_amount/(total_ask_commission_amount + total_bid_commission_amount)
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
            data.loc[data['date'] == date, self.get_key()] = total_ask_commission_amount/(total_ask_commission_amount + total_bid_commission_amount)
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

class AskLargeAmountBillFactor(TimewindowStockTickFactor):
    """委买大额挂单因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_013_ASK_LARGE_AMOUNT_BILL'
    version = '1.0'

    def __init__(self):
        TimewindowStockTickFactor.__init__(self)

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['bid_price1', 'bid_volume1', 'bid_price2', 'bid_volume2','bid_price3', 'bid_volume3','bid_price4', 'bid_volume4','bid_price5', 'bid_volume5',
                             'ask_price1', 'ask_volume1', 'ask_price2', 'ask_volume2', 'ask_price3', 'ask_volume3','ask_price4', 'ask_volume4','ask_price5', 'ask_volume5']
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
                cur_date_data.loc[cur_date_data['ask_commission_amount'] > cur_date_data[
                    'mean_ask_commission_amount'] * 1.5, self.get_key()] = 1
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
        stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')[
            'mean_ask_commission_amount', 'ask_commission_amount'].mean()
        df_stock_data_per_date = pd.DataFrame(
            {'mean_ask_commission_amount': stock_data_per_date_group_by['mean_ask_commission_amount'],
             'ask_commission_amount': stock_data_per_date_group_by['ask_commission_amount'],
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
        #计算时间区间，
        # 考虑：
        # 1. 因子是在哪个时间区间计算？所有时间区间：集合竞价和开盘之后
        # 2. 周期？3天
        get_logger().debug('Current date: {} and stock: {}'.format(date, stock))
        instrument_stock_data = self.prepare_timewindow_data(instrument)[stock]
        three_days_before = get_last_or_next_trading_date(stock, date, 3)
        instrument_stock_data = instrument_stock_data[(instrument_stock_data['date'] >= three_days_before) & (instrument_stock_data['date'] <= date)]
        instrument_stock_data = instrument_stock_data[instrument_stock_data['time'] > add_milliseconds_suffix(STOCK_OPEN_CALL_AUACTION_2ND_STAGE_END_TIME)] #当天也只需要已成交的数据
        instrument_stock_data['ask_commission_amount'] = instrument_stock_data.apply(lambda item: self.amount_sum(item, 'ask'), axis=1)
        instrument_stock_data['mean_ask_commission_amount'] = instrument_stock_data['ask_commission_amount'].rolling(len(data) * 3).mean()
        data = instrument_stock_data[instrument_stock_data['date'] == date]
        return data

    def amount_sum(self, item, type):
        sum = 0
        for i in range(1, 6):
            sum = sum + ((item[type + '_price' + str(i)]) * (item[type + '_volume' + str(i)]))
        return sum


class BidLargeAmountBillFactor(TimewindowStockTickFactor):
    """委卖大额挂单因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_014_BID_LARGE_AMOUNT_BILL'
    version = '1.0'

    def __init__(self):
        TimewindowStockTickFactor.__init__(self)

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['bid_price1', 'bid_volume1', 'bid_price2', 'bid_volume2','bid_price3', 'bid_volume3','bid_price4', 'bid_volume4','bid_price5', 'bid_volume5',
                             'ask_price1', 'ask_volume1', 'ask_price2', 'ask_volume2', 'ask_price3', 'ask_volume3','ask_price4', 'ask_volume4','ask_price5', 'ask_volume5']
        return columns

    def get_factor_columns(self, data):
        columns = data.columns.tolist() + [self.get_key()]
        return columns

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    @timing
    @profile
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
                cur_date_data[self.get_key()] = 0
                cur_date_data.loc[cur_date_data['bid_commission_amount'] > cur_date_data[
                    'mean_bid_commission_amount'] * 1.5, self.get_key()] = 1
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
        stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')[
            'mean_bid_commission_amount', 'bid_commission_amount'].mean()
        df_stock_data_per_date = pd.DataFrame(
            {'mean_bid_commission_amount': stock_data_per_date_group_by['mean_bid_commission_amount'],
             'bid_commission_amount': stock_data_per_date_group_by['bid_commission_amount'],
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
        #计算时间区间，
        # 考虑：
        # 1. 因子是在哪个时间区间计算？所有时间区间：集合竞价和开盘之后
        # 2. 周期？3天
        get_logger().debug('Current date: {} and stock: {}'.format(date, stock))
        instrument_stock_data = self.prepare_timewindow_data(instrument)[stock]
        three_days_before = get_last_or_next_trading_date(stock, date, 3)
        instrument_stock_data = instrument_stock_data[(instrument_stock_data['date'] >= three_days_before) & (instrument_stock_data['date'] <= date)]
        instrument_stock_data = instrument_stock_data[instrument_stock_data['time'] > add_milliseconds_suffix(STOCK_OPEN_CALL_AUACTION_2ND_STAGE_END_TIME)] #当天也只需要已成交的数据
        instrument_stock_data['bid_commission_amount'] = instrument_stock_data.apply(lambda item: self.amount_sum(item, 'bid'), axis=1)
        instrument_stock_data['mean_bid_commission_amount'] = instrument_stock_data['bid_commission_amount'].rolling(len(data) * 3).mean()
        data = instrument_stock_data[instrument_stock_data['date'] == date]
        return data

    def amount_sum(self, item, type):
        sum = 0
        for i in range(1, 6):
            sum = sum + ((item[type + '_price' + str(i)]) * (item[type + '_volume' + str(i)]))
        return sum

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
    version = '1.0'

    def __init__(self):
        StockTickFactor.__init__(self)

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['bid_price1', 'bid_volume1', 'bid_price2', 'bid_volume2','bid_price3', 'bid_volume3','bid_price4', 'bid_volume4','bid_price5', 'bid_volume5','bid_price6', 'bid_volume6','bid_price7', 'bid_volume7','bid_price8', 'bid_volume8','bid_price9', 'bid_volume9','bid_price10', 'bid_volume10',
                             'ask_price1', 'ask_volume1', 'ask_price2', 'ask_volume2', 'ask_price3', 'ask_volume3','ask_price4', 'ask_volume4','ask_price5', 'ask_volume5','ask_price6', 'ask_volume6','ask_price7', 'ask_volume7','ask_price8', 'ask_volume8','ask_price9', 'ask_volume9','ask_price10', 'ask_volume10', 'amount', 'price', 'delta_price']
        return columns

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    def execute_caculation(self, date, stock_data_per_date):
        stock_data_per_date = stock_data_per_date[stock_data_per_date['time'] > add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)]
        stock_data_per_date['amount_and_ask_commission_amount'] = stock_data_per_date.apply(
            lambda item: self.amount_sum(item, 'ask'), axis=1)
        stock_data_per_date['amount_and_bid_commission_amount'] = stock_data_per_date.apply(
            lambda item: self.amount_sum(item, 'bid'), axis=1)
        stock_data_per_date.loc[stock_data_per_date['delta_price'] > 0, 'amount_and_ask_commission_amount'] = \
        stock_data_per_date['amount_and_ask_commission_amount'] + stock_data_per_date['amount']
        stock_data_per_date.loc[stock_data_per_date['delta_price'] < 0, 'amount_and_bid_commission_amount'] = \
        stock_data_per_date['amount_and_bid_commission_amount'] + stock_data_per_date['amount']
        stock_data_per_date['total_amount_and_commission_amount'] = stock_data_per_date['amount_and_ask_commission_amount'] + \
                                                                    stock_data_per_date['amount_and_bid_commission_amount']
        stock_data_per_date = stock_data_per_date.rename(columns={'time': 'cur_time'})
        stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')[
            'amount_and_ask_commission_amount', 'total_amount_and_commission_amount'].sum()
        stock_data_per_date_group_by[self.get_key()] = stock_data_per_date_group_by.apply(
            lambda x: 0 if x['total_amount_and_commission_amount'] == 0 else x['amount_and_ask_commission_amount'] / x[
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

    def amount_sum(self, item, type):
        sum = 0
        for i in range(1, 11):
            sum = sum + ((item[type + '_price' + str(i)]) * (item[type + '_volume' + str(i)]))
        return sum


class RisingFallingVolumeRatioFactor(StockTickFactor):
    """上涨下跌成交量比例因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_017_RISING_FALLING_VOLUME_RATIO'
    version = '1.0'

    def __init__(self):
        StockTickFactor.__init__(self)

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['volume', 'price', 'delta_price']
        return columns

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    def execute_caculation(self, date, stock_data_per_date):
        stock_data_per_date = stock_data_per_date[stock_data_per_date['time'] > add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)]
        stock_data_per_date['rising_volume'] = 0
        stock_data_per_date['falling_volume'] = 0
        stock_data_per_date.loc[stock_data_per_date['delta_price'] > 0, 'rising_volume'] = stock_data_per_date['volume']
        stock_data_per_date.loc[stock_data_per_date['delta_price'] < 0, 'falling_volume'] = stock_data_per_date[
            'volume']
        stock_data_per_date['total_volume'] = stock_data_per_date['volume']
        stock_data_per_date['diff_volume'] = stock_data_per_date['rising_volume'] - stock_data_per_date[
            'falling_volume']
        stock_data_per_date = stock_data_per_date.rename(columns={'time': 'cur_time'})
        stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')['diff_volume', 'total_volume'].sum()
        stock_data_per_date_group_by[self.get_key()] = stock_data_per_date_group_by.apply(
            lambda x: 0 if x['total_volume'] == 0 else x['diff_volume'] / x['total_volume'], axis=1)
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
        columns = columns + ['amount','transaction_number','price','delta_price','total_rising_amount_per_transaction','total_falling_amount_per_transaction','total_amount_per_transaction']
        return columns

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    def execute_caculation(self, date, stock_data_per_date):
        stock_data_per_date = stock_data_per_date[stock_data_per_date['time'] > add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)]
        stock_data_per_date = stock_data_per_date.rename(columns={'time': 'cur_time'})
        stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')[
            'total_rising_amount_per_transaction', 'total_amount_per_transaction'].sum()
        stock_data_per_date_group_by[self.get_key()] = stock_data_per_date_group_by.apply(
            lambda x: 0 if x['total_amount_per_transaction'] == 0 else x['total_rising_amount_per_transaction'] / x[
                'total_amount_per_transaction'], axis=1)
        df_stock_data_per_date = pd.DataFrame(
            {self.get_key(): stock_data_per_date_group_by[self.get_key()], 'time': stock_data_per_date_group_by.index})
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
        temp_data = temp_data.reset_index()
        temp_data['delta_transaction_number'] = temp_data['transaction_number'] - temp_data['transaction_number'].shift(1)
        temp_data.loc[temp_data[np.isnan(temp_data['delta_transaction_number'])].index, 'delta_transaction_number'] = 0
        temp_data['amount_per_transaction'] = temp_data.apply(lambda x: 0 if x['delta_transaction_number'] == 0 else x['amount'] / x['delta_transaction_number'], axis=1)
        temp_data['delta_price'] = temp_data['price'] - temp_data['price'].shift(1)
        temp_data.loc[temp_data[np.isnan(temp_data['delta_price'])].index, 'delta_price'] = 0
        temp_data['rising_amount_per_transaction'] = 0
        temp_data['falling_amount_per_transaction'] = 0
        temp_data.loc[temp_data['delta_price'] > 0, 'rising_amount_per_transaction'] = temp_data['amount_per_transaction']
        temp_data.loc[temp_data['delta_price'] < 0, 'falling_amount_per_transaction'] = temp_data['amount_per_transaction']
        # 日累加
        temp_data['total_rising_amount_per_transaction'] = temp_data['rising_amount_per_transaction'].cumsum()
        temp_data['total_falling_amount_per_transaction'] = temp_data['falling_amount_per_transaction'].cumsum()
        temp_data['total_amount_per_transaction'] = temp_data['total_rising_amount_per_transaction'] + temp_data['total_falling_amount_per_transaction']
        return temp_data

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
        columns = columns + ['close', 'price']
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
        stock_data_per_date_group_by[self.get_key()] = stock_data_per_date_group_by['limit_sign'] / \
                                                       STOCK_INDEX_INFO[product]['STOCK_COUNT']
        df_stock_data_per_date = pd.DataFrame(
            {self.get_key(): stock_data_per_date_group_by[self.get_key()], 'time': stock_data_per_date_group_by.index})
        return date, df_stock_data_per_date

    def get_stock_data(self, date, stock):
        data = self._data_access.access(date, stock)
        data['daily_return'] = (data['price'] - data['close'])/data['close']
        data['limit_sign'] = data.apply(lambda item: self.check_limit(item, date, stock))
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

if __name__ == '__main__':
    # data = read_decompress(TEST_PATH + 'IF1810.pkl')
    # data['product'] = 'IF'
    # data['instrument'] = 'IF1810'
    # data['date'] = data['datetime'].str[0:10]
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
    # draw_analysis_curve(data, show_signal=True,
    #                     signal_keys=bid_large_amount_bill_factor.get_keys())

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

    data_access = StockDataAccess(check_original=False)
    data = data_access.access('2017-12-18', '002311')
    temp_data = data[(data['time'] <= STOCK_TRANSACTION_START_TIME) & (data['volume'] > 0)]
    end_price = temp_data.iloc[0]['ask_price1']
    print(end_price)




