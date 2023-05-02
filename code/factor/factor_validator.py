#! /usr/bin/env python
# -*- coding:utf8 -*-
import os
import re
import time
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages
import uuid
import numpy as np
import heapq

from common.aop import timing
from common.constants import FUTURE_TICK_ORGANIZED_DATA_PATH, CONFIG_PATH, FACTOR_PATH, STOCK_INDEX_PRODUCTS, \
    REPORT_PATH, STOCK_TRANSACTION_START_TIME, STOCK_TRANSACTION_END_TIME, STOCK_TRANSACTION_NOON_END_TIME, STOCK_TRANSACTION_NOON_START_TIME
from common.exception.exception import InvalidStatus
from common.localio import read_decompress, list_files_in_path, save_compress
from common.persistence.dbutils import create_session
from common.persistence.po import FutureInstrumentConfig
from common.visualization import draw_line, join_two_images, draw_histogram
from factor.volume_price_factor import WilliamFactor, CloseMinusMovingAverageFactor
from factor.spot_goods_factor import TotalCommissionRatioFactor, TenGradeCommissionRatioFactor, AmountAndCommissionRatioFactor, FiveGradeCommissionRatioFactor, \
    TenGradeWeightedCommissionRatioFactor, FiveGradeCommissionRatioFactor, RisingFallingAmountRatioFactor, UntradedStockRatioFactor, DailyAccumulatedLargeOrderRatioFactor, \
    RollingAccumulatedLargeOrderRatioFactor, RisingStockRatioFactor, SpreadFactor, OverNightYieldFactor, DeltaTotalCommissionRatioFactor, TotalCommissionRatioDifferenceFactor, \
    TenGradeCommissionRatioDifferenceFactor, FiveGradeCommissionRatioDifferenceFactor, DailyRisingStockRatioFactor, FiveGradeCommissionRatioMeanFactor, FiveGradeCommissionRatioStdFactor,\
    TenGradeCommissionRatioStdFactor, TenGradeCommissionRatioMeanFactor, TenGradeWeightedCommissionRatioFactor,\
    AmountBid10GradeCommissionRatioFactor, AmountAsk10GradeCommissionRatioFactor, AmountBid5GradeCommissionRatioFactor, AmountAsk5GradeCommissionRatioFactor
from common.exception.exception import ValidationFailed
from data.access import StockDataAccess
from common.timeutils import add_milliseconds_suffix
from common.localio import FileWriter
from common.log import get_logger

class FactorAnalysisResult():
    """
    因子分析结果
    instruments: []
        instrument: {}
            dates: []
                date: {}
                    nan_count: int
                    pre_market_count: int
                    morning_market_count: int
                    afternoon_market_count: int
    """

    def __init__(self, factor, product):
        self._instruments = []
        self._factor = factor
        self._product = product

    def add_instrument(self, instrument):
        self._instruments.append(instrument)

    @timing
    def print(self):
        file_writer = FileWriter(FACTOR_PATH + os.path.sep + 'report' + os.path.sep + self._factor.get_full_name() + '_' + self._product)
        file_writer.write_file_line("Instruments")
        for instrument_info in self._instruments:
            file_writer.write_file_line("      " + instrument_info.get_instrument() + " : " + str(instrument_info.get_total_count()))
            for date_info in instrument_info.get_dates():
                file_writer.write_file_line("            " + date_info.get_date() + " : " + str(date_info.get_total_count()))
                file_writer.write_file_line("                  nan_count : " + str(date_info.get_nan_count()))
                file_writer.write_file_line("                  zero_count : " + str(date_info.get_zero_count()))
                file_writer.write_file_line("                  morning_market_count : " + str(date_info.get_morning_market_count()))
                file_writer.write_file_line("                  afternoon_market_count : " + str(date_info.get_afternoon_market_count()))
        file_writer.close_file()

class InstrumentInfo():
    def __init__(self, instrument):
        self._instrument = instrument
        self._dates = []

    def add_date(self, date):
        self._dates.append(date)

    def get_total_count(self):
        total_count = 0
        for date in self._dates:
            total_count = total_count + date.get_total_count()
        return total_count

    def get_instrument(self):
        return self._instrument

    def get_dates(self):
        return self._dates

class DateInfo():

    def __init__(self, date, nan_count, zero_count, morning_market_count, afternoon_market_count):
        self._date = date
        self._nan_count = nan_count
        self._zero_count = zero_count
        self._morning_market_count = morning_market_count
        self._afternoon_market_count = afternoon_market_count

    def get_total_count(self):
        return self._morning_market_count + self._afternoon_market_count

    def get_nan_count(self):
        return self._nan_count

    def get_zero_count(self):
        return self._zero_count

    def get_morning_market_count(self):
        return self._morning_market_count

    def get_afternoon_market_count(self):
        return self._afternoon_market_count

    def get_date(self):
        return self._date

class FactorValidator():
    """因子校验基类

    Parameters
    ----------
    """

    def __init__(self, validator_list = [], product_list = STOCK_INDEX_PRODUCTS):
        self._validator_list = validator_list
        self._product_list = product_list

    def set_product_list(self, product_list = STOCK_INDEX_PRODUCTS):
        self._product_list = product_list

    @timing
    def validate(self, factor_list):
        """
        在基类中组装因子校验链表，依此执行校验

        Parameters
        ----------
        factor_list

        Returns
        -------

        """
        if len(factor_list) == 0:
            raise InvalidStatus('Empty factor list')

        #遍历检测器，执行检查
        if len(self._validator_list) > 0:
            for validator in self._validator_list:
                validator.set_product_list(self._product_list)
                validator.validate(factor_list)

class BasicValidator(FactorValidator):
    """
    一些基本检查
    1. 生成分析报告：
    合约数，天数，记录数（盘前，早盘，午盘），为空或者0的记录数

    """

    def __init__(self):
        self._product_list = STOCK_INDEX_PRODUCTS

    @timing
    def validate(self, factor_list):
        error_factor = []
        for factor in factor_list:
            get_logger().info('Validate for factor {0}'.format(factor.get_full_name()))
            for product in self._product_list:
                get_logger().info('Validate for product {0}'.format(product))
                data = factor.load(product, True)
                factor_analysis_result = FactorAnalysisResult(factor, product)
                instruments = list(set(data['instrument'].tolist()))
                instruments.sort()
                for instrument in instruments:
                    get_logger().debug('Validate for instrument {0}'.format(instrument))
                    instrument_info = InstrumentInfo(instrument)
                    instrument_data = data[data['instrument'] == instrument]
                    dates = list(set(instrument_data['date'].tolist()))
                    dates.sort()
                    for date in dates:
                        get_logger().debug('Validate for date {0}'.format(date))
                        date_data = data[(data['instrument'] == instrument) & (data['date'] == date)]
                        if len(factor.get_params()) == 0:
                            nan_count = len(date_data[np.isnan(date_data[factor.get_key()])])
                            zero_count = len(date_data[date_data[factor.get_key()] == 0])
                        else:
                            nan_count = 0
                            zero_count = 0
                            for param in factor.get_params():
                                nan_count = nan_count + len(date_data[np.isnan(date_data[factor.get_key(param)])])
                                zero_count = zero_count + len(date_data[date_data[factor.get_key(param)] == 0])
                        # 量价因子文件没有time列，现货因子的time列是从股票数据带来的，所以对于量价因子需要特殊处理下
                        if 'time' not in date_data.columns:
                            date_data['time'] = date_data['datetime'].apply(lambda item: item[11:23])
                        morning_market_count = len(date_data[(date_data['time'] >= add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)) & (date_data['time'] <= add_milliseconds_suffix(STOCK_TRANSACTION_NOON_END_TIME))])
                        afternoon_market_count = len(date_data[(date_data['time'] >= add_milliseconds_suffix(STOCK_TRANSACTION_NOON_START_TIME)) & (date_data['time'] <= add_milliseconds_suffix(STOCK_TRANSACTION_END_TIME))])
                        date_info = DateInfo(date, nan_count, zero_count, morning_market_count, afternoon_market_count)
                        instrument_info.add_date(date_info)
                    factor_analysis_result.add_instrument(instrument_info)
                factor_analysis_result.print()

class StatisticsAnalysis(FactorValidator):

    def __init__(self):
        self._product_list = STOCK_INDEX_PRODUCTS

    """
    生成统计信息

    """
    @timing
    def validate(self, factor_list):
        factor_report_path = REPORT_PATH + 'factor' + os.path.sep + 'statistics'
        if not os.path.exists(factor_report_path):
            os.makedirs(factor_report_path)
        for factor in factor_list:
            if len(factor.get_params()) > 0:
                for param in factor.get_params():
                    self.create_statistics_info(factor, factor_report_path, factor.get_key(param))
            else:
                self.create_statistics_info(factor, factor_report_path, factor.get_key())

    def create_statistics_info(self, factor, factor_report_path, key):
        """
        生成统计信息
        绘制分布图

        Parameters
        ----------
        factor
        factor_report_path
        key

        Returns
        -------

        """
        #生成统计信息
        self.statistic_info(factor, factor_report_path, key)
        #绘制分布图
        self.distribution_histogram(factor, factor_report_path, key)

    def distribution_histogram(self, factor, factor_report_path, key):
        for product in self._product_list:
            data = factor.load(product, is_organized=True)
            factor_values = data[key]
            histogram_path = factor_report_path + os.path.sep + key + '_distribution.png'
            draw_histogram(factor_values, 1000, save_path=histogram_path)

    def statistic_info(self, factor, factor_report_path, key):
        columns = ['count', 'mean', 'std', 'min', 'max', '1%', '5%', '10%', '50%', '90%', '95%', '99%', 'abs_mean',
                   'ic']
        content = []
        index = []
        for product in self._product_list:
            data = factor.load(product, is_organized=True)
            count = len(data)
            mean = data[key].mean()
            std = data[key].std()
            min = data[key].min()
            max = data[key].max()
            quantile1 = data[key].quantile(0.01)
            quantile5 = data[key].quantile(0.05)
            quantile10 = data[key].quantile(0.1)
            quantile50 = data[key].quantile(0.5)
            quantile90 = data[key].quantile(0.9)
            quantile95 = data[key].quantile(0.95)
            quantile99 = data[key].quantile(0.99)
            abs_mean = ''
            ic = ''
            content.append(
                [count, mean, std, min, max, quantile1, quantile5, quantile10, quantile50, quantile90, quantile95,
                 quantile99, abs_mean, ic])
            index = index + [product]
        report = pd.DataFrame(content, columns=columns, index=index)
        report.to_csv(factor_report_path + os.path.sep + key + '_report.csv')


class SingleFactorLoopbackAnalysis(FactorValidator):
    """
    单因子区间回测

    """
    @timing
    def validate(self, factor_list):
        # 划分区间
        return


class StabilityValidator(FactorValidator):
    """因子稳定性检查类
        按天计算因子值的均值和标准差
    """

    def __init__(self):
        self._product_list = STOCK_INDEX_PRODUCTS

    @timing
    def validate(self, factor_list):
        factor_diagram_path = REPORT_PATH + 'factor'
        if not os.path.exists(factor_diagram_path):
            os.makedirs(factor_diagram_path)
        for factor in factor_list:
            get_logger().info('Start to check the stability for factor: {}'.format(factor.get_full_name()))
            if len(factor.get_params()) > 0:
                for param in factor.get_params():
                    for product in self._product_list:
                        self.create_diagram(factor, factor_diagram_path, product, factor.get_key(param))
            else:
                for product in self._product_list:
                    self.create_diagram(factor, factor_diagram_path, product, factor.get_key())

    def create_diagram(self, factor, factor_diagram_path, product, key):
        """
        生成并保存均值，标准差图
        Parameters
        ----------
        factor
        factor_diagram_path
        product
        key

        Returns
        -------

        """
        data = factor.load(product, is_organized=True)
        mean = data.groupby('date')[key].mean()
        std = data.groupby('date')[key].std()
        groupby_data = pd.DataFrame({
            'mean': mean,
            'std': std
        })
        groupby_data['date'] = groupby_data.index
        mean_path = factor_diagram_path + os.path.sep + product + '_' + key + '_mean.png'
        std_path = factor_diagram_path + os.path.sep + product + '_' + key + '_std.png'
        final_path = factor_diagram_path + os.path.sep + product + '_' + key + '.png'
        draw_line(groupby_data, key, 'Date', 'Mean',
                  {'x': 'date', 'y': [{'key': 'mean', 'label': 'Mean'}]}, save_path=mean_path)
        draw_line(groupby_data, key, 'Date', 'Std',
                  {'x': 'date', 'y': [{'key': 'std', 'label': 'Std'}]}, save_path=std_path)
        if os.path.exists(final_path):
            os.remove(final_path)
        join_two_images(mean_path, std_path, final_path, flag='vertical')
        os.remove(mean_path)
        os.remove(std_path)

class SingleFactorBackTestValidator(FactorValidator):
    """
    单因子回测检查

    """

    def __init__(self, group_number = 10, level = -1):
        self._product_list = STOCK_INDEX_PRODUCTS
        self._group_number = group_number
        self._ret_key = 'ret.30'
        self._level = level
        # 计算分割点的分位数
        self._split_quantile = []
        for i in range(0, self._group_number + 1):
            self._split_quantile = self._split_quantile + [50 + i * 50/self._group_number]

    @timing
    def validate(self, factor_list):
        for factor in factor_list:
            get_logger().info('Validate for factor {0}'.format(factor.get_full_name()))
            for product in self._product_list:
                get_logger().info('Validate for product {0}'.format(product))
                original_data = factor.load(product, True)
                original_data = original_data.dropna()
                if len(factor.get_params()) > 0:
                    for param in factor.get_params():
                        data = original_data.copy()
                        factor_value_list = data[factor.get_key(param)].tolist()
                        split_points = np.percentile(factor_value_list, self._split_quantile)
                        self.simulate(factor, data, product, split_points, factor.get_key(param))
                else:
                    data = original_data.copy()
                    factor_value_list = data[factor.get_key()].tolist()
                    split_points = np.percentile(factor_value_list, self._split_quantile)
                    self.simulate(factor, data, product, split_points, factor.get_key())
    class SimulationResult():

        _columns = ['product','index','key','trade_count','long_trade_count','short_trade_count','win_trade_count','lose_trade_count','win_trade_amount','lose_trade_amount','long_win_trade_amount','long_lose_trade_amount','short_win_trade_amount','short_lose_trade_amount']
        def __init__(self):
            self._data = pd.DataFrame(columns = self._columns)

        def set_property(self, product, index, key, trade_count, long_trade_count, short_trade_count, win_trade_count, loss_trade_count, win_trade_amount, loss_trade_amount,
                           long_win_trade_amount, long_lose_trade_amount, short_win_trade_amount, short_lose_trade_amount):
            self._data = pd.concat([self._data,  pd.DataFrame({ 'product' : [product],
            'index' : [index],
            'key' : [key],
            'trade_count' : [trade_count],
            'long_trade_count' : [long_trade_count],
            'short_trade_count' : [short_trade_count],
            'win_trade_count' : [win_trade_count],
            'lose_trade_count' : [loss_trade_count],
            'win_trade_amount' : [win_trade_amount],
            'lose_trade_amount' : [loss_trade_amount],
            'long_win_trade_amount' : [long_win_trade_amount],
            'long_lose_trade_amount' : [long_lose_trade_amount],
            'short_win_trade_amount' : [short_win_trade_amount],
            'short_lose_trade_amount' : [short_lose_trade_amount]}, columns = self._columns)])


        def print(self, product, key):
            for index, row in self._data.iterrows():
                try:
                    result_info = '品种：' + row['product'] + ' 分位档：' + str(row['index']) + ' 参数：' + row['key'] + \
                                  '\n总交易次数：' + str(row['trade_count']) + '\n做多：' + str(row['long_trade_count']) + \
                                  '\n做空：' + str(row['short_trade_count']) + '\n胜率：' + str(row['win_trade_count'] / row['trade_count']) + \
                                  '\n盈利因子：' + str(row['win_trade_amount'] / row['lose_trade_amount']) + '\n多头盈利因子：' + str(row['long_win_trade_amount'] / row['long_lose_trade_amount']) + \
                                  '\n空头盈利因子：' + str(row['short_win_trade_amount'] / row['short_lose_trade_amount'])
                    print('--------------------------------------------------')
                    print(result_info)
                except Exception as e:
                    print(e)
                              # '\n平均每笔盈利：' + str(all_industry_total_profit[-1] / row.trade_count)
            self._data.to_csv(REPORT_PATH + os.path.sep + 'factor' + os.path.sep + 'simulation' + os.path.sep + product +  '_' + key + '.csv')

    def simulate(self, factor, data, product, split_points, key):
        simulation_result = SingleFactorBackTestValidator.SimulationResult()
        for index in range(1, len(split_points)):
            if self._level > 0 and index != self._level:
                continue
            data['action'] = 0
            data['profit'] = 0
            if index == 1:
                long_threshold_high = split_points[index]
                long_threshold_low = split_points[0]
                short_threshold_high = split_points[0]
                short_threshold_low = split_points[0] - (split_points[index] - split_points[0])
                data.loc[(data[key] > long_threshold_low) & (data[key] < long_threshold_high), 'action'] = 1
                data.loc[(data[key] > short_threshold_low) & (data[key] < short_threshold_high), 'action'] = -1
            else:
                long_threshold_high = split_points[index]
                long_threshold_low = split_points[index - 1]
                short_threshold_high = split_points[0] - (split_points[index - 1] - split_points[0])
                short_threshold_low = split_points[0] - (split_points[index] - split_points[0])
                data.loc[(data[key] > long_threshold_low) & (data[key] < long_threshold_high), 'action'] = 1
                data.loc[(data[key] > short_threshold_low) & (data[key] < short_threshold_high), 'action'] = -1
            get_logger().info('Get distribution for {0} and long high value: {1}, long low value: {2}, short high value: {3}, short low value: {4}'
                              .format(str(index), str(long_threshold_high), str(long_threshold_low), str(short_threshold_high), str(short_threshold_low)))
            #考虑行为延迟
            data[['action_tick_open','action_tick_close','action_tick_ret']] = data[['open','close',self._ret_key]].shift(-factor.get_action_delay())
            data.loc[np.isnan(data['action_tick_open']), 'action_tick_open'] = data['open']
            data.loc[np.isnan(data['action_tick_close']), 'action_tick_close'] = data['close']
            data.loc[np.isnan(data['action_tick_ret']), 'action_tick_ret'] = data[self._ret_key]
            data.loc[data['action'] != 0, 'profit'] = data.apply(lambda item: self.caculate_profit(item), axis=1)
            trade_count = len(data[data['action'] != 0])
            long_trade_count = len(data[data['action'] == 1])
            short_trade_count = len(data[data['action'] == -1])
            win_trade_count = len(data[data['profit'] > 0])
            loss_trade_count = len(data[data['profit'] < 0])
            win_trade_amount = data[(data['profit'] > 0)]['profit'].sum()
            loss_trade_amount = abs(data[data['profit'] < 0]['profit'].sum())
            long_win_trade_amount = data[(data['profit'] > 0) & (data['action'] == 1)]['profit'].sum()
            long_lose_trade_amount = abs(data[(data['profit'] < 0) & (data['action'] == 1)]['profit'].sum())
            short_win_trade_amount = data[(data['profit'] > 0) & (data['action'] == -1)]['profit'].sum()
            short_lose_trade_amount = abs(data[(data['profit'] < 0) & (data['action'] == -1)]['profit'].sum())
            simulation_result.set_property(product, index, key, trade_count, long_trade_count, short_trade_count, win_trade_count, loss_trade_count,
                                win_trade_amount, loss_trade_amount, long_win_trade_amount, long_lose_trade_amount,
                                                                               short_win_trade_amount, short_lose_trade_amount)
            data['date'] = data['datetime'].apply(lambda x:x[0:10])
            data_group_by_date = pd.DataFrame({'profit' : data.groupby('date')['profit'].sum()})
            data_group_by_date['total_profit'] = data_group_by_date['profit'].cumsum()
            # 用于定位某些收益曲线陡增或陡降的问题
            # top_profit_delta = heapq.nsmallest(5, data_group_by_date['profit'].tolist())
            # for val in top_profit_delta:
            #     print(data_group_by_date[data_group_by_date['profit']==val])
            data_group_by_date['date'] = data_group_by_date.index
            draw_line(data_group_by_date, xlabel='date', ylabel='total_profit', plot_info={'x': 'date', 'y': [{'key': 'total_profit', 'label': 'total_profit'}]},
                      save_path = REPORT_PATH + os.path.sep + 'factor' + os.path.sep + 'simulation' + os.path.sep + product + '_' +  key + '_' + str(index) + '.png')
        simulation_result.print(product, key)

    def caculate_profit(self, item):
        rate = item['action_tick_ret']
        profit = item['action_tick_open'] * rate
        if item['action'] == -1:
            return -profit
        elif item['action'] == 1:
            return profit
        else:
            return 0


def parse_factor_file(product, factor_list, start_date, end_date='', is_organized=True):
    """
    分析片段
    Parameters
    ----------
    product
    factor_list
    start_date
    end_date

    Returns
    -------

    """
    if is_organized:
        path = FACTOR_PATH + os.path.sep + 'organized' + os.path.sep + product + '_' + '_'.join(list(map(lambda factor: factor.get_full_name(), factor_list)))
    else:
        path = FACTOR_PATH + os.path.sep + product + '_' + '_'.join(list(map(lambda factor: factor.get_full_name(), factor_list)))
    data = read_decompress(path)
    if end_date == '':
        data = data[data['date'] == start_date]
    else:
        data = data[(data['date'] >= start_date) & (data['date'] <= end_date)]
    for factor in factor_list:
        if is_organized:
            data.to_csv(FACTOR_PATH + os.path.sep + 'validation' + os.path.sep + product + '_' + factor.get_full_name() + '_organized_' + str(uuid.uuid4()) + '.csv')
        else:
            data.to_csv(FACTOR_PATH + os.path.sep + 'validation' + os.path.sep + product + '_' + factor.get_full_name() + '_' + str(uuid.uuid4()) + '.csv')

def parse_spot_goods_stock_data(product, date, start_time, end_time=''):
    """
    分析现货因子对应的现货信息
    Parameters
    ----------
    product
    date
    start_time
    end_time

    Returns
    -------

    """
    data = pd.DataFrame()
    if product == 'IH':
        stock_abstract = pd.read_pickle(CONFIG_PATH + os.path.sep + '50_stocks_abstract.pkl')
    elif product == 'IF':
        stock_abstract = pd.read_pickle(CONFIG_PATH + os.path.sep + '300_stocks_abstract.pkl')
    else:
        stock_abstract = pd.read_pickle(CONFIG_PATH + os.path.sep + '500_stocks_abstract.pkl')
    date = date.replace('-', '')
    stock_list = []
    for key in stock_abstract.keys():
        start_date = key.split('_')[0]
        end_date = key.split('_')[1]
        if date >= start_date and date <= end_date:
            stock_list = stock_abstract[key]
    stock_list.sort()
    get_logger().info('The stocks list: {0}'.format('|'.join(stock_list)))
    data_access = StockDataAccess(False)
    for stock in stock_list:
        try:
            data_per_date = data_access.access(date, stock)
        except Exception as e:
            get_logger().error('The data is missing for {0} and {1}'.format(date, stock))
            continue
        if end_time != '':
            data_per_date = data_per_date[(data_per_date['time'] >= add_milliseconds_suffix(start_time)) & (data_per_date['time'] <= add_milliseconds_suffix(end_time))]
        else:
            data_per_date = data_per_date[data_per_date['time'] == add_milliseconds_suffix(start_time)]
        data = pd.concat([data, data_per_date])
    data.to_csv(FACTOR_PATH + os.path.sep + 'validation' + os.path.sep + product + '_' + date + '_stocks_' + str(uuid.uuid4()) + '.csv')


def analyze_stability_extreme_value(factor, product, key, top_k):
    """
    打印极值日期

    Parameters
    ----------
    factor
    product
    key
    quantile

    Returns
    -------

    """
    data = factor.load(product, is_organized=True)
    data = data.dropna()
    mean_group_by = data.groupby('date')[key].mean()
    mean_list = np.array(mean_group_by.tolist())
    largest_top_k_mean = heapq.nlargest(top_k, mean_list)
    largest_top_k_mean_index = list(map(lambda val: mean_group_by[mean_group_by == val].index, largest_top_k_mean))
    smallest_top_k_mean = heapq.nsmallest(top_k, mean_list)
    smallest_top_k_mean_index = list(map(lambda val: mean_group_by[mean_group_by == val].index, smallest_top_k_mean))
    std_group_by = data.groupby('date')[key].std()
    std_list = np.array(std_group_by.tolist())
    largest_top_k_std = heapq.nlargest(top_k, std_list)
    largest_top_k_std_index = list(map(lambda val: std_group_by[std_group_by == val].index, largest_top_k_std))
    smallest_top_k_std = heapq.nsmallest(top_k, std_list)
    smallest_top_k_std_index = list(map(lambda val: std_group_by[std_group_by == val].index, smallest_top_k_std))
    return largest_top_k_mean, largest_top_k_mean_index, smallest_top_k_mean, smallest_top_k_mean_index, largest_top_k_std, largest_top_k_std_index, smallest_top_k_std, smallest_top_k_std_index


def check_factor_value_trend(factor, product, param):
    data = factor.load(product)
    data['year'] = data['date'].apply(lambda date: date[0:4])
    data['month'] = data['date'].apply(lambda date: date[0:7])
    year_data = data.groupby('year')[factor.get_key(param)].mean()
    print(year_data)
    month_data = data.groupby('month')[factor.get_key(param)].mean()
    month_data.to_csv('E:\\data\\temp\\aa.csv')
    print(month_data)


if __name__ == '__main__':
    #测试因子检测基类
    factor_validator = FactorValidator([
        BasicValidator(),
        StatisticsAnalysis(),
        StabilityValidator(),
        SingleFactorBackTestValidator()
    ])
    # factor_validator.validate([TotalCommissionRatioFactor()])
    # factor_validator.validate([SpreadFactor()])
    # factor_validator.validate([RisingStockRatioFactor()])
    # factor_validator.validate([RisingFallingAmountRatioFactor()])
    # factor_validator.validate([TenGradeCommissionRatioDifferenceFactor([20,50,100,200])])
    # factor_validator.validate([FiveGradeCommissionRatioFactor()])
    # factor_validator.validate([TenGradeCommissionRatioFactor()])
    # factor_validator.validate([UntradedStockRatioFactor()])
    # factor_validator.validate([TotalCommissionRatioDifferenceFactor([20,50,100,200])])
    # factor_validator.validate([FiveGradeCommissionRatioDifferenceFactor([20,50,100,200])])
    # factor_validator.validate([WilliamFactor([100,200,500,1000,2000,5000])])
    # factor_validator.validate([CloseMinusMovingAverageFactor([200,500,1000,1500])])
    # factor_validator.validate([DailyRisingStockRatioFactor()])
    # factor_validator.validate([TenGradeCommissionRatioFactor(), FiveGradeCommissionRatioFactor(), TenGradeCommissionRatioDifferenceFactor([20, 50, 100, 200]),FiveGradeCommissionRatioDifferenceFactor([20, 50, 100, 200]),
    #                RisingFallingAmountRatioFactor(), SpreadFactor(), TenGradeCommissionRatioMeanFactor([20,50,100,300,500]), TenGradeCommissionRatioStdFactor([50,100,300,500]), FiveGradeCommissionRatioMeanFactor([20,50,100,300,500]), FiveGradeCommissionRatioStdFactor([50,100,300,500])
    #                , TenGradeWeightedCommissionRatioFactor()])
    factor_validator.validate([AmountBid10GradeCommissionRatioFactor([20, 50, 100, 200]),
                   AmountBid5GradeCommissionRatioFactor([20, 50, 100, 200]),
                   AmountAsk10GradeCommissionRatioFactor([20, 50, 100, 200]),
                   AmountAsk5GradeCommissionRatioFactor([20, 50, 100, 200])])

    #检查片段
    # parse_factor_file('IC', [TenGradeCommissionRatioFactor()], '2022-02-14', is_organized=False)
    # parse_factor_file('IH', [RisingStockRatioFactor()], '2022-03-09')

    #检查现货相关股票文件
    # parse_spot_goods_stock_data('IC', '2022-02-14', '14:02:45')

    #分析因子极值
    # print(analyze_stability_extreme_value(RisingFallingAmountRatioFactor(), 'IC', RisingFallingAmountRatioFactor().get_key(), 5))

    #生成因子片段检查计算逻辑
    # factor = TenGradeCommissionRatioFactor()
    # date, data = factor.caculate_by_date(['2022-02-14', 'IC2203', 'IC'])
    # print(data[factor.get_key()])

    #因子的趋势性分析
    # check_factor_value_trend(RisingStockRatioFactor([20]), 'IH', 20)

    #检查有问题的因子
    # data = FiveGradeCommissionRatioFactor().load('IC')
    # print(data)
    # print(data[data.isnull().values==True])
    # print(data.isnull().stack()[lambda x:x].index.tolist())
    # print(data[['datetime', 'FCT_02_002_10_GRADE_COMMISSION_RATIO']])
    # print(data.isnull().sum())
    # print(data[np.isnan(data['ret.1'])]['datetime'])


