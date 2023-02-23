#! /usr/bin/env python
# -*- coding:utf8 -*-
import os
import re
import time
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages
import uuid
import numpy as np

from common.aop import timing
from common.constants import FUTURE_TICK_ORGANIZED_DATA_PATH, CONFIG_PATH, FACTOR_PATH, STOCK_INDEX_PRODUCTS, \
    REPORT_PATH, STOCK_TRANSACTION_START_TIME, STOCK_TRANSACTION_END_TIME, STOCK_TRANSACTION_NOON_END_TIME, STOCK_TRANSACTION_NOON_START_TIME
from common.exception.exception import InvalidStatus
from common.localio import read_decompress, list_files_in_path, save_compress
from common.persistence.dbutils import create_session
from common.persistence.po import FutureInstrumentConfig
from common.visualization import draw_line, join_two_images
from factor.volume_price_factor import WilliamFactor
from factor.spot_goods_factor import TotalCommissionRatioFactor, TenGradeCommissionRatioFactor, AmountAndCommissionRatioFactor, FiveGradeCommissionRatioFactor, \
    TenGradeWeightedCommissionRatioFactor, FiveGradeCommissionRatioFactor, RisingFallingVolumeRatioFactor, UntradedStockRatioFactor, DailyAccumulatedLargeOrderRatioFactor, \
    RollingAccumulatedLargeOrderRatioFactor, RisingStockRatioFactor, SpreadFactor, OverNightYieldFactor, DeltaTotalCommissionRatioFactor
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

    def __init__(self, validator_list = []):
        self._validator_list = validator_list

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
                validator.validate(factor_list)

class BasicValidator(FactorValidator):
    """
    一些基本检查
    1. 生成分析报告：
    合约数，天数，记录数（盘前，早盘，午盘），为空或者0的记录数

    """

    def __init__(self, check_nan = True):
        self._check_nan = check_nan

    @timing
    def validate(self, factor_list):
        error_factor = []
        for factor in factor_list:
            get_logger().info('Validate for factor {0}'.format(factor.get_full_name()))
            for product in STOCK_INDEX_PRODUCTS:
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
                            for param in factor.get_params():
                                nan_count = nan_count + len(date_data[np.isnan(date_data[factor.get_key()])])
                                zero_count = zero_count + len(date_data[date_data[factor.get_key(param)] == 0])
                        morning_market_count = len(date_data[(date_data['time'] >= add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)) & (date_data['time'] <= add_milliseconds_suffix(STOCK_TRANSACTION_NOON_END_TIME))])
                        afternoon_market_count = len(date_data[(date_data['time'] >= add_milliseconds_suffix(STOCK_TRANSACTION_NOON_START_TIME)) & (date_data['time'] <= add_milliseconds_suffix(STOCK_TRANSACTION_END_TIME))])
                        date_info = DateInfo(date, nan_count, zero_count, morning_market_count, afternoon_market_count)
                        instrument_info.add_date(date_info)
                    factor_analysis_result.add_instrument(instrument_info)
                factor_analysis_result.print()
                if self._check_nan: #打印空数据
                    if len(factor.get_params()) == 0:
                        if len(data[np.isnan(data[factor.get_key()])]) > 0:
                            error_factor.append(product + '-' + factor.get_full_name())
                    else:
                        for param in factor.get_params():
                            if len(data[np.isnan(data[factor.get_key(param)])]) > 0:
                                error_factor.append(product + '-' + factor.get_full_name() + str(param))
        if len(error_factor) > 0:
            error_factor_list = '|'.join(error_factor)
            raise ValidationFailed(error_factor_list + ' has nan value')

class StatisticsAnalysis(FactorValidator):
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

        Parameters
        ----------
        factor
        factor_report_path
        key

        Returns
        -------

        """
        columns = ['count', 'mean', 'std', 'min', 'max', '1%', '5%', '10%', '50%', '90%', '95%', '99%', 'abs_mean',
                   'ic']
        content = []
        index = []
        for product in STOCK_INDEX_PRODUCTS:
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
            content.append([count, mean, std, min, max, quantile1, quantile5, quantile10, quantile50, quantile90, quantile95, quantile99, abs_mean, ic])
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


class StabilityValidator(FactorValidator):
    """因子稳定性检查类
        按天计算因子值的均值和标准差
    """

    @timing
    def validate(self, factor_list):
        factor_diagram_path = REPORT_PATH + 'factor'
        if not os.path.exists(factor_diagram_path):
            os.makedirs(factor_diagram_path)
        for factor in factor_list:
            if len(factor.get_params()) > 0:
                for param in factor.get_params():
                    for product in STOCK_INDEX_PRODUCTS:
                        self.create_diagram(factor, factor_diagram_path, product, factor.get_key(param))
            else:
                for product in STOCK_INDEX_PRODUCTS:
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
        join_two_images(mean_path, std_path, final_path, flag='vertical')
        os.remove(mean_path)
        os.remove(std_path)


def parse_factor_file(product, factor_list, start_date, end_date=''):
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
    path = FACTOR_PATH + os.path.sep + 'organized' + os.path.sep + product + '_' + '_'.join(list(map(lambda factor: factor.get_full_name(), factor_list)))
    data = read_decompress(path)
    if end_date == '':
        data = data[data['date'] == start_date]
    else:
        data = data[(data['date'] >= start_date) & (data['date'] <= end_date)]
    for factor in factor_list:
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


if __name__ == '__main__':
    #测试因子检测基类
    factor_validator = FactorValidator([
        BasicValidator(),
        StatisticsAnalysis(),
        StabilityValidator()
    ])
    factor_validator.validate([TotalCommissionRatioFactor()])
    # factor_validator.validate([SpreadFactor()])
    # factor_validator.validate([RisingStockRatioFactor()])

    #检查片段
    # parse_factor_file('IH', [TotalCommissionRatioFactor()], '2017-03-20')

    #检查现货相关股票文件
    # parse_spot_goods_stock_data('IC', '2022-01-11', ' 9:30:03')

    #检查有问题的因子
    # data = WilliamFactor().load('IC')
    # print(data)
    # print(data[data.isnull().values==True])
    # print(data.isnull().stack()[lambda x:x].index.tolist())
    # print(data[['datetime', 'FCT_01_001_WILLIAM']])


