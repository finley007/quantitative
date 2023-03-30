#! /usr/bin/env python
# -*- coding:utf8 -*-

from abc import ABCMeta, abstractmethod
import os
import numpy as np
import datetime as dt
from functools import lru_cache
import time

from common.persistence.dbutils import create_session
from common.persistence.dao import FutureConfigDao, IndexConstituentConfigDao
from common.constants import STOCK_INDEX_PRODUCTS, FACTOR_PATH, FACTOR_STANDARD_FIELD_TYPE, RET_PERIOD, FUTURE_TICK_ORGANIZED_DATA_PATH
from common.localio import save_compress, read_decompress
from factor.spot_goods_factor import TotalCommissionRatioFactor, RisingStockRatioFactor, TenGradeCommissionRatioFactor, SpreadFactor, RisingFallingAmountRatioFactor, UntradedStockRatioFactor, FiveGradeCommissionRatioFactor, TotalCommissionRatioDifferenceFactor
from common.log import get_logger
from common.timeutils import get_current_time
from framework.localconcurrent import ProcessRunner
from framework.pagination import Pagination
from framework.localconcurrent import ProcessExcecutor

"""
该包主要用于因子文件的进一步处理和数据修复
"""

def recaculate_factor_to_ignore_invalid_data(factor_list, products=[], is_backup=True):
    """
    重新计算因子，根据股指成分股配置忽略掉非法数据
    Parameters
    ----------
    factor_list
    products
    is_backup

    Returns
    -------

    """
    # index_constituent_config_dao = IndexConstituentConfigDao()
    # dates_to_be_fixed = index_constituent_config_dao.get_invalid_date_list([2])
    session = create_session()
    dates_to_be_fixed = session.execute(
        "select distinct date from (select distinct date from stock_validation_result where validation_code = '20230309-finley' and result = 2 union select distinct date from stock_validation_result where validation_code = '20230309-finley' and result = 1 and issue_count <= 10) t order by date").fetchall()
    dates_to_be_fixed = list(map(lambda date:date[0][0:4] + '-' + date[0][4:6] + '-' + date[0][6:8], dates_to_be_fixed))
    print(dates_to_be_fixed)
    fix_factor(factor_list, products, dates_to_be_fixed, is_backup)

def fix_factor(factor_list, products=[], dates_to_be_fixed=[], is_backup=True):
    """
    修复因子:
    按天重新计算因子
    Parameters
    ----------
    factor_list
    products

    Returns
    -------

    """
    if len(products) == 0:
        products = STOCK_INDEX_PRODUCTS
    for factor in factor_list:
        for product in products:
            data = factor.load(product)
            if is_backup:
                save_compress(data, FACTOR_PATH + os.path.sep + product + '_' + factor.get_full_name() + '.bak' + get_current_time())
            pagination = Pagination(dates_to_be_fixed, page_size=10)
            while pagination.has_next():
                date_list = pagination.next()
                get_logger().info('Fix product: {0}, date list: {1} for factor: {2}'.format(product, date_list, factor.get_full_name()))
                params_list = list(map(lambda date: create_params_for_fix_factor_by_date(date, factor, product), date_list))
                while '' in params_list:
                    params_list.remove('')
                print(params_list)
                results = ProcessExcecutor(5).execute(fix_factor_by_date, params_list)
                for result in results:
                    if len(factor.get_params()) > 0:
                        for param in factor.get_params():
                            date_data_dict = dict(zip(result[1]['time'], result[1][factor.get_key(param)]))
                            data.loc[data['date'] == result[0], factor.get_key(param)] = data['datetime'].apply(lambda item: get_value(item, date_data_dict))
                    else:
                        date_data_dict = dict(zip(result[1]['time'], result[1][factor.get_key()]))
                        data.loc[data['date'] == result[0], factor.get_key()] = data['datetime'].apply(lambda item: get_value(item, date_data_dict))
            save_path = FACTOR_PATH + os.path.sep + product + '_' + factor.get_full_name()
            save_compress(data, save_path)


def fix_factor_by_date(*args):
    """
    多进程计算因子值
    Parameters
    ----------
    date
    factor
    instrument
    product

    Returns
    -------

    """
    factor = args[0][1]
    param_date = args[0][0]
    param_instrument = args[0][2]
    param_product = args[0][3]
    date, date_data = factor.caculate_by_date([param_date, param_instrument, param_product])
    return date, date_data

def create_params_for_fix_factor_by_date(date, factor, product):
    future_config_dao = FutureConfigDao()
    instrument = future_config_dao.get_main_instrument_by_product_and_date(product, date)
    if instrument == '':
        return ''
    else:
        return [date, factor, instrument, product]

def fix_ret(factor_list, products=[], is_backup=True):
    """
    修复因子文件中的收益率
    Parameters
    ----------
    factor_list
    products
    is_backup

    Returns
    -------

    """
    if len(products) == 0:
        products = STOCK_INDEX_PRODUCTS

    runner = ProcessRunner(10)
    for factor in factor_list:
        for product in products:
            fix_ret_by_product(factor, is_backup, product)
            # runner.execute(fix_ret_by_product, args=(factor, is_backup, product))
    time.sleep(100000)

def fix_ret_by_product(factor, is_backup, product):
    future_config_dao = FutureConfigDao()
    data = factor.load(product)
    date_list = list(set(data['date'].tolist()))
    date_list.sort()
    get_logger().info('Date list for product {0}: {1}'.format(product, date_list))
    if is_backup:
        save_compress(data,
                      FACTOR_PATH + os.path.sep + product + '_' + factor.get_full_name() + '.bak' + get_current_time())
    for date in date_list:
        get_logger().info('Fix product: {0}, date: {1} for factor: {2}'.format(product, date, factor.get_full_name()))
        instrument = future_config_dao.get_main_instrument_by_product_and_date(product, date)
        if instrument == '':
            get_logger().warning('Main instrument not found for product: {0} and date: {1}'.format(product, date))
            continue
        instrument_data = get_instrument_data(instrument, product)
        for period in RET_PERIOD:
            date_data_dict = dict(zip(instrument_data['datetime'], instrument_data['ret.' + str(period)]))
            data.loc[data['date'] == date, 'ret.' + str(period)] = data[data['date'] == date]['datetime'].apply(lambda item: get_value(item, date_data_dict, 'datetime'))
    save_path = FACTOR_PATH + os.path.sep + product + '_' + factor.get_full_name()
    save_compress(data, save_path)


@lru_cache(maxsize=10)
def get_instrument_data(instrument, product):
    """
    获取合约文件，增加缓存
    Parameters
    ----------
    instrument
    product

    Returns
    -------

    """
    return read_decompress(FUTURE_TICK_ORGANIZED_DATA_PATH + os.path.sep + product + os.path.sep + instrument + '.pkl')


def get_value(item, date_data_dict, field='time'):
    if field == 'time':
        time = item[11:23]
    else:
        time = item
    try:
        return date_data_dict[time]
    except Exception as e:
        get_logger().error('Fix factor error')
        return 0

def handle_factor(factor_list, products=[]):
    """
    处理因子
    Parameters
    ----------
    factor_list
    products

    Returns
    -------

    """
    if len(products) == 0:
        products = STOCK_INDEX_PRODUCTS
    handler_list = create_handler_list()
    for factor in factor_list:
        for product in products:
            data = factor.load(product)
            get_logger().info('Handle product: {0}'.format(product))
            for handler in handler_list:
                if len(handler.factor_filter()) == 0 or factor.factor_code in handler.factor_filter():
                    data = handler.handle(product, data, factor)
            save_path = FACTOR_PATH + os.path.sep + 'organized' + os.path.sep + product + '_' + factor.get_full_name()
            save_compress(data, save_path)

def create_handler_list():
    return [DateFilterHandler(), MissingDataFilterHandler(), TotalCommissionRatioFactorHandler()]

class FactorHandler(metaclass=ABCMeta):
    """
    因子处理类接口
    """

    @abstractmethod
    def handle(self, product, data, factor):
        """
        处理主逻辑
        Parameters
        ----------
        data

        Returns
        -------

        """
        pass

    def factor_filter(self):
        return []

class DateFilterHandler(FactorHandler):
    """
    1. 检查因子字段类型，转换为float64
    2. 数据过滤，过滤20170103和20220812之外的数据和现货数据缺失的几天：
    '2017-05-10','2017-05-22','2017-12-01','2019-05-08','2021-07-06','2021-08-11','2022-08-01','2022-08-02','2022-08-03','2022-08-04','2022-08-05','2022-08-06','2022-08-07','2022-08-08','2022-08-09','2022-08-10','2022-08-11'
    再加上一个2022-02-08，这一天很多数据不全导致IF的计算有问题
    """
    def handle(self, product, data, factor):
        get_logger().info('Run date filter handler for factor: {0}'.format(factor.get_full_name()))
        if len(factor.get_params()) > 0:
            for param in factor.get_params():
                if data.dtypes[factor.get_key(param)] != FACTOR_STANDARD_FIELD_TYPE:
                    data[factor.get_key(param)] = data[factor.get_key(param)].astype(FACTOR_STANDARD_FIELD_TYPE)
        else:
            if data.dtypes[factor.get_key()] != FACTOR_STANDARD_FIELD_TYPE:
                data[factor.get_key()] = data[factor.get_key()].astype(FACTOR_STANDARD_FIELD_TYPE)
        future_config_dao = FutureConfigDao()
        date_list = future_config_dao.filter_date('2016-12-30', '2022-08-13')
        date_list = date_list + ['2017-05-10','2017-05-22','2017-12-01','2019-05-08','2021-07-06','2021-07-06','2021-08-11','2022-02-08','2022-08-01','2022-08-02','2022-08-03','2022-08-04','2022-08-05','2022-08-06','2022-08-07','2022-08-08','2022-08-09','2022-08-10','2022-08-11']
        for date in date_list:
            data = data.drop(data.index[data['date'] == date])
        return data

class MissingDataFilterHandler(FactorHandler):
    """
    2020-07-08,454,0,0,454 - data miss
    2020-07-09,741,0,0,741 - data miss
    2020-07-14,918,0,0,918 - data miss
    2021-07-22,361,0,0,361 - data miss
    2021-08-03,346,0,0,346 - data miss
    2022-05-30,13,0,0,13 - data miss
    2020-08-03,640,0,0,640 - IH data miss
    处理方式：补0
    """
    def handle(self, product, data, factor):
        get_logger().info('Missing data filter handler for factor: {0}'.format(factor.get_full_name()))
        date_list = ['2020-07-08','2020-07-09','2020-07-14','2021-07-22','2021-08-03','2022-05-30']
        for date in date_list:
            if len(factor.get_params()) > 0:
                for param in factor.get_params():
                    data.loc[(data['date'] == date) & np.isnan(data[factor.get_key(param)]), factor.get_key(param)] = 0
            else:
                data.loc[(data['date'] == date) & np.isnan(data[factor.get_key()]), factor.get_key()] = 0
        if product == 'IH':
            if len(factor.get_params()) > 0:
                for param in factor.get_params():
                    data.loc[(data['date'] == '2020-08-03') & np.isnan(data[factor.get_key(param)]), factor.get_key(param)] = 0
            else:
                data.loc[(data['date'] == '2020-08-03') & np.isnan(data[factor.get_key()]), factor.get_key()] = 0
        return data

class TotalCommissionRatioFactorHandler(FactorHandler):
    """
    总委比因子需要这四个字段：'weighted_average_bid_price', 'weighted_average_ask_price', 'total_bid_volume', 'total_ask_volume'
    从2022-02-07之后的数据就没有这四个字段，对于已经生成的因子需要处理填充0，
    这个直接抹0是有问题的，需要用10档因子值先代替总委比
    # TODO
    对于因子计算逻辑如果数据缺失就自动填0
    """
    def handle(self, product, data, factor):
        get_logger().info('Total commission ratio handler for factor: {0}'.format(factor.get_full_name()))
        future_config_dao = FutureConfigDao()
        date_list = future_config_dao.filter_date('', '2022-02-07')
        ten_grade_commission_ratio_factor = TenGradeCommissionRatioFactor()
        for date in date_list:
            data.loc[data['date'] == date, factor.get_key()] = 0
            # 不能直接用10档替换总委比，不连续
            # instrument = future_config_dao.get_main_instrument_by_product_and_date(product, date)
            # if instrument == '':
            #     get_logger().warning('Main instrument not found for product: {0} and date: {1}'.format(product, date))
            #     continue
            # if len(data[data['date'] == date]) > 0:
            #     get_logger().info('Fix total commission ratio for date: {0}'.format(date))
            #     date, date_data = ten_grade_commission_ratio_factor.caculate_by_date([date, instrument, product])
            #     date_data_dict = dict(zip(date_data['time'], date_data[ten_grade_commission_ratio_factor.get_key()]))
            #     data.loc[data['date'] == date, factor.get_key()] = data['datetime'].apply(lambda item: self.get_value(item, date_data_dict))
        #IH 数据修复
        instrument_list = [{'instrument':'IH1701','dates':['2017-01-13']}, {'instrument':'IH1704','dates':['2017-03-20','2017-04-05','2017-04-11']},{'instrument':'IH1707','dates':['2017-06-28','2017-07-05','2017-07-18']},{'instrument':'IH1708','dates':['2017-08-02','2017-08-08']},{'instrument':'IH1709','dates':['2017-08-29']}
                           ,{'instrument':'IH1710','dates':['2017-10-12']},{'instrument':'IH1801','dates':['2018-01-02','2018-01-09']},{'instrument':'IH1802','dates':['2018-01-29','2018-02-02','2018-02-12']},{'instrument':'IH1803','dates':['2018-02-26','2018-03-01','2018-03-09']}
                           ,{'instrument':'IH1804','dates':['2018-03-26','2018-04-13']},{'instrument':'IH1806','dates':['2018-06-08']},{'instrument':'IH2105','dates':['2021-05-10']}]
        if product == 'IH':
            for instrument_info in instrument_list:
                instrument = instrument_info['instrument']
                dates = instrument_info['dates']
                for date in dates:
                    date, date_data = factor.caculate_by_date([date, instrument, product])
                    issue_item_time = data[(data['date'] == date) & np.isnan(data[factor.get_key()])]['datetime'].tolist()
                    for datetime in issue_item_time:
                        time = datetime[11:23]
                        factor_value = date_data[date_data['time'] == time][factor.get_key()]
                        data.loc[(data['date'] == date) & np.isnan(data[factor.get_key()]), factor.get_key()] = factor_value[0]
        return data

    def get_value(self, item, date_data_dict):
        time = item[11:23]
        try:
            return date_data_dict[time]
        except Exception as e:
            get_logger().error('Fix total commission ratio error')
            return 0

    def factor_filter(self):
        return [TotalCommissionRatioFactor.factor_code]


if __name__ == '__main__':
    # factor_list = [TotalCommissionRatioFactor()]
    # factor_list = [RisingStockRatioFactor()]
    # factor_list = [SpreadFactor()]
    # factor_list = [RisingFallingAmountRatioFactor()]
    # factor_list = [TenGradeCommissionRatioFactor()]
    # factor_list = [FiveGradeCommissionRatioFactor()]
    # factor_list = [UntradedStockRatioFactor()]
    # factor_list = [TotalCommissionRatioDifferenceFactor([20,50,100,200])]
    # handle_factor(factor_list)

    # factor = TotalCommissionRatioFactor()
    # data = factor.load('IC')
    # handler = TotalCommissionRatioFactorHandler()
    # handler.handle('IC', data, factor)

    # factor_list = [RisingFallingAmountRatioFactor()]
    # fix_factor(factor_list, ['IC'], dates_to_be_fixed=['2022-06-02'])

    factor_list = [FiveGradeCommissionRatioFactor()]
    recaculate_factor_to_ignore_invalid_data(factor_list)

    # factor_list = [FiveGradeCommissionRatioFactor()]
    # fix_ret(factor_list)