#! /usr/bin/env python
# -*- coding:utf8 -*-

from abc import ABCMeta, abstractmethod
import os
import numpy as np

from common.persistence.dao import FutureConfigDao
from common.constants import STOCK_INDEX_PRODUCTS, FACTOR_PATH, FACTOR_STANDARD_FIELD_TYPE
from common.localio import save_compress
from factor.spot_goods_factor import TotalCommissionRatioFactor

"""
该包主要用于因子文件的进一步处理和数据修复
"""
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
        # products = ['IH']
    handler_list = create_handler_list()
    for factor in factor_list:
        for product in products:
            data = factor.load(product)
            for handler in handler_list:
                if len(handler.factor_filter()) == 0 or factor.factor_code in handler.factor_filter():
                    data = handler.handle(product, data, factor)
                factor_name = '_'.join(list(map(lambda factor: factor.get_full_name(), factor_list)))
            save_path = FACTOR_PATH + os.path.sep + 'organized' + os.path.sep + product + '_' + factor_name
            save_compress(data, save_path)

def create_handler_list():
    return [DateFilterHandler(), MissingDataFilterHandler(), TotalCommissionRatioFactoHandler()]

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
    '2017-05-10','2017-05-22','2017-12-01','2019-05-08','2021-07-06','2021-07-06','2021-08-11'
    """
    def handle(self, product, data, factor):
        if len(factor.get_params()) > 0:
            for param in factor.get_params():
                if data.dtypes[factor.get_key(param)] != FACTOR_STANDARD_FIELD_TYPE:
                    data[factor.get_key(param)] = data[factor.get_key(param)].astype(FACTOR_STANDARD_FIELD_TYPE)
        else:
            if data.dtypes[factor.get_key()] != FACTOR_STANDARD_FIELD_TYPE:
                data[factor.get_key()] = data[factor.get_key()].astype(FACTOR_STANDARD_FIELD_TYPE)
        future_config_dao = FutureConfigDao()
        date_list = future_config_dao.filter_date('2016-12-30', '2022-09-01')
        date_list = date_list + ['2017-05-10','2017-05-22','2017-12-01','2019-05-08','2021-07-06','2021-07-06','2021-08-11']
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

class TotalCommissionRatioFactoHandler(FactorHandler):
    """
    总委比因子需要这四个字段：'weighted_average_bid_price', 'weighted_average_ask_price', 'total_bid_volume', 'total_ask_volume'
    从2022-02-07之后的数据就没有这四个字段，对于已经生成的因子需要处理填充0，
    # TODO
    对于因子计算逻辑如果数据缺失就自动填0
    """
    def handle(self, product, data, factor):
        future_config_dao = FutureConfigDao()
        date_list = future_config_dao.filter_date('', '2022-02-07')
        for date in date_list:
            data.loc[data['date'] == date, factor.get_key()] = 0
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

    def factor_filter(self):
        return [TotalCommissionRatioFactor.factor_code]


if __name__ == '__main__':
    factor_list = [TotalCommissionRatioFactor()]
    handle_factor(factor_list)
