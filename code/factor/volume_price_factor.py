#! /usr/bin/env python
# -*- coding:utf8 -*-
import math
from math import sqrt

import pandas as pd

from common.visualization import draw_analysis_curve
from factor.base_factor import Factor
from common.constants import TEST_PATH
from common.localio import read_decompress
from factor.indicator import ATR, MovingAverage, LinearRegression

"""量价类因子
分类编号：01
"""
class WilliamFactor(Factor):
    """威廉因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_01_001_WILLIAM'
    version = '1.0'

    def __init__(self, params = [1000]):
        self._params = params

    def caculate(self, data):
        for param in self._params:
            data[self.get_key(param)] = (data['close'] - (data['high'].rolling(window=param).max()+data['low'].rolling(window=param).min())/2)/(data['high'].rolling(window=param).max()-data['low'].rolling(window=param).min())
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class CloseMinusMovingAverageFactor(Factor):
    """
    TSSB CLOSE MINUS MOVING AVERAGE HistLength ATRlength 因子
    """

    factor_code = 'FCT_01_002_CLOSE_MINUS_MOVING_AVERAGE'
    version = '1.0'

    def __init__(self, params = [200, 500, 1000, 1500]):
        self._params = params
        self._atr = ATR(list(map(lambda param: 2 * param, self._params)))
        self._moving_average = MovingAverage(self._params)

    def caculate(self, data):
        data = self._moving_average.enrich(data)
        data = self._atr.enrich(data)
        for param in self._params:
            data[self.get_key(param)] = data.apply(lambda item : self.formula(item, param), axis = 1)
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data
    
    def formula(self, item, param):
        moving_average_get_key = self._moving_average.get_key(param)
        numerator = math.log((item['close'] / item[moving_average_get_key]), math.e)
        atr_get_key = self._atr.get_key(2 * param)
        denominator = sqrt(param) * item[atr_get_key]
        if denominator == 0:
            return 0
        else:
            return numerator/denominator

class LinearPerAtrFactor(Factor):
    """
    TSSB LINEAR PER ATR HistLength ATRlength 因子
    """

    factor_code = 'FCT_01_003_LINEAR_PER_ATR'
    version = '1.0'

    def __init__(self, params = [10, 20, 50, 100]):
        self._params = params
        self._atr = ATR(self._params)
        self._linear_regression = LinearRegression(self._params, 'log')

    def caculate(self, data):
        data['log'] = data.apply(lambda item:math.log((item['open'] + item['close'] + item['high'] + item['low'])/4), axis=1)
        data = self._linear_regression.enrich(data)
        data = self._atr.enrich(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._linear_regression.get_key(param)]/data[self._atr.get_key(param)]
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data


if __name__ == '__main__':
    # 测试数据
    # data = read_decompress(TEST_PATH + 'IC2003.pkl')
    data = read_decompress('/Users/finley/Projects/stock-index-future/data/organised/future/IH/IH2209.pkl')

    # 测试威廉因子
    # william_factor = WilliamFactor([10])
    # print(william_factor.factor_code)
    # print(william_factor.version)
    # print(william_factor.get_params())
    # print(william_factor.get_category())
    # print(william_factor.get_full_name())
    # print(william_factor.get_key(5))
    # print(william_factor.get_keys())
    # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # data = william_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=william_factor.get_keys())

    # 测试TSSB CLOSE MINUS MOVING AVERAGE HistLength ATRlength 因子
    # close_minus_moving_average_factor = CloseMinusMovingAverageFactor([10])
    # print(close_minus_moving_average_factor.factor_code)
    # print(close_minus_moving_average_factor.version)
    # print(close_minus_moving_average_factor.get_params())
    # print(close_minus_moving_average_factor.get_category())
    # print(close_minus_moving_average_factor.get_full_name())
    # print(close_minus_moving_average_factor.get_key(5))
    # print(close_minus_moving_average_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # data = close_minus_moving_average_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=close_minus_moving_average_factor.get_keys())

    # TSSB LINEAR PER ATR HistLength ATRlength
    linear_per_atr_factor = LinearPerAtrFactor([10])
    print(linear_per_atr_factor.factor_code)
    print(linear_per_atr_factor.version)
    print(linear_per_atr_factor.get_params())
    print(linear_per_atr_factor.get_category())
    print(linear_per_atr_factor.get_full_name())
    print(linear_per_atr_factor.get_key(5))
    print(linear_per_atr_factor.get_keys())
    # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    data = linear_per_atr_factor.caculate(data)
    data.index = pd.DatetimeIndex(data['datetime'])
    draw_analysis_curve(data, show_signal=True, signal_keys=linear_per_atr_factor.get_keys())

    # 测试加载数据
    # data = william_factor.load()
    # print(data)





