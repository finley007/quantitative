#! /usr/bin/env python
# -*- coding:utf8 -*-
import math
from math import sqrt

import pandas as pd
import numpy as np

from common.visualization import draw_analysis_curve
from factor.base_factor import Factor
from common.localio import read_decompress
from factor.indicator import ATR, MovingAverage, LinearRegression, PolynomialRegression, StandardDeviation, ADX, TR, \
    Variance, Skewness, Kurtosis

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
        self._linear_regression.set_caculation_func(self.caculation_function)

    def caculate(self, data):
        data['log'] = data.apply(lambda item:math.log((item['open'] + item['close'] + item['high'] + item['low'])/4), axis=1)
        data = self._linear_regression.enrich(data)
        data = self._atr.enrich(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._linear_regression.get_key(param)]/data[self._atr.get_key(param)]
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

    def caculation_function(self, model, window):
        return model.coef_

class LinearDeviationFactor(Factor):
    """
    TSSB LINEAR DEVIATION HistLength 因子
    """

    factor_code = 'FCT_01_004_LINEAR_DEVIATION'
    version = '1.0'

    def __init__(self, params = [10, 20, 50, 100]):
        self._params = params
        self._linear_regression = LinearRegression(self._params)
        self._linear_regression.set_caculation_func(self.caculation_function)

    def caculate(self, data):
        data = self._linear_regression.enrich(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._linear_regression.get_key(param)]
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

    def caculation_function(self, model, window):
        x = np.linspace(0, 1, len(window)).reshape(-1, 1)
        estimated_values = model.predict(x)
        estimated_value = estimated_values[-1]
        true_value = window.tolist()[-1]
        return (true_value - estimated_value) / self.get_standard_error(window.tolist(), estimated_values)

    def get_standard_error(self, true_values, estimated_values):
        n = len(true_values)
        return ((((np.array(true_values) - np.array(estimated_values))**2).sum() / (n - 1)) ** 0.5) / n ** 0.5 #stardand error等于标准差除以根号n

class QuadraticDeviationFactor(Factor):
    """
    TSSB QUADRATIC DEVIATION HistLength 因子
    """

    factor_code = 'FCT_01_005_QUADRATIC_DEVIATION'
    version = '1.0'

    def __init__(self, params = [10, 20, 50, 100]):
        self._params = params
        self._polynomial_regression = PolynomialRegression(self._params)
        self._polynomial_regression.set_caculation_func(self.caculation_function)

    def caculate(self, data):
        data = self._polynomial_regression.enrich(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._polynomial_regression.get_key(param)]
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

    def caculation_function(self, model, poly, window):
        x = np.linspace(0, 1, len(window)).reshape(-1, 1)
        X = poly.transform(x.reshape(-1, 1))
        estimated_values = model.predict(X)
        estimated_value = estimated_values[-1]
        true_value = window.tolist()[-1]
        return (true_value - estimated_value) / self.get_standard_error(window.tolist(), estimated_values)

    def get_standard_error(self, true_values, estimated_values):
        n = len(true_values)
        return ((((np.array(true_values) - np.array(estimated_values))**2).sum() / (n - 1)) ** 0.5) / n ** 0.5 #stardand error等于标准差除以根号n

class CubicDeviationFactor(Factor):
    """
    TSSB CUBIC DEVIATION HistLength 因子
    """

    factor_code = 'FCT_01_006_CUBIC_DEVIATION'
    version = '1.0'

    def __init__(self, params = [10, 20, 50, 100]):
        self._params = params
        self._polynomial_regression = PolynomialRegression(self._params, order = 3) #三阶多项式
        self._polynomial_regression.set_caculation_func(self.caculation_function)

    def caculate(self, data):
        data = self._polynomial_regression.enrich(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._polynomial_regression.get_key(param)]
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

    def caculation_function(self, model, poly, window):
        x = np.linspace(0, 1, len(window)).reshape(-1, 1)
        X = poly.transform(x.reshape(-1, 1))
        estimated_values = model.predict(X)
        estimated_value = estimated_values[-1]
        true_value = window.tolist()[-1]
        return (true_value - estimated_value) / self.get_standard_error(window.tolist(), estimated_values)

    def get_standard_error(self, true_values, estimated_values):
        n = len(true_values)
        return ((((np.array(true_values) - np.array(estimated_values))**2).sum() / (n - 1)) ** 0.5) / n ** 0.5 #stardand error等于标准差除以根号n

class PriceMomentumFactor(Factor):
    """
    TSSB PRICE MOMENTUM HistLength StdDevLength 因子
    """

    factor_code = 'FCT_01_007_PRICE_MOMENTUM'
    version = '1.0'

    def __init__(self, params = [10, 20, 50, 100]):
        self._params = params
        self._multiplier = 2
        self._standard_deviation = StandardDeviation(list(map(lambda x: self._multiplier*x, self._params)), 'log2')

    def caculate(self, data):
        data['mean'] = (data['open'] + data['close'] + data['high'] + data['low'])/4
        data['log2'] = data['mean'].rolling(2).apply(lambda item: self.formula(item))
        data = self._standard_deviation.enrich(data)
        for param in self._params:
            data['log' + str(param)] = data['mean'].rolling(param).apply(lambda item: self.formula(item))
            data[self.get_key(param)] = data['log' + str(param)]/(data[self._standard_deviation.get_key(self._multiplier*param)]*(param**0.5))
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

    def formula(self, item):
        list = item.tolist()
        return math.log(list[-1]/list[0])

class AdxFactor(Factor):
    """
    TSSB ADX HistLength 因子
    """

    factor_code = 'FCT_01_008_ADX'
    version = '1.0'

    def __init__(self, params = [14]):
        self._params = params
        self._adx = ADX(self._params)

    def caculate(self, data):
        data = self._adx.enrich(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._adx.get_key(param)]
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class MinAdxFactor(Factor):
    """
    TSSB MIN ADX HistLength MinLength 因子
    """

    factor_code = 'FCT_01_008_MIN_ADX'
    version = '1.0'

    def __init__(self, params = [20]):
        self._params = params
        self._adx = ADX([14])

    def caculate(self, data):
        data = self._adx.enrich(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._adx.get_key(self._adx.get_params()[0])].rolling(param).min()
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class ResidualMinAdxFactor(Factor):
    """
    TSSB RESIDUAL MIN ADX HistLength MinLength 因子
    """

    factor_code = 'FCT_01_009_RESIDUAL_MIN_ADX'
    version = '1.0'

    def __init__(self, params = [20]):
        self._params = params
        self._adx = ADX([14])

    def caculate(self, data):
        data = self._adx.enrich(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._adx.get_key(self._adx.get_params()[0])] - data[self._adx.get_key(self._adx.get_params()[0])].rolling(param).min()
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class MaxAdxFactor(Factor):
    """
    TSSB MAX ADX HistLength MaxLength 因子
    """

    factor_code = 'FCT_01_010_MAX_ADX'
    version = '1.0'

    def __init__(self, params = [20]):
        self._params = params
        self._adx = ADX([14])

    def caculate(self, data):
        data = self._adx.enrich(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._adx.get_key(self._adx.get_params()[0])].rolling(param).max()
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class ResidualMaxAdxFactor(Factor):
    """
    TSSB RESIDUAL MAX ADX HistLength MaxLength 因子
    """

    factor_code = 'FCT_01_011_RESIDUAL_MAX_ADX'
    version = '1.0'

    def __init__(self, params = [20]):
        self._params = params
        self._adx = ADX([14])

    def caculate(self, data):
        data = self._adx.enrich(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._adx.get_key(self._adx.get_params()[0])].max() - data[self._adx.get_key(self._adx.get_params()[0])].rolling(param)
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data


class DeltaAdxFactor(Factor):
    """
    TSSB DELTA ADX HistLength DeltaLength 因子
    """

    factor_code = 'FCT_01_012_DELTA_ADX'
    version = '1.0'

    def __init__(self, params = [20]):
        self._params = params
        self._adx = ADX([14])

    def caculate(self, data):
        data = self._adx.enrich(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._adx.get_key(self._adx.get_params()[0])] - data[self._adx.get_key(self._adx.get_params()[0])].shift(param)
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class AccelAdxFactor(Factor):
    """
    TSSB ACCEL ADX HistLength DeltaLength 因子
    """

    factor_code = 'FCT_01_013_ACCEL_ADX'
    version = '1.0'

    def __init__(self, params = [20]):
        self._params = params
        self._adx = ADX([14])

    def caculate(self, data):
        data = self._adx.enrich(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._adx.get_key(self._adx.get_params()[0])] + data[self._adx.get_key(self._adx.get_params()[0])].shift(2 * param)\
                                        - 2 * data[self._adx.get_key(self._adx.get_params()[0])].shift(param)
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class IntradayIntensityFactor(Factor):
    """
    TSSB INTRADAY INTENSITY HistLength 因子
    """

    factor_code = 'FCT_01_014_INTRADAY_INTENSITY'
    version = '1.0'

    def __init__(self, params = [50, 100, 200, 500]):
        self._params = params
        self._tr = TR()
        self._moving_average = MovingAverage(self._params, IntradayIntensityFactor.factor_code)

    def caculate(self, data):
        data = self._tr.enrich(data)
        data[IntradayIntensityFactor.factor_code] = (data['close'] - data['open'])/data[self._tr.get_key()]
        data = self._moving_average.enrich(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._moving_average.get_key(param)]
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class DeltaIntradayIntensityFactor(Factor):
    """
    TSSB DELTA INTRADAY INTENSITY HistLength DeltaLength 因子
    """

    factor_code = 'FCT_01_015_DELTA_INTRADAY_INTENSITY'
    version = '1.0'

    def __init__(self, params = [50, 100, 200, 500]):
        self._params = params
        self._tr = TR()

    def caculate(self, data):
        data = self._tr.enrich(data)
        data[IntradayIntensityFactor.factor_code] = (data['close'] - data['open'])/data[self._tr.get_key()]
        for param in self._params:
            data[self.get_key(param)] = data[IntradayIntensityFactor.factor_code] - data[IntradayIntensityFactor.factor_code].shift(param)
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class PriceVarianceRatioFactor(Factor):
    """
    TSSB PRICE VARIANCE RATIO HistLength Multiplier 因子
    """

    factor_code = 'FCT_01_016_PRICE_VARIANCE_RATIO'
    version = '1.0'

    def __init__(self, params = [50, 100, 200, 500]):
        self._params = params
        self._multiplier = 2
        var_params = list(set((np.array(self._params) * self._multiplier).tolist() + self._params))
        self._var = Variance(var_params, 'log')

    def caculate(self, data):
        data['log'] = data['close'].apply(lambda item: math.log(item))
        data = self._var.enrich(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._var.get_key(param)]/data[self._var.get_key(self._multiplier * param)]
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class MinPriceVarianceRatioFactor(Factor):
    """
    TSSB MIN PRICE VARIANCE RATIO HistLength Multiplier 因子
    """

    factor_code = 'FCT_01_017_MIN_PRICE_VARIANCE_RATIO'
    version = '1.0'

    def __init__(self, params = [50, 100, 200, 500]):
        self._params = params
        self._price_variance_ratio_factor = PriceVarianceRatioFactor(self._params)

    def caculate(self, data):
        data = self._price_variance_ratio_factor.caculate(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._price_variance_ratio_factor.get_key(param)].rolling(param).min()
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data


class MaxPriceVarianceRatioFactor(Factor):
    """
    TSSB MAX PRICE VARIANCE RATIO HistLength Multiplier 因子
    """

    factor_code = 'FCT_01_018_MAX_PRICE_VARIANCE_RATIO'
    version = '1.0'

    def __init__(self, params = [50, 100, 200, 500]):
        self._params = params
        self._price_variance_ratio_factor = PriceVarianceRatioFactor(self._params)

    def caculate(self, data):
        data = self._price_variance_ratio_factor.caculate(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._price_variance_ratio_factor.get_key(param)].rolling(param).max()
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class ChangeVarianceRatioFactor(Factor):
    """
    TSSB CHANGE VARIANCE RATIO HistLength Multiplier 因子
    """

    factor_code = 'FCT_01_019_CHANGE_VARIANCE_RATIO'
    version = '1.0'

    def __init__(self, params = [50, 100, 200, 500]):
        self._params = params
        self._multiplier = 2
        var_params = list(set((np.array(self._params) * self._multiplier).tolist() + self._params))
        self._var = Variance(var_params, 'log')

    def caculate(self, data):
        data['change'] = data['close']/data['close'].shift(1)
        data['log'] = data['change'].apply(lambda item: math.log(item))
        data = self._var.enrich(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._var.get_key(param)]/data[self._var.get_key(self._multiplier * param)]
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class MinChangeVarianceRatioFactor(Factor):
    """
    TSSB MIN CHANGE VARIANCE RATIO HistLength Multiplier 因子
    """

    factor_code = 'FCT_01_020_MIN_CHANGE_VARIANCE_RATIO'
    version = '1.0'

    def __init__(self, params = [50, 100, 200, 500]):
        self._params = params
        self._change_variance_ratio_factor = ChangeVarianceRatioFactor(self._params)

    def caculate(self, data):
        data = self._change_variance_ratio_factor.caculate(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._change_variance_ratio_factor.get_key(param)].rolling(param).min()
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data


class MaxChangeVarianceRatioFactor(Factor):
    """
    TSSB MAX CHANGE VARIANCE RATIO HistLength Multiplier 因子
    """

    factor_code = 'FCT_01_021_MAX_CHANGE_VARIANCE_RATIO'
    version = '1.0'

    def __init__(self, params = [50, 100, 200, 500]):
        self._params = params
        self._change_variance_ratio_factor = ChangeVarianceRatioFactor(self._params)

    def caculate(self, data):
        data = self._change_variance_ratio_factor.caculate(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._change_variance_ratio_factor.get_key(param)].rolling(param).max()
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class AtrRatioFactor(Factor):
    """
    TSSB ATR RATIO HistLength Multiplier 因子
    """

    factor_code = 'FCT_01_022_ATR_RATIO'
    version = '1.0'

    def __init__(self, params = [50, 100, 200, 500]):
        self._params = params
        self._multiplier = 2
        var_params = list(set((np.array(self._params) * self._multiplier).tolist() + self._params))
        self._atr = ATR(var_params)

    def caculate(self, data):
        data = self._atr.enrich(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._atr.get_key(param)]/data[self._atr.get_key(param * self._multiplier)]
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data


class DeltaPriceVarianceRatioFactor(Factor):
    """
    TSSB DELTA PRICE VARIANCE RATIO HistLength Multiplier 因子
    """

    factor_code = 'FCT_01_023_DELTA_PRICE_VARIANCE_RATIO'
    version = '1.0'

    def __init__(self, params = [50, 100, 200, 500]):
        self._params = params
        self._multiplier = 2
        self._price_variance_ratio_factor = PriceVarianceRatioFactor(self._params)

    def caculate(self, data):
        data = self._price_variance_ratio_factor.caculate(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._price_variance_ratio_factor.get_key(param)] - data[self._price_variance_ratio_factor.get_key(param)].shift(param * self._multiplier)
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class DeltaChangeVarianceRatioFactor(Factor):
    """
    TSSB DELTA CHANGE VARIANCE RATIO HistLength Multiplier 因子
    """

    factor_code = 'FCT_01_024_DELTA_CHANGE_VARIANCE_RATIO'
    version = '1.0'

    def __init__(self, params = [50, 100, 200, 500]):
        self._params = params
        self._multiplier = 2
        self._change_variance_ratio_factor = ChangeVarianceRatioFactor(self._params)

    def caculate(self, data):
        data = self._change_variance_ratio_factor.caculate(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._change_variance_ratio_factor.get_key(param)] - data[self._change_variance_ratio_factor.get_key(param)].shift(param * self._multiplier)
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class DeltaAtrRatioFactor(Factor):
    """
    TSSB DELTA ATR RATIO HistLength Multiplier 因子
    """

    factor_code = 'FCT_01_025_DELTA_ATR_RATIO'
    version = '1.0'

    def __init__(self, params = [50, 100, 200, 500]):
        self._params = params
        self._multiplier = 2
        self._atr_ratio_factor = AtrRatioFactor(self._params)

    def caculate(self, data):
        data = self._atr_ratio_factor.caculate(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._atr_ratio_factor.get_key(param)] - data[self._atr_ratio_factor.get_key(param)].shift(param * self._multiplier)
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class PriceSkewnessFactor(Factor):
    """
    TSSB PRICE SKEWNESS HistLength Multiplier 因子
    """

    factor_code = 'FCT_01_026_PRICE_SKEWNESS'
    version = '1.0'

    def __init__(self, params = [50, 100, 200, 500]):
        self._params = params
        self._skewness = Skewness(self._params)

    def caculate(self, data):
        data = self._skewness.enrich(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._skewness.get_key(param)]
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class ChangeSkewnessFactor(Factor):
    """
    TSSB CHANGE SKEWNESS HistLength Multiplier 因子
    """

    factor_code = 'FCT_01_027_CHANGE_SKEWNESS'
    version = '1.0'

    def __init__(self, params = [50, 100, 200, 500]):
        self._params = params
        self._skewness = Skewness(self._params, 'change')

    def caculate(self, data):
        data['change'] = data['close'] - data['close'].shift(1)
        data = self._skewness.enrich(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._skewness.get_key(param)]
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data


class PriceKurtosisFactor(Factor):
    """
    TSSB PRICE KURTOSIS HistLength Multiplier 因子
    """

    factor_code = 'FCT_01_028_PRICE_KURTOSIS'
    version = '1.0'

    def __init__(self, params = [50, 100, 200, 500]):
        self._params = params
        self._kurtosis = Kurtosis(self._params)

    def caculate(self, data):
        data = self._kurtosis.enrich(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._kurtosis.get_key(param)]
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class ChangeKurtosisFactor(Factor):
    """
    TSSB CHANGE KURTOSIS HistLength Multiplier 因子
    """

    factor_code = 'FCT_01_029_CHANGE_KURTOSIS'
    version = '1.0'

    def __init__(self, params = [50, 100, 200, 500]):
        self._params = params
        self._kurtosis = Kurtosis(self._params, 'change')

    def caculate(self, data):
        data['change'] = data['close'] - data['close'].shift(1)
        data = self._kurtosis.enrich(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._kurtosis.get_key(param)]
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
    # linear_per_atr_factor = LinearPerAtrFactor([10])
    # print(linear_per_atr_factor.factor_code)
    # print(linear_per_atr_factor.version)
    # print(linear_per_atr_factor.get_params())
    # print(linear_per_atr_factor.get_category())
    # print(linear_per_atr_factor.get_full_name())
    # print(linear_per_atr_factor.get_key(5))
    # print(linear_per_atr_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # data = linear_per_atr_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=linear_per_atr_factor.get_keys())

    # TSSB LINEAR DEVIATION HistLength
    # linear_deviation_factor = LinearDeviationFactor([10])
    # print(linear_deviation_factor.factor_code)
    # print(linear_deviation_factor.version)
    # print(linear_deviation_factor.get_params())
    # print(linear_deviation_factor.get_category())
    # print(linear_deviation_factor.get_full_name())
    # print(linear_deviation_factor.get_key(5))
    # print(linear_deviation_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # data = linear_deviation_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=linear_deviation_factor.get_keys())

    # TSSB LINEAR DEVIATION HistLength
    # quadratic_deviation_factor = QuadraticDeviationFactor([10])
    # print(quadratic_deviation_factor.factor_code)
    # print(quadratic_deviation_factor.version)
    # print(quadratic_deviation_factor.get_params())
    # print(quadratic_deviation_factor.get_category())
    # print(quadratic_deviation_factor.get_full_name())
    # print(quadratic_deviation_factor.get_key(5))
    # print(quadratic_deviation_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # data = quadratic_deviation_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=quadratic_deviation_factor.get_keys())

    # TSSB LINEAR DEVIATION HistLength
    # cubic_deviation_factor = CubicDeviationFactor([10])
    # print(cubic_deviation_factor.factor_code)
    # print(cubic_deviation_factor.version)
    # print(cubic_deviation_factor.get_params())
    # print(cubic_deviation_factor.get_category())
    # print(cubic_deviation_factor.get_full_name())
    # print(cubic_deviation_factor.get_key(5))
    # print(cubic_deviation_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # data = cubic_deviation_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=cubic_deviation_factor.get_keys())

    # TSSB PRICE MOMENTUM HistLength StdDevLength
    # price_momentum_factor = PriceMomentumFactor([10])
    # print(price_momentum_factor.factor_code)
    # print(price_momentum_factor.version)
    # print(price_momentum_factor.get_params())
    # print(price_momentum_factor.get_category())
    # print(price_momentum_factor.get_full_name())
    # print(price_momentum_factor.get_key(5))
    # print(price_momentum_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = price_momentum_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=price_momentum_factor.get_keys())

    # TSSB ADX HistLength
    # adx_factor = AdxFactor()
    # print(adx_factor.factor_code)
    # print(adx_factor.version)
    # print(adx_factor.get_params())
    # print(adx_factor.get_category())
    # print(adx_factor.get_full_name())
    # print(adx_factor.get_key(5))
    # print(adx_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # data = adx_factor.caculate(data)
    # print(data.iloc[0:50][adx_factor.get_keys()])
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=adx_factor.get_keys())

    # TSSB MIN ADX HistLength MinLength
    # min_adx_factor = MinAdxFactor([20])
    # print(min_adx_factor.factor_code)
    # print(min_adx_factor.version)
    # print(min_adx_factor.get_params())
    # print(min_adx_factor.get_category())
    # print(min_adx_factor.get_full_name())
    # print(min_adx_factor.get_key(5))
    # print(min_adx_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = min_adx_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=min_adx_factor.get_keys())

    # TSSB RESIDUAL MIN ADX HistLength MinLength
    # residual_min_adx_factor = ResidualMinAdxFactor([20])
    # print(residual_min_adx_factor.factor_code)
    # print(residual_min_adx_factor.version)
    # print(residual_min_adx_factor.get_params())
    # print(residual_min_adx_factor.get_category())
    # print(residual_min_adx_factor.get_full_name())
    # print(residual_min_adx_factor.get_key(5))
    # print(residual_min_adx_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = residual_min_adx_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=residual_min_adx_factor.get_keys())

    # TSSB MAX ADX HistLength MaxLength
    # max_adx_factor = MaxAdxFactor([20])
    # print(max_adx_factor.factor_code)
    # print(max_adx_factor.version)
    # print(max_adx_factor.get_params())
    # print(max_adx_factor.get_category())
    # print(max_adx_factor.get_full_name())
    # print(max_adx_factor.get_key(5))
    # print(max_adx_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = max_adx_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=max_adx_factor.get_keys())

    # TSSB RESIDUAL MAX ADX HistLength MaxLength
    # residual_max_adx_factor = ResidualMaxAdxFactor([20])
    # print(residual_max_adx_factor.factor_code)
    # print(residual_max_adx_factor.version)
    # print(residual_max_adx_factor.get_params())
    # print(residual_max_adx_factor.get_category())
    # print(residual_max_adx_factor.get_full_name())
    # print(residual_max_adx_factor.get_key(5))
    # print(residual_max_adx_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = residual_max_adx_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=residual_max_adx_factor.get_keys())

    # TSSB DELTA ADX HistLength DeltaLength
    # delta_adx_factor = DeltaAdxFactor([20])
    # print(delta_adx_factor.factor_code)
    # print(delta_adx_factor.version)
    # print(delta_adx_factor.get_params())
    # print(delta_adx_factor.get_category())
    # print(delta_adx_factor.get_full_name())
    # print(delta_adx_factor.get_key(5))
    # print(delta_adx_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = delta_adx_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=delta_adx_factor.get_keys())

    # TSSB ACCEL ADX HistLength DeltaLength
    # accel_adx_factor = AccelAdxFactor([20])
    # print(accel_adx_factor.factor_code)
    # print(accel_adx_factor.version)
    # print(accel_adx_factor.get_params())
    # print(accel_adx_factor.get_category())
    # print(accel_adx_factor.get_full_name())
    # print(accel_adx_factor.get_key(5))
    # print(accel_adx_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = accel_adx_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=accel_adx_factor.get_keys())

    # TSSB ACCEL ADX HistLength DeltaLength
    # intraday_intensity_factor = IntradayIntensityFactor([20])
    # print(intraday_intensity_factor.factor_code)
    # print(intraday_intensity_factor.version)
    # print(intraday_intensity_factor.get_params())
    # print(intraday_intensity_factor.get_category())
    # print(intraday_intensity_factor.get_full_name())
    # print(intraday_intensity_factor.get_key(5))
    # print(intraday_intensity_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = intraday_intensity_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=intraday_intensity_factor.get_keys())

    # TSSB DELTA INTRADAY INTENSITY HistLength DeltaLength
    # delta_intraday_intensity_factor = DeltaIntradayIntensityFactor([20])
    # print(delta_intraday_intensity_factor.factor_code)
    # print(delta_intraday_intensity_factor.version)
    # print(delta_intraday_intensity_factor.get_params())
    # print(delta_intraday_intensity_factor.get_category())
    # print(delta_intraday_intensity_factor.get_full_name())
    # print(delta_intraday_intensity_factor.get_key(5))
    # print(delta_intraday_intensity_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = delta_intraday_intensity_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=delta_intraday_intensity_factor.get_keys())

    # TSSB PRICE VARIANCE RATIO HistLength Multiplier
    # price_variance_ratio_factor = PriceVarianceRatioFactor([20])
    # print(price_variance_ratio_factor.factor_code)
    # print(price_variance_ratio_factor.version)
    # print(price_variance_ratio_factor.get_params())
    # print(price_variance_ratio_factor.get_category())
    # print(price_variance_ratio_factor.get_full_name())
    # print(price_variance_ratio_factor.get_key(5))
    # print(price_variance_ratio_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = price_variance_ratio_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=price_variance_ratio_factor.get_keys())

    # TSSB MIN PRICE VARIANCE RATIO HistLength Multiplier
    # min_price_variance_ratio_factor = MinPriceVarianceRatioFactor([20])
    # print(min_price_variance_ratio_factor.factor_code)
    # print(min_price_variance_ratio_factor.version)
    # print(min_price_variance_ratio_factor.get_params())
    # print(min_price_variance_ratio_factor.get_category())
    # print(min_price_variance_ratio_factor.get_full_name())
    # print(min_price_variance_ratio_factor.get_key(5))
    # print(min_price_variance_ratio_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = min_price_variance_ratio_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=min_price_variance_ratio_factor.get_keys())

    # TSSB MAX PRICE VARIANCE RATIO HistLength Multiplier
    # max_price_variance_ratio_factor = MaxPriceVarianceRatioFactor([20])
    # print(max_price_variance_ratio_factor.factor_code)
    # print(max_price_variance_ratio_factor.version)
    # print(max_price_variance_ratio_factor.get_params())
    # print(max_price_variance_ratio_factor.get_category())
    # print(max_price_variance_ratio_factor.get_full_name())
    # print(max_price_variance_ratio_factor.get_key(5))
    # print(max_price_variance_ratio_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = max_price_variance_ratio_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=max_price_variance_ratio_factor.get_keys())

    # TSSB CHANGE VARIANCE RATIO HistLength Multiplier
    # change_variance_ratio_factor = ChangeVarianceRatioFactor([20])
    # print(change_variance_ratio_factor.factor_code)
    # print(change_variance_ratio_factor.version)
    # print(change_variance_ratio_factor.get_params())
    # print(change_variance_ratio_factor.get_category())
    # print(change_variance_ratio_factor.get_full_name())
    # print(change_variance_ratio_factor.get_key(5))
    # print(change_variance_ratio_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = change_variance_ratio_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=change_variance_ratio_factor.get_keys())

    # TSSB MIN CHANGE VARIANCE RATIO HistLength Multiplier
    # min_change_variance_ratio_factor = MinChangeVarianceRatioFactor([20])
    # print(min_change_variance_ratio_factor.factor_code)
    # print(min_change_variance_ratio_factor.version)
    # print(min_change_variance_ratio_factor.get_params())
    # print(min_change_variance_ratio_factor.get_category())
    # print(min_change_variance_ratio_factor.get_full_name())
    # print(min_change_variance_ratio_factor.get_key(5))
    # print(min_change_variance_ratio_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = min_change_variance_ratio_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=min_change_variance_ratio_factor.get_keys())

    # TSSB MAX CHANGE VARIANCE RATIO HistLength Multiplier
    # max_change_variance_ratio_factor = MaxChangeVarianceRatioFactor([20])
    # print(max_change_variance_ratio_factor.factor_code)
    # print(max_change_variance_ratio_factor.version)
    # print(max_change_variance_ratio_factor.get_params())
    # print(max_change_variance_ratio_factor.get_category())
    # print(max_change_variance_ratio_factor.get_full_name())
    # print(max_change_variance_ratio_factor.get_key(5))
    # print(max_change_variance_ratio_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = max_change_variance_ratio_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=max_change_variance_ratio_factor.get_keys())

    # TSSB ATR RATIO HistLength Multiplier
    # atr_ratio_factor = AtrRatioFactor([20])
    # print(atr_ratio_factor.factor_code)
    # print(atr_ratio_factor.version)
    # print(atr_ratio_factor.get_params())
    # print(atr_ratio_factor.get_category())
    # print(atr_ratio_factor.get_full_name())
    # print(atr_ratio_factor.get_key(5))
    # print(atr_ratio_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = atr_ratio_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=atr_ratio_factor.get_keys())

    # DELTA PRICE VARIANCE RATIO HistLength Multiplier
    # delta_price_variance_ratio_factor = DeltaPriceVarianceRatioFactor([20])
    # print(delta_price_variance_ratio_factor.factor_code)
    # print(delta_price_variance_ratio_factor.version)
    # print(delta_price_variance_ratio_factor.get_params())
    # print(delta_price_variance_ratio_factor.get_category())
    # print(delta_price_variance_ratio_factor.get_full_name())
    # print(delta_price_variance_ratio_factor.get_key(5))
    # print(delta_price_variance_ratio_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = delta_price_variance_ratio_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=delta_price_variance_ratio_factor.get_keys())

    # DELTA CHANGE VARIANCE RATIO HistLength Multiplier
    # delta_change_variance_ratio_factor = DeltaChangeVarianceRatioFactor([20])
    # print(delta_change_variance_ratio_factor.factor_code)
    # print(delta_change_variance_ratio_factor.version)
    # print(delta_change_variance_ratio_factor.get_params())
    # print(delta_change_variance_ratio_factor.get_category())
    # print(delta_change_variance_ratio_factor.get_full_name())
    # print(delta_change_variance_ratio_factor.get_key(5))
    # print(delta_change_variance_ratio_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = delta_change_variance_ratio_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=delta_change_variance_ratio_factor.get_keys())

    # DELTA ATR RATIO HistLength Multiplier
    # delta_atr_ratio_factor = DeltaAtrRatioFactor([20])
    # print(delta_atr_ratio_factor.factor_code)
    # print(delta_atr_ratio_factor.version)
    # print(delta_atr_ratio_factor.get_params())
    # print(delta_atr_ratio_factor.get_category())
    # print(delta_atr_ratio_factor.get_full_name())
    # print(delta_atr_ratio_factor.get_key(5))
    # print(delta_atr_ratio_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = delta_atr_ratio_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=delta_atr_ratio_factor.get_keys())

    # PRICE SKEWNESS HistLength Multiplier
    # price_skewness_factor = PriceSkewnessFactor([20])
    # print(price_skewness_factor.factor_code)
    # print(price_skewness_factor.version)
    # print(price_skewness_factor.get_params())
    # print(price_skewness_factor.get_category())
    # print(price_skewness_factor.get_full_name())
    # print(price_skewness_factor.get_key(5))
    # print(price_skewness_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = price_skewness_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=price_skewness_factor.get_keys())

    # CHANGE SKEWNESS HistLength Multiplier
    change_skewness_factor = ChangeSkewnessFactor([20])
    print(change_skewness_factor.factor_code)
    print(change_skewness_factor.version)
    print(change_skewness_factor.get_params())
    print(change_skewness_factor.get_category())
    print(change_skewness_factor.get_full_name())
    print(change_skewness_factor.get_key(5))
    print(change_skewness_factor.get_keys())
    # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    print(data.iloc[0:10])
    data = change_skewness_factor.caculate(data)
    data.index = pd.DatetimeIndex(data['datetime'])
    draw_analysis_curve(data, show_signal=True, signal_keys=change_skewness_factor.get_keys())

    # PRICE KURTOSIS HistLength Multiplier
    price_kurtosis_factor = PriceKurtosisFactor([20])
    print(price_kurtosis_factor.factor_code)
    print(price_kurtosis_factor.version)
    print(price_kurtosis_factor.get_params())
    print(price_kurtosis_factor.get_category())
    print(price_kurtosis_factor.get_full_name())
    print(price_kurtosis_factor.get_key(5))
    print(price_kurtosis_factor.get_keys())
    # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    print(data.iloc[0:10])
    data = price_kurtosis_factor.caculate(data)
    data.index = pd.DatetimeIndex(data['datetime'])
    draw_analysis_curve(data, show_signal=True, signal_keys=price_kurtosis_factor.get_keys())

    # CHANGE KURTOSIS HistLength Multiplier
    change_kurtosis_factor = ChangeKurtosisFactor([20])
    print(change_kurtosis_factor.factor_code)
    print(change_kurtosis_factor.version)
    print(change_kurtosis_factor.get_params())
    print(change_kurtosis_factor.get_category())
    print(change_kurtosis_factor.get_full_name())
    print(change_kurtosis_factor.get_key(5))
    print(change_kurtosis_factor.get_keys())
    # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    print(data.iloc[0:10])
    data = change_kurtosis_factor.caculate(data)
    data.index = pd.DatetimeIndex(data['datetime'])
    draw_analysis_curve(data, show_signal=True, signal_keys=change_kurtosis_factor.get_keys())

    # 测试加载数据
    # data = william_factor.load()
    # print(data)





