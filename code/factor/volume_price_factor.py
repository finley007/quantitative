#! /usr/bin/env python
# -*- coding:utf8 -*-
import math
from math import sqrt

import numpy as np
import pandas as pd

from common.visualization import draw_analysis_curve
from factor.base_factor import Factor
from common.localio import read_decompress
from factor.indicator import ATR, MovingAverage, LinearRegression, PolynomialRegression, StandardDeviation, ADX, TR, \
    Variance, Skewness, Kurtosis, WeightedMovingAverage, Median, Quantile, OBV, RSI

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

    def caculation_function(self, model, window, variable):
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

    def caculation_function(self, model, window, variable):
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
        x = np.linspace(1, len(window), len(window)).reshape(-1, 1)
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
        x = np.linspace(1, len(window), len(window)).reshape(-1, 1)
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
            data[self.get_key(param)] = data[self._adx.get_key(param, self._adx._ext_params[0])]
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class MinAdxFactor(Factor):
    """
    TSSB MIN ADX HistLength MinLength 因子
    """

    factor_code = 'FCT_01_009_MIN_ADX'
    version = '1.0'

    def __init__(self, params = [20]):
        self._params = params
        self._adx = ADX([14])

    def caculate(self, data):
        data = self._adx.enrich(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._adx.get_key(self._adx.get_params()[0], self._adx._ext_params[0])].rolling(param).min()
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class ResidualMinAdxFactor(Factor):
    """
    TSSB RESIDUAL MIN ADX HistLength MinLength 因子
    """

    factor_code = 'FCT_01_010_RESIDUAL_MIN_ADX'
    version = '1.0'

    def __init__(self, params = [20]):
        self._params = params
        self._adx = ADX([14])

    def caculate(self, data):
        data = self._adx.enrich(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._adx.get_key(self._adx.get_params()[0], self._adx._ext_params[0])] - data[self._adx.get_key(self._adx.get_params()[0], self._adx._ext_params[0])].rolling(param).min()
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class MaxAdxFactor(Factor):
    """
    TSSB MAX ADX HistLength MaxLength 因子
    """

    factor_code = 'FCT_01_011_MAX_ADX'
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

    factor_code = 'FCT_01_012_RESIDUAL_MAX_ADX'
    version = '1.0'

    def __init__(self, params = [20]):
        self._params = params
        self._adx = ADX([14])

    def caculate(self, data):
        data = self._adx.enrich(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._adx.get_key(self._adx.get_params()[0])].rolling(param).max() - data[self._adx.get_key(self._adx.get_params()[0])]
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data


class DeltaAdxFactor(Factor):
    """
    TSSB DELTA ADX HistLength DeltaLength 因子
    """

    factor_code = 'FCT_01_013_DELTA_ADX'
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

    factor_code = 'FCT_01_014_ACCEL_ADX'
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

    factor_code = 'FCT_01_015_INTRADAY_INTENSITY'
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

    factor_code = 'FCT_01_016_DELTA_INTRADAY_INTENSITY'
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

    factor_code = 'FCT_01_017_PRICE_VARIANCE_RATIO'
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

    factor_code = 'FCT_01_018_MIN_PRICE_VARIANCE_RATIO'
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

    factor_code = 'FCT_01_019_MAX_PRICE_VARIANCE_RATIO'
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

    factor_code = 'FCT_01_020_CHANGE_VARIANCE_RATIO'
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

    factor_code = 'FCT_01_021_MIN_CHANGE_VARIANCE_RATIO'
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

    factor_code = 'FCT_01_022_MAX_CHANGE_VARIANCE_RATIO'
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

    factor_code = 'FCT_01_023_ATR_RATIO'
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

    factor_code = 'FCT_01_024_DELTA_PRICE_VARIANCE_RATIO'
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

    factor_code = 'FCT_01_025_DELTA_CHANGE_VARIANCE_RATIO'
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

    factor_code = 'FCT_01_026_DELTA_ATR_RATIO'
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

    factor_code = 'FCT_01_027_PRICE_SKEWNESS'
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

    factor_code = 'FCT_01_028_CHANGE_SKEWNESS'
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

    factor_code = 'FCT_01_029_PRICE_KURTOSIS'
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

    factor_code = 'FCT_01_030_CHANGE_KURTOSIS'
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

class DeltaPriceSkewnessFactor(Factor):
    """
    TSSB DELTA PRICE SKEWNESS HistLength Multiplier DeltaLength 因子
    """

    factor_code = 'FCT_01_031_DELTA_PRICE_SKEWNESS'
    version = '1.0'

    def __init__(self, params = [50, 100, 200, 500]):
        self._params = params
        self._price_skewness_factor = PriceSkewnessFactor(self._params)

    def caculate(self, data):
        data = self._price_skewness_factor.caculate(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._price_skewness_factor.get_key(param)] - data[self._price_skewness_factor.get_key(param)].shift(param)
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class DeltaChangeSkewnessFactor(Factor):
    """
    TSSB DELTA CHANGE SKEWNESS HistLength Multiplier DeltaLength 因子
    """

    factor_code = 'FCT_01_032_DELTA_CHANGE_SKEWNESS'
    version = '1.0'

    def __init__(self, params = [50, 100, 200, 500]):
        self._params = params
        self._change_skewness_factor = ChangeSkewnessFactor(self._params)

    def caculate(self, data):
        data = self._change_skewness_factor.caculate(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._change_skewness_factor.get_key(param)] - data[self._change_skewness_factor.get_key(param)].shift(param)
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class DeltaPriceKurtosisFactor(Factor):
    """
    TSSB DELTA PRICE KURTOSIS HistLength Multiplier DeltaLength 因子
    """

    factor_code = 'FCT_01_033_DELTA_PRICE_KURTOSIS'
    version = '1.0'

    def __init__(self, params = [50, 100, 200, 500]):
        self._params = params
        self._price_kurtosis_factor = PriceKurtosisFactor(self._params)

    def caculate(self, data):
        data = self._price_kurtosis_factor.caculate(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._price_kurtosis_factor.get_key(param)] - data[self._price_kurtosis_factor.get_key(param)].shift(param)
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class DeltaChangeKurtosisFactor(Factor):
    """
    TSSB DELTA CHANGE KURTOSIS HistLength Multiplier DeltaLength 因子
    """

    factor_code = 'FCT_01_034_DELTA_CHANGE_KURTOSIS'
    version = '1.0'

    def __init__(self, params = [50, 100, 200, 500]):
        self._params = params
        self._change_kurtosis_factor = ChangeKurtosisFactor(self._params)

    def caculate(self, data):
        data = self._change_kurtosis_factor.caculate(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._change_kurtosis_factor.get_key(param)] - data[self._change_kurtosis_factor.get_key(param)].shift(param)
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data


class VolumeMomentumFactor(Factor):
    """
    TSSB VOLUME MOMENTUM HistLength Multiplier 因子
    """

    factor_code = 'FCT_01_035_VOLUME_MOMENTUM'
    version = '1.0'

    def __init__(self, params = [50, 100, 200, 500]):
        self._params = params
        self._multiplier = 2
        var_params = list(set((np.array(self._params) * self._multiplier).tolist() + self._params))
        self._moving_average = MovingAverage(var_params, 'volume')

    def caculate(self, data):
        data = self._moving_average.enrich(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._moving_average.get_key(param)]/data[self._moving_average.get_key(param * self._multiplier)]
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class DeltaVolumeMomentumFactor(Factor):
    """
    TSSB DELTA VOLUME MOMENTUM HistLen Multiplier DeltaLen 因子
    """

    factor_code = 'FCT_01_036_DELTA_VOLUME_MOMENTUM'
    version = '1.0'

    def __init__(self, params = [50, 100, 200, 500]):
        self._params = params
        self._volume_momentum_factor = VolumeMomentumFactor(self._params)

    def caculate(self, data):
        data = self._volume_momentum_factor.caculate(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._volume_momentum_factor.get_key(param)] - data[self._volume_momentum_factor.get_key(param)].shift(param)
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class VolumeWeightedMaOverMaFactor(Factor):
    """
    TSSB VOLUME WEIGHTED MA OVER MA HistLength 因子
    """

    factor_code = 'FCT_01_037_VOLUME_WEIGHTED_MA_OVER_MA'
    version = '1.0'

    def __init__(self, params = [50, 100, 200, 500]):
        self._params = params
        self._weighted_moving_average = WeightedMovingAverage(self._params)
        self._moving_average = MovingAverage(self._params)

    def caculate(self, data):
        data = self._weighted_moving_average.enrich(data)
        data = self._moving_average.enrich(data)
        for param in self._params:
            data[self.get_key(param)] = data.apply(lambda item:math.log(item[self._weighted_moving_average.get_key(param)]/item[self._moving_average.get_key(param)]), axis=1)
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class DiffVolumeWeightedMaOverMaFactor(Factor):
    """
    TSSB DIFF VOLUME WEIGHTED MA OVER MA ShortDist LongDist 因子
    """

    factor_code = 'FCT_01_038_DIFF_VOLUME_WEIGHTED_MA_OVER_MA'
    version = '1.0'

    def __init__(self, params = [50, 100, 200, 500]):
        self._params = params
        self._multiplier = 2
        var_params = list(set((np.array(self._params) * self._multiplier).tolist() + self._params))
        self._volume_weighted_ma_over_ma_factor = VolumeWeightedMaOverMaFactor(var_params)

    def caculate(self, data):
        data = self._volume_weighted_ma_over_ma_factor.caculate(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._volume_weighted_ma_over_ma_factor.get_key(param)] - data[self._volume_weighted_ma_over_ma_factor.get_key(param * self._multiplier)]
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class PriceVolumeFitFactor(Factor):
    """
    TSSB PRICE VOLUME FIT HistLength 因子
    """

    factor_code = 'FCT_01_039_PRICE_VOLUME_FIT'
    version = '1.0'

    def __init__(self, params = [50, 100, 200, 500]):
        self._params = params
        self._linear_regression = LinearRegression(self._params, 'log_price', 'log_volume')
        self._linear_regression.set_caculation_func(self.caculation_function)

    def caculate(self, data):
        data['log_price'] = data.apply(lambda item: math.log(item['close']), axis = 1)
        data['log_volume'] = data.apply(lambda item: 0 if item['volume'] == 0 else math.log(item['volume']), axis = 1)
        data = self._linear_regression.enrich(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._linear_regression.get_key(param)]
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

    def caculation_function(self, model, window, variable):
        return model.coef_

class DiffPriceVolumeFitFactor(Factor):
    """
    TSSB DIFF PRICE VOLUME FIT ShortDist LongDist 因子
    """

    factor_code = 'FCT_01_040_DIFF_PRICE_VOLUME_FIT'
    version = '1.0'

    def __init__(self, params = [50, 100, 200, 500]):
        self._params = params
        self._multiplier = 2
        self._price_volume_fit_factor = PriceVolumeFitFactor(self._params)

    def caculate(self, data):
        data = self._price_volume_fit_factor.caculate(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._price_volume_fit_factor.get_key(param)] - data[self._price_volume_fit_factor.get_key(param)].shift(self._multiplier * param)
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class DeltaPriceVolumeFitFactor(Factor):
    """
    TSSB DELTA PRICE VOLUME FIT ShortDist LongDist 因子
    """

    factor_code = 'FCT_01_041_DELTA_PRICE_VOLUME_FIT'
    version = '1.0'

    def __init__(self, params = [50, 100, 200, 500]):
        self._params = params
        self._multiplier = 2
        var_params = list(set((np.array(self._params) * self._multiplier).tolist() + self._params))
        self._price_volume_fit_factor = PriceVolumeFitFactor(var_params)

    def caculate(self, data):
        data = self._price_volume_fit_factor.caculate(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._price_volume_fit_factor.get_key(param)] - data[self._price_volume_fit_factor.get_key(self._multiplier * param)]
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class PositiveVolumeIndicatorFactor(Factor):
    """
    TSSB POSITIVE VOLUME INDICATOR HistLength 因子
    """

    factor_code = 'FCT_01_042_POSITIVE_VOLUME_INDICATOR'
    version = '1.0'

    def __init__(self, params = [50, 100, 200, 500]):
        self._params = params
        self._moving_average = MovingAverage(self._params, 'positive_change')
        self._multiplier = 2
        self._std = StandardDeviation(np.array(self._params) * self._multiplier, 'positive_change')

    def caculate(self, data):
        data['change'] = data['close'].shift(1)/data['close']
        data['delta_volume'] = data['volume'] - data['volume'].shift(1)
        data['positive_change'] = 0
        data.loc[data['delta_volume'] > 0, 'positive_change'] = data['change']
        data = self._moving_average.enrich(data)
        data = self._std.enrich(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._moving_average.get_key(param)]/data[self._std.get_key(self._multiplier * param)]
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class DeltaPositiveVolumeIndicatorFactor(Factor):
    """
    TSSB DELTA POSITIVE VOLUME INDICATOR HistLength DeltaDist 因子
    """

    factor_code = 'FCT_01_043_DELTA_POSITIVE_VOLUME_INDICATOR'
    version = '1.0'

    def __init__(self, params = [50, 100, 200, 500]):
        self._params = params
        self._positive_volume_indicator_factor = PositiveVolumeIndicatorFactor(self._params)

    def caculate(self, data):
        data = self._positive_volume_indicator_factor.caculate(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._positive_volume_indicator_factor.get_key(param)] - data[self._positive_volume_indicator_factor.get_key(param)].shift(param)
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data


class NegativeVolumeIndicatorFactor(Factor):
    """
    TSSB NEGATIVE VOLUME INDICATOR HistLength 因子
    """

    factor_code = 'FCT_01_044_NEGATIVE_VOLUME_INDICATOR'
    version = '1.0'

    def __init__(self, params = [50, 100, 200, 500]):
        self._params = params
        self._moving_average = MovingAverage(self._params, 'negative_change')
        self._multiplier = 2
        self._std = StandardDeviation(np.array(self._params) * self._multiplier, 'negative_change')

    def caculate(self, data):
        data['change'] = data['close'].shift(1)/data['close']
        data['delta_volume'] = data['volume'] - data['volume'].shift(1)
        data['negative_change'] = 0
        data.loc[data['delta_volume'] < 0, 'negative_change'] = data['change']
        data = self._moving_average.enrich(data)
        data = self._std.enrich(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._moving_average.get_key(param)]/data[self._std.get_key(self._multiplier * param)]
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class DeltaNegativeVolumeIndicatorFactor(Factor):
    """
    TSSB DELTA NEGATIVE VOLUME INDICATOR HistLen DeltaDist 因子
    """

    factor_code = 'FCT_01_045_DELTA_NEGATIVE_VOLUME_INDICATOR'
    version = '1.0'

    def __init__(self, params = [50, 100, 200, 500]):
        self._params = params
        self._negative_volume_indicator_factor = NegativeVolumeIndicatorFactor(self._params)

    def caculate(self, data):
        data = self._negative_volume_indicator_factor.caculate(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._negative_volume_indicator_factor.get_key(param)] - data[self._negative_volume_indicator_factor.get_key(param)].shift(param)
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class ProductPriceVolumeFactor(Factor):
    """
    TSSB PRODUCT PRICE VOLUME HistLength 因子
    """

    factor_code = 'FCT_01_046_PRODUCT_PRICE_VOLUME'
    version = '1.0'

    def __init__(self, params = [250]):
        self._params = params
        self._volume_median = Median(self._params, 'volume')
        self._log_price_median = Median(self._params, 'log_price')
        self._log_price_quantile = Quantile(self._params, 'log_price', [0.25, 0.75])

    def caculate(self, data):
        data['change_price'] = data['close']/data['close'].shift(1)
        data['log_price'] = data.apply(lambda item: math.log(item['change_price']), axis = 1)
        data = self._volume_median.enrich(data)
        data = self._log_price_median.enrich(data)
        data = self._log_price_quantile.enrich(data)
        for param in self._params:
            data['normalized.volume' + str(param)] = data['volume']/data[self._volume_median.get_key(param)]
            data['normalized.log_price' + str(param)] = (data['log_price'] - data[self._log_price_median.get_key(param)])/(data[self._log_price_quantile.get_key(param, 0.75)] - data[self._log_price_quantile.get_key(param, 0.25)])
            data[self.get_key(param)] = data['normalized.volume' + str(param)] * data['normalized.log_price' + str(param)]
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class SumPriceVolumeFactor(Factor):
    """
    TSSB SUM PRICE VOLUME HistLength 因子
    """

    factor_code = 'FCT_01_047_SUM_PRICE_VOLUME'
    version = '1.0'

    def __init__(self, params = [250]):
        self._params = params
        self._volume_median = Median(self._params, 'volume')
        self._log_price_median = Median(self._params, 'log_price')
        self._log_price_quantile = Quantile(self._params, 'log_price', [0.25, 0.75])

    def caculate(self, data):
        data['change_price'] = data['close']/data['close'].shift(1)
        data['log_price'] = data.apply(lambda item: math.log(item['change_price']), axis = 1)
        data = self._volume_median.enrich(data)
        data = self._log_price_median.enrich(data)
        data = self._log_price_quantile.enrich(data)
        for param in self._params:
            data['normalized.volume' + str(param)] = data['volume']/data[self._volume_median.get_key(param)]
            data['normalized.log_price' + str(param)] = (data['log_price'] - data[self._log_price_median.get_key(param)])/(data[self._log_price_quantile.get_key(param, 0.75)] - data[self._log_price_quantile.get_key(param, 0.25)])
            data.loc[data['normalized.log_price' + str(param)] >= 0, self.get_key(param)] = data['normalized.volume' + str(param)] + data['normalized.log_price' + str(param)]
            data.loc[data['normalized.log_price' + str(param)] < 0, self.get_key(param)] = -(data['normalized.volume' + str(param)] - data['normalized.log_price' + str(param)])
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class DeltaProductPriceVolumeFactor(Factor):
    """
    TSSB DELTA PRODUCT PRICE VOLUME HistLength DeltaDist 因子
    """

    factor_code = 'FCT_01_048_DELTA_PRODUCT_PRICE_VOLUME'
    version = '1.0'

    def __init__(self, params = [250]):
        self._params = params
        self._multiplier = 2
        self._product_price_volume_factor = ProductPriceVolumeFactor(self._params)

    def caculate(self, data):
        data = self._product_price_volume_factor.caculate(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._product_price_volume_factor.get_key(param)] - data[self._product_price_volume_factor.get_key(param)].shift(self._multiplier * param)
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class DeltaSumPriceVolumeFactor(Factor):
    """
    TSSB DELTA SUM PRICE VOLUME HistLength DeltaDist 因子
    """

    factor_code = 'FCT_01_049_DELTA_SUM_PRICE_VOLUME'
    version = '1.0'

    def __init__(self, params = [250]):
        self._params = params
        self._multiplier = 2
        self._sum_price_volume_factor = SumPriceVolumeFactor(self._params)

    def caculate(self, data):
        data = self._sum_price_volume_factor.caculate(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._sum_price_volume_factor.get_key(param)] - data[self._sum_price_volume_factor.get_key(param)].shift(self._multiplier * param)
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class NDayHighFactor(Factor):
    """
    TSSB N DAY HIGH HistLength 因子
    """

    factor_code = 'FCT_01_050_N_DAY_HIGH'
    version = '1.0'

    def __init__(self, params = [50, 100, 200, 500]):
        self._params = params

    def caculate(self, data):
        for param in self._params:
            data[self.get_key(param)] = data['close'].rolling(param).apply(self.n_day_high)
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

    def n_day_high(self, window):
        price_list = window.tolist()
        cur_price = price_list[-1]
        n = len(window) + 1
        for i in range(len(window)):
            print(price_list[-(i+1)])
            if i > 0 and price_list[-(i+1)] > cur_price:
                n = i
                cur_price = price_list[-(i+1)]
        return 100*(n-1)/len(window) - 50


class NDayLowFactor(Factor):
    """
    TSSB N DAY LOW HistLength 因子
    """

    factor_code = 'FCT_01_051_N_DAY_LOW'
    version = '1.0'

    def __init__(self, params = [50, 100, 200, 500]):
        self._params = params

    def caculate(self, data):
        for param in self._params:
            data[self.get_key(param)] = data['close'].rolling(param).apply(self.n_day_low)
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

    def n_day_low(self, window):
        price_list = window.tolist()
        cur_price = price_list[-1]
        n = len(window) + 1
        for i in range(len(window)):
            if i > 0 and price_list[-(i+1)] < cur_price:
                n = i
                price_list[-(i + 1)] = cur_price
        return 100*(n-1)/len(window) - 50

class NDayNarrowFactor(Factor):
    """
    TSSB N DAY NARROWER HistLength 因子
    """

    factor_code = 'FCT_01_052_N_DAY_NARROWER'
    version = '1.0'

    def __init__(self, params = [50, 100, 200, 500]):
        self._params = params
        self._tr = TR()

    def caculate(self, data):
        self._tr.enrich(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._tr.get_key()].rolling(param).apply(self.n_day_narrower)
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

    def n_day_narrower(self, window):
        price_list = window.tolist()
        cur_price = price_list[-1]
        n = len(window) + 1
        for i in range(len(window)):
            if i > 0 and price_list[-(i+1)] < cur_price:
                n = i
                price_list[-(i + 1)] = cur_price
        return 100*(n-1)/len(window) - 50


class NDayWiderFactor(Factor):
    """
    TSSB N DAY WIDER HistLength 因子
    """

    factor_code = 'FCT_01_053_N_DAY_WIDER'
    version = '1.0'

    def __init__(self, params = [50, 100, 200, 500]):
        self._params = params
        self._tr = TR()

    def caculate(self, data):
        self._tr.enrich(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._tr.get_key()].rolling(param).apply(self.n_day_wider)
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

    def n_day_wider(self, window):
        price_list = window.tolist()
        cur_price = price_list[-1]
        n = len(window) + 1
        for i in range(len(window)):
            if i > 0 and price_list[-(i+1)] > cur_price:
                n = i
                price_list[-(i + 1)] = cur_price
        return 100*(n-1)/len(window) - 50


class OnBalanceVolumeFactor(Factor):
    """
    TSSB ON BALANCE VOLUME HistLength 因子
    """

    factor_code = 'FCT_01_054_ON_BALANCE_VOLUME'
    version = '1.0'

    def __init__(self):
        self._obv = OBV()

    def get_key(self):
        return self.factor_code

    def caculate(self, data):
        self._obv.enrich(data)
        data[self.get_key()] = data[self._obv.get_key()]
        data.loc[data[self.get_key()].isnull(), self.get_key()] = 0
        return data

class DeltaOnBalanceVolumeFactor(Factor):
    """
    TSSB DELTA ON BALANCE VOLUME HistLength 因子
    """

    factor_code = 'FCT_01_055_DELTA_ON_BALANCE_VOLUME'
    version = '1.0'

    def __init__(self, params = [50, 100, 200, 500]):
        self._params = params
        self._on_balance_volume_factor = OnBalanceVolumeFactor()

    def get_key(self, param):
        return self.factor_code + '.' + str(param)

    def caculate(self, data):
        self._on_balance_volume_factor.caculate(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._on_balance_volume_factor.get_key()] - data[self._on_balance_volume_factor.get_key()].shift(param)
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class DetrendedRsiFactor(Factor):
    """
    TSSB DETRENDED RSI DetrendedLength DetrenderLength Lookback 因子
    """

    factor_code = 'FCT_01_056_DETRENDED_RSI'
    version = '1.0'

    def __init__(self, params = [50]):
        self._params = params
        self._multiplier = 2
        var_params = list(set((np.array(self._params) * self._multiplier).tolist() + self._params))
        self._rsi = RSI(var_params)
        self._regression_model = {}
        for param in self._params:
            linear_regression = LinearRegression([param], self._rsi.get_key(param), self._rsi.get_key(param * self._multiplier))
            linear_regression.set_caculation_func(self.caculation_function)
            self._regression_model['model.' + str(param)] = linear_regression

    def get_key(self, param):
        return self.factor_code + '.' + str(param)

    def caculate(self, data):
        data = self._rsi.enrich(data)
        #这样做会不会有问题？
        var_params = list(set((np.array(self._params) * self._multiplier).tolist() + self._params))
        for param in var_params:
            data.loc[data[self._rsi.get_key(param)].isnull(), self._rsi.get_key(param)] = 0
        for param in self._params:
            data = self._regression_model['model.' + str(param)].enrich(data)
            key = self._regression_model['model.' + str(param)].get_key(param)
            data[self.get_key(param)] = data[key]
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

    def caculation_function(self, model, window, variable):
        estimated_values = model.predict(variable)
        estimated_value = estimated_values[-1]
        true_value = window.tolist()[-1]
        return true_value - estimated_value

class ThresholdRsiFactor(Factor):
    """
    TSSB THRESHOLDED RSI LookbackLength UpperThresh LowerThresh 因子
    """

    factor_code = 'FCT_01_057_THRESHOLDED_RSI'
    version = '1.0'

    def __init__(self, params = [50, 100, 200, 500], upper = 70, lower = 30):
        self._params = params
        self._rsi = RSI(self._params)
        self._upper = upper
        self._lower = lower

    def get_key(self, param):
        return self.factor_code + '.' + str(param)

    def caculate(self, data):
        data = self._rsi.enrich(data)
        for param in self._params:
            data[self.get_key(param)] = 0
            data.loc[data[self._rsi.get_key(param)] >= self._upper, self.get_key(param)] = 1
            data.loc[data[self._rsi.get_key(param)] <= self._lower, self.get_key(param)] = -1
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

class SupportCloseThreeBollFactor(Factor):
    """
    Support close three boll 因子，来自长青账户
    """

    factor_code = 'FCT_01_058_SUPPORT_CLOSE_THREE_BOLL'
    version = '1.0'

    def __init__(self, params):
        self._params = params
        self._level = 3

    def caculate(self, data):
        for param in self._params:
            data['top.' + str(param)] = data['high'].rolling(param).max()
            data['bottom.' + str(param)] = data['low'].rolling(param).min()
            data['cur.top.' + str(param)] = data['top.' + str(param)].shift(1) # 取昨天的上下边界
            data['cur.bottom.' + str(param)] = data['bottom.' + str(param)].shift(1)
            # 区间宽度
            data['cur.width.' + str(param)] = (data['cur.top.' + str(param)] - data['cur.bottom.' + str(param)])/self._level
            data['mean_price'] = (data['open'] + data['close'] + data['high'] + data['low'])/4
            for i in range(1, self._level + 1):
                data[str(i) + '.level.' + str(param)] = data['cur.bottom.' + str(param)] + data['cur.width.' + str(param)] * i
            for i in range(1, self._level + 1):
                if i == 1:
                    data[str(i) + '.level.price.total.' + str(param)] = data[(data['mean_price'] >= data['cur.bottom.' + str(param)]) & (data['mean_price'] < data[str(i) + '.level.' + str(param)])]['mean_price'].rolling(param).sum()
                    data[str(i) + '.level.volume.total.' + str(param)] = data[(data['mean_price'] >= data['cur.bottom.' + str(param)]) & (data['mean_price'] < data[str(i) + '.level.' + str(param)])]['volume'].rolling(param).sum()
                    data[str(i) + '.level.count.total.' + str(param)] = data[(data['mean_price'] >= data['cur.bottom.' + str(param)]) & (data['mean_price'] < data[str(i) + '.level.' + str(param)])].rolling(param).count()
                else:
                    data[str(i) + '.level.price.total.' + str(param)] = data[(data['mean_price'] >= data[str(i - 1) + '.level.' + str(param)]) & (data['mean_price'] < data[str(i) + '.level.' + str(param)])]['mean_price'].rolling(param).sum()
                    data[str(i) + '.level.volume.total.' + str(param)] = data[(data['mean_price'] >= data[str(i - 1) + '.level.' + str(param)]) & (data['mean_price'] < data[str(i) + '.level.' + str(param)])]['volume'].rolling(param).sum()
                    data[str(i) + '.level.count.total.' + str(param)] = data[(data['mean_price'] >= data[str(i - 1) + '.level.' + str(param)]) & (data['mean_price'] < data[str(i) + '.level.' + str(param)])].rolling(param).count()
            data['support_price.' + str(param)] = data.apply(lambda item: self.get_support_price(item, param), axis = 1)
            data['support_price.std.' + str(param)] = data['support_price.' + str(param)].rolling(param).std()
            data['support_price.top.' + str(param)] = data['support_price.' + str(param)] + data['support_price.std.' + str(param)] * 2
            data['support_price.bottom.' + str(param)] = data['support_price.' + str(param)] - data['support_price.std.' + str(param)] * 2
            data['support_price.witdh.' + str(param)] = data.apply(lambda item: self.get_width(item, param), axis = 1)
            data[self.get_key(param)] = data.apply(lambda item: self.get_key_value(item, param), axis = 1)
            return data

    def get_key_value(self, item, param):
        if abs(item['close'] - item['support_price.' + str(param)])/'support_price.' + str(param) > 0.03:
            return 0
        else:
            (item['close'] - item['support_price.' + str(param)])/data['support_price.witdh.' + str(param)]

    def get_width(self, item, param):
        return max((item['support_price.top.' + str(param)] - item['support_price.bottom.' + str(param)]), item['close'] * 0.004)

    def get_support_price(self, item, param):
        price = 0
        index = 0
        for i in range(1, self._level + 1):
            if price < item[str(i) + '.level.price.total.' + str(param)]:
                price = item[str(i) + '.level.price.total.' + str(param)]
                index = i
        support_price = price/item[str(index) + '.level.count.total.' + str(param)]
        if support_price == 0:
            return item['close']
        else:
            return support_price



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
    # change_skewness_factor = ChangeSkewnessFactor([20])
    # print(change_skewness_factor.factor_code)
    # print(change_skewness_factor.version)
    # print(change_skewness_factor.get_params())
    # print(change_skewness_factor.get_category())
    # print(change_skewness_factor.get_full_name())
    # print(change_skewness_factor.get_key(5))
    # print(change_skewness_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = change_skewness_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=change_skewness_factor.get_keys())

    # PRICE KURTOSIS HistLength Multiplier
    # price_kurtosis_factor = PriceKurtosisFactor([20])
    # print(price_kurtosis_factor.factor_code)
    # print(price_kurtosis_factor.version)
    # print(price_kurtosis_factor.get_params())
    # print(price_kurtosis_factor.get_category())
    # print(price_kurtosis_factor.get_full_name())
    # print(price_kurtosis_factor.get_key(5))
    # print(price_kurtosis_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = price_kurtosis_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=price_kurtosis_factor.get_keys())

    # CHANGE KURTOSIS HistLength Multiplier
    # change_kurtosis_factor = ChangeKurtosisFactor([20])
    # print(change_kurtosis_factor.factor_code)
    # print(change_kurtosis_factor.version)
    # print(change_kurtosis_factor.get_params())
    # print(change_kurtosis_factor.get_category())
    # print(change_kurtosis_factor.get_full_name())
    # print(change_kurtosis_factor.get_key(5))
    # print(change_kurtosis_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = change_kurtosis_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=change_kurtosis_factor.get_keys())

    # DELTA PRICE SKEWNESS HistLength Multiplier DeltaLength
    # delta_price_skewness_factor = DeltaPriceSkewnessFactor([20])
    # print(delta_price_skewness_factor.factor_code)
    # print(delta_price_skewness_factor.version)
    # print(delta_price_skewness_factor.get_params())
    # print(delta_price_skewness_factor.get_category())
    # print(delta_price_skewness_factor.get_full_name())
    # print(delta_price_skewness_factor.get_key(5))
    # print(delta_price_skewness_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = delta_price_skewness_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=delta_price_skewness_factor.get_keys())

    # DELTA CHANGE SKEWNESS HistLength Multiplier DeltaLength
    # delta_change_skewness_factor = DeltaChangeSkewnessFactor([20])
    # print(delta_change_skewness_factor.factor_code)
    # print(delta_change_skewness_factor.version)
    # print(delta_change_skewness_factor.get_params())
    # print(delta_change_skewness_factor.get_category())
    # print(delta_change_skewness_factor.get_full_name())
    # print(delta_change_skewness_factor.get_key(5))
    # print(delta_change_skewness_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = delta_change_skewness_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=delta_change_skewness_factor.get_keys())

    # DELTA PRICE KURTOSIS HistLength Multiplier DeltaLength
    # delta_price_kurtosis_factor = DeltaPriceKurtosisFactor([20])
    # print(delta_price_kurtosis_factor.factor_code)
    # print(delta_price_kurtosis_factor.version)
    # print(delta_price_kurtosis_factor.get_params())
    # print(delta_price_kurtosis_factor.get_category())
    # print(delta_price_kurtosis_factor.get_full_name())
    # print(delta_price_kurtosis_factor.get_key(5))
    # print(delta_price_kurtosis_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = delta_price_kurtosis_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=delta_price_kurtosis_factor.get_keys())

    # DELTA CHANGE KURTOSIS HistLength Multiplier DeltaLength
    # delta_change_kurtosis_factor = DeltaChangeKurtosisFactor([20])
    # print(delta_change_kurtosis_factor.factor_code)
    # print(delta_change_kurtosis_factor.version)
    # print(delta_change_kurtosis_factor.get_params())
    # print(delta_change_kurtosis_factor.get_category())
    # print(delta_change_kurtosis_factor.get_full_name())
    # print(delta_change_kurtosis_factor.get_key(5))
    # print(delta_change_kurtosis_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = delta_change_kurtosis_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=delta_change_kurtosis_factor.get_keys())

    # VOLUME MOMENTUM HistLength Multiplier
    # volume_momentum_factor = VolumeMomentumFactor([20])
    # print(volume_momentum_factor.factor_code)
    # print(volume_momentum_factor.version)
    # print(volume_momentum_factor.get_params())
    # print(volume_momentum_factor.get_category())
    # print(volume_momentum_factor.get_full_name())
    # print(volume_momentum_factor.get_key(5))
    # print(volume_momentum_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = volume_momentum_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=volume_momentum_factor.get_keys())

    # DELTA VOLUME MOMENTUM HistLength Multiplier
    # delta_volume_momentum_factor = DeltaVolumeMomentumFactor([20])
    # print(delta_volume_momentum_factor.factor_code)
    # print(delta_volume_momentum_factor.version)
    # print(delta_volume_momentum_factor.get_params())
    # print(delta_volume_momentum_factor.get_category())
    # print(delta_volume_momentum_factor.get_full_name())
    # print(delta_volume_momentum_factor.get_key(5))
    # print(delta_volume_momentum_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = delta_volume_momentum_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=delta_volume_momentum_factor.get_keys())

    # VOLUME WEIGHTED MA OVER MA HistLength
    # volume_weighted_ma_over_ma_factor = VolumeWeightedMaOverMaFactor([20])
    # print(volume_weighted_ma_over_ma_factor.factor_code)
    # print(volume_weighted_ma_over_ma_factor.version)
    # print(volume_weighted_ma_over_ma_factor.get_params())
    # print(volume_weighted_ma_over_ma_factor.get_category())
    # print(volume_weighted_ma_over_ma_factor.get_full_name())
    # print(volume_weighted_ma_over_ma_factor.get_key(5))
    # print(volume_weighted_ma_over_ma_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = volume_weighted_ma_over_ma_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=volume_weighted_ma_over_ma_factor.get_keys())

    # DIFF VOLUME WEIGHTED MA OVER MA ShortDist LongDist
    # diff_volume_weighted_ma_over_ma_factor = DiffVolumeWeightedMaOverMaFactor([20])
    # print(diff_volume_weighted_ma_over_ma_factor.factor_code)
    # print(diff_volume_weighted_ma_over_ma_factor.version)
    # print(diff_volume_weighted_ma_over_ma_factor.get_params())
    # print(diff_volume_weighted_ma_over_ma_factor.get_category())
    # print(diff_volume_weighted_ma_over_ma_factor.get_full_name())
    # print(diff_volume_weighted_ma_over_ma_factor.get_key(5))
    # print(diff_volume_weighted_ma_over_ma_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = diff_volume_weighted_ma_over_ma_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=diff_volume_weighted_ma_over_ma_factor.get_keys())

    # PRICE VOLUME FIT HistLength
    # price_volume_fit_factor = PriceVolumeFitFactor([20])
    # print(price_volume_fit_factor.factor_code)
    # print(price_volume_fit_factor.version)
    # print(price_volume_fit_factor.get_params())
    # print(price_volume_fit_factor.get_category())
    # print(price_volume_fit_factor.get_full_name())
    # print(price_volume_fit_factor.get_key(5))
    # print(price_volume_fit_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = price_volume_fit_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=price_volume_fit_factor.get_keys())

    # DIFF PRICE VOLUME FIT ShortDist LongDist
    # diff_price_volume_fit_factor = DiffPriceVolumeFitFactor([20])
    # print(diff_price_volume_fit_factor.factor_code)
    # print(diff_price_volume_fit_factor.version)
    # print(diff_price_volume_fit_factor.get_params())
    # print(diff_price_volume_fit_factor.get_category())
    # print(diff_price_volume_fit_factor.get_full_name())
    # print(diff_price_volume_fit_factor.get_key(5))
    # print(diff_price_volume_fit_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = diff_price_volume_fit_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=diff_price_volume_fit_factor.get_keys())

    # DELTA PRICE VOLUME FIT HistLength DeltaDist
    # delta_price_volume_fit_factor = DeltaPriceVolumeFitFactor([20])
    # print(delta_price_volume_fit_factor.factor_code)
    # print(delta_price_volume_fit_factor.version)
    # print(delta_price_volume_fit_factor.get_params())
    # print(delta_price_volume_fit_factor.get_category())
    # print(delta_price_volume_fit_factor.get_full_name())
    # print(delta_price_volume_fit_factor.get_key(5))
    # print(delta_price_volume_fit_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = delta_price_volume_fit_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=delta_price_volume_fit_factor.get_keys())

    # POSITIVE VOLUME INDICATOR HistLength
    # positive_volume_indicator_factor = PositiveVolumeIndicatorFactor([20])
    # print(positive_volume_indicator_factor.factor_code)
    # print(positive_volume_indicator_factor.version)
    # print(positive_volume_indicator_factor.get_params())
    # print(positive_volume_indicator_factor.get_category())
    # print(positive_volume_indicator_factor.get_full_name())
    # print(positive_volume_indicator_factor.get_key(5))
    # print(positive_volume_indicator_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = positive_volume_indicator_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=positive_volume_indicator_factor.get_keys())

    # DELTA POSITIVE VOLUME INDICATOR HistLength DeltaDist
    # delta_positive_volume_indicator_factor = DeltaPositiveVolumeIndicatorFactor([20])
    # print(delta_positive_volume_indicator_factor.factor_code)
    # print(delta_positive_volume_indicator_factor.version)
    # print(delta_positive_volume_indicator_factor.get_params())
    # print(delta_positive_volume_indicator_factor.get_category())
    # print(delta_positive_volume_indicator_factor.get_full_name())
    # print(delta_positive_volume_indicator_factor.get_key(5))
    # print(delta_positive_volume_indicator_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = delta_positive_volume_indicator_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=delta_positive_volume_indicator_factor.get_keys())

    # NEGATIVE VOLUME INDICATOR HistLength
    # negative_volume_indicator_factor = NegativeVolumeIndicatorFactor([20])
    # print(negative_volume_indicator_factor.factor_code)
    # print(negative_volume_indicator_factor.version)
    # print(negative_volume_indicator_factor.get_params())
    # print(negative_volume_indicator_factor.get_category())
    # print(negative_volume_indicator_factor.get_full_name())
    # print(negative_volume_indicator_factor.get_key(5))
    # print(negative_volume_indicator_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = negative_volume_indicator_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=negative_volume_indicator_factor.get_keys())

    # DELTA NEGATIVE VOLUME INDICATOR HistLength DeltaDist
    # delta_negative_volume_indicator_factor = DeltaNegativeVolumeIndicatorFactor([20])
    # print(delta_negative_volume_indicator_factor.factor_code)
    # print(delta_negative_volume_indicator_factor.version)
    # print(delta_negative_volume_indicator_factor.get_params())
    # print(delta_negative_volume_indicator_factor.get_category())
    # print(delta_negative_volume_indicator_factor.get_full_name())
    # print(delta_negative_volume_indicator_factor.get_key(5))
    # print(delta_negative_volume_indicator_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = delta_negative_volume_indicator_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=delta_negative_volume_indicator_factor.get_keys())

    # PRODUCT PRICE VOLUME HistLength
    # product_price_volume_factor = ProductPriceVolumeFactor([20])
    # print(product_price_volume_factor.factor_code)
    # print(product_price_volume_factor.version)
    # print(product_price_volume_factor.get_params())
    # print(product_price_volume_factor.get_category())
    # print(product_price_volume_factor.get_full_name())
    # print(product_price_volume_factor.get_key(5))
    # print(product_price_volume_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = product_price_volume_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=product_price_volume_factor.get_keys())

    # SUM PRICE VOLUME HistLength
    # sum_price_volume_factor = SumPriceVolumeFactor([20])
    # print(sum_price_volume_factor.factor_code)
    # print(sum_price_volume_factor.version)
    # print(sum_price_volume_factor.get_params())
    # print(sum_price_volume_factor.get_category())
    # print(sum_price_volume_factor.get_full_name())
    # print(sum_price_volume_factor.get_key(5))
    # print(sum_price_volume_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = sum_price_volume_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=sum_price_volume_factor.get_keys())

    # DELTA PRODUCT PRICE VOLUME HistLength DeltaDist
    # delta_product_price_volume_factor = DeltaProductPriceVolumeFactor([20])
    # print(delta_product_price_volume_factor.factor_code)
    # print(delta_product_price_volume_factor.version)
    # print(delta_product_price_volume_factor.get_params())
    # print(delta_product_price_volume_factor.get_category())
    # print(delta_product_price_volume_factor.get_full_name())
    # print(delta_product_price_volume_factor.get_key(5))
    # print(delta_product_price_volume_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = delta_product_price_volume_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=delta_product_price_volume_factor.get_keys())

    # DELTA SUM PRICE VOLUME HistLength DeltaDist
    # delta_sum_price_volume_factor = DeltaSumPriceVolumeFactor([20])
    # print(delta_sum_price_volume_factor.factor_code)
    # print(delta_sum_price_volume_factor.version)
    # print(delta_sum_price_volume_factor.get_params())
    # print(delta_sum_price_volume_factor.get_category())
    # print(delta_sum_price_volume_factor.get_full_name())
    # print(delta_sum_price_volume_factor.get_key(5))
    # print(delta_sum_price_volume_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = delta_sum_price_volume_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=delta_sum_price_volume_factor.get_keys())

    # N DAY HIGH HistLength
    # n_day_high_factor = NDayHighFactor([20])
    # print(n_day_high_factor.factor_code)
    # print(n_day_high_factor.version)
    # print(n_day_high_factor.get_params())
    # print(n_day_high_factor.get_category())
    # print(n_day_high_factor.get_full_name())
    # print(n_day_high_factor.get_key(5))
    # print(n_day_high_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = n_day_high_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=n_day_high_factor.get_keys())

    # N DAY LOW HistLength
    # n_day_low_factor = NDayLowFactor([20])
    # print(n_day_low_factor.factor_code)
    # print(n_day_low_factor.version)
    # print(n_day_low_factor.get_params())
    # print(n_day_low_factor.get_category())
    # print(n_day_low_factor.get_full_name())
    # print(n_day_low_factor.get_key(5))
    # print(n_day_low_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = n_day_low_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=n_day_low_factor.get_keys())

    # N DAY NARROWER HistLength
    # n_day_narrow_factor = NDayNarrowFactor([20])
    # print(n_day_narrow_factor.factor_code)
    # print(n_day_narrow_factor.version)
    # print(n_day_narrow_factor.get_params())
    # print(n_day_narrow_factor.get_category())
    # print(n_day_narrow_factor.get_full_name())
    # print(n_day_narrow_factor.get_key(5))
    # print(n_day_narrow_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = n_day_narrow_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=n_day_narrow_factor.get_keys())

    # N DAY WIDER HistLength
    # n_day_wider_factor = NDayWiderFactor([20])
    # print(n_day_wider_factor.factor_code)
    # print(n_day_wider_factor.version)
    # print(n_day_wider_factor.get_params())
    # print(n_day_wider_factor.get_category())
    # print(n_day_wider_factor.get_full_name())
    # print(n_day_wider_factor.get_key(5))
    # print(n_day_wider_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = n_day_wider_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=n_day_wider_factor.get_keys())

    # ON BALANCE VOLUME HistLength
    # on_balance_volume_factor = OnBalanceVolumeFactor()
    # print(on_balance_volume_factor.factor_code)
    # print(on_balance_volume_factor.version)
    # print(on_balance_volume_factor.get_params())
    # print(on_balance_volume_factor.get_category())
    # print(on_balance_volume_factor.get_full_name())
    # print(on_balance_volume_factor.get_key())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = on_balance_volume_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=on_balance_volume_factor.get_keys())

    # DELTA ON BALANCE VOLUME HistLength DeltaDist
    # delta_on_balance_volume_factor = DeltaOnBalanceVolumeFactor()
    # print(delta_on_balance_volume_factor.factor_code)
    # print(delta_on_balance_volume_factor.version)
    # print(delta_on_balance_volume_factor.get_params())
    # print(delta_on_balance_volume_factor.get_category())
    # print(delta_on_balance_volume_factor.get_full_name())
    # print(delta_on_balance_volume_factor.get_key())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = delta_on_balance_volume_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=delta_on_balance_volume_factor.get_keys())

    # DETRENDED RSI DetrendedLength DetrenderLength Lookback
    # detrended_rsi_factor = DetrendedRsiFactor()
    # print(detrended_rsi_factor.factor_code)
    # print(detrended_rsi_factor.version)
    # print(detrended_rsi_factor.get_params())
    # print(detrended_rsi_factor.get_category())
    # print(detrended_rsi_factor.get_full_name())
    # print(detrended_rsi_factor.get_key(5))
    # print(detrended_rsi_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = detrended_rsi_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=detrended_rsi_factor.get_keys())

    # THRESHOLDED RSI LookbackLength UpperThresh LowerThresh
    # threshold_rsi_factor = ThresholdRsiFactor()
    # print(threshold_rsi_factor.factor_code)
    # print(threshold_rsi_factor.version)
    # print(threshold_rsi_factor.get_params())
    # print(threshold_rsi_factor.get_category())
    # print(threshold_rsi_factor.get_full_name())
    # print(threshold_rsi_factor.get_key(5))
    # print(threshold_rsi_factor.get_keys())
    # # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data.iloc[0:10])
    # data = threshold_rsi_factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=threshold_rsi_factor.get_keys())

    # 测试加载数据
    data = william_factor.load()
    print(data)





