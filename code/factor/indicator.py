#! /usr/bin/env python
# -*- coding:utf8 -*-
from abc import ABCMeta, abstractmethod
from sklearn.linear_model import LinearRegression as lr
from sklearn.preprocessing import PolynomialFeatures
import numpy as np

from common.localio import read_decompress


class Indicator(metaclass=ABCMeta):
    """常用指标基类

    Parameters
    ----------
    key : string
    _params ： list
    """
    key = ''
    _params = []

    def get_params(self):
        return self._params

    def get_key(self, param):
        return self.key + '.' + str(param)

    def get_keys(self):
        return list(map(lambda param: self.key + '.' + str(param), self._params))

    @abstractmethod
    def enrich(self, data):
        """
        计算填充指标

        Parameters
        ----------
        data

        Returns
        -------

        """
        pass


class MovingAverage(Indicator):
    """
    移动平均线
    """
    key = 'moving_average'

    def __init__(self, params, target='close'):
        self._params = params
        self._target = target

    def get_key(self, param):
        return self.key + '.' + self._target + '.' + str(param)

    def enrich(self, data):
        for param in self._params:
            data[self.get_key(param)] = data[self._target].rolling(param).mean()
        return data


class WeightedMovingAverage(Indicator):
    """
    加权移动平均线
    """
    key = 'weighted_moving_average'

    def __init__(self, params, target='close', weight='volume'):
        self._params = params
        self._target = target
        self._weight = weight

    def get_key(self, param):
        return self.key + '.' + self._target + '.' + self._weight + '.' + str(param)

    def enrich(self, data):
        simplifier_data = data[[self._weight, self._target]]
        for param in self._params:
            data[self.get_key(param)] = data['close'].rolling(param).apply(lambda item: self.caculate(item, simplifier_data))
        return data

    def caculate(self, window, data):
        #TODO: 这里需要优化
        target = data.loc[window.index, [self._target]]
        weight = data.loc[window.index, [self._weight]]
        result = (target.values * weight.values).sum()/weight.values.sum()
        return result


# 指数移动平均线
class ExpMovingAverage(Indicator):
    """
    指数移动平均线
    """
    key = 'exp_moving_average'

    def __init__(self, params, target='close'):
        self._params = params
        self._target = target

    def get_key(self, param):
        return self.key + '.' + self._target + '.' + str(param)

    def enrich(self, data):
        for param in self._params:
            data[self.get_key(param)] = data[self._target].ewm(span=param, adjust=False).mean()
        return data

class StandardDeviation(Indicator):
    """
    标准差
    """
    key = 'standard_deviation'

    def __init__(self, params, target='close'):
        self._params = params
        self._target = target

    def get_key(self, param):
        return self.key + '.' + self._target + '.' + str(param)

    def enrich(self, data):
        for param in self._params:
            data[self.get_key(param)] = data[self._target].rolling(param).std()
        return data

class Variance(Indicator):
    """
    方差
    """
    key = 'variance'

    def __init__(self, params, target='close'):
        self._params = params
        self._target = target

    def get_key(self, param):
        return self.key + '.' + self._target + '.' + str(param)

    def enrich(self, data):
        for param in self._params:
            data[self.get_key(param)] = data[self._target].rolling(param).var()
        return data

class Skewness(Indicator):
    """
    偏度
    """
    key = 'skewness'

    def __init__(self, params, target='close'):
        self._params = params
        self._target = target

    def get_key(self, param):
        return self.key + '.' + self._target + '.' + str(param)

    def enrich(self, data):
        for param in self._params:
            data[self.get_key(param)] = data[self._target].rolling(param).skew()
        return data

class Kurtosis(Indicator):
    """
    峰度
    """
    key = 'kurtosis'

    def __init__(self, params, target='close'):
        self._params = params
        self._target = target

    def get_key(self, param):
        return self.key + '.' + self._target + '.' + str(param)

    def enrich(self, data):
        for param in self._params:
            data[self.get_key(param)] = data[self._target].rolling(param).kurt()
        return data

class Median(Indicator):
    """
    中位数
    """
    key = 'median'

    def __init__(self, params, target='close'):
        self._params = params
        self._target = target

    def get_key(self, param):
        return self.key + '.' + self._target + '.' + str(param)

    def enrich(self, data):
        for param in self._params:
            data[self.get_key(param)] = data[self._target].rolling(param).median()
        return data

class Quantile(Indicator):
    """
    分位数
    """
    key = 'quantile'

    def __init__(self, params, target='close', proportions=[0.5]):
        self._params = params
        self._target = target
        self._proportions = proportions

    def get_key(self, param, proportion):
        return self.key + '.' + self._target + '.' + str(param) + '.' + str(proportion)

    def enrich(self, data):
        for param in self._params:
            for proportion in self._proportions:
                data[self.get_key(param, proportion)] = data[self._target].rolling(param).quantile(proportion)
        return data

class TR(Indicator):
    """
    TR
    """

    key = 'tr'

    def get_key(self):
        return self.key

    def enrich(self, data):
        # 当日振幅
        data['current_amp'] = data['high'] - data['low']
        # 昨日真实涨幅
        data['last_rise'] = abs(data['high'] - data['close'].shift(1))
        # 昨日真实跌幅
        data['last_fall'] = abs(data['low'] - data['close'].shift(1))
        # 真实波幅
        data[self.get_key()] = data.apply(lambda x: max(x['current_amp'], x['last_rise'], x['last_fall']), axis=1)
        return data

class ATR(Indicator):
    """
    ATR
    """

    key = 'atr'

    def __init__(self, params):
        self._params = params
        self._tr = TR()
        self._ma_indictor = MovingAverage(self._params, self._tr.get_key())

    def enrich(self, data):
        data = self._tr.enrich(data)
        data = self._ma_indictor.enrich(data)
        for param in self._params:
            data[self.get_key(param)] = data[self._ma_indictor.get_key(param)]
        return data

class LinearRegression(Indicator):
    """
    线性回归
    """

    key = 'linear_regression'

    model = lr()

    def __init__(self, params, target = 'close', variable = ''):
        self._params = params
        self._target = target
        #自变量
        self._variable = variable

    def set_caculation_func(self, func):
        self._caculation_func = func

    def get_key(self, param):
        return self.key + '.' + self._target + '.' + str(param)

    def enrich(self, data):
        for param in self._params:
            data[self.get_key(param)] = data[self._target].rolling(param).apply(lambda x: self.regression(x, data, self._caculation_func))
        return data

    def regression(self, window, data, func):
        if '' == self._variable:
            x = np.linspace(0, 1, len(window)).reshape(-1, 1)
        else:
            # TODO: 这里需要优化
            x = data.loc[window.index, [self._variable]]
        y = np.array(window.tolist()).reshape(-1, 1)
        self.model.fit(x, y)
        return func(self.model, window, x)

class PolynomialRegression(Indicator):
    """
    多项式回归
    """

    key = 'polynomial_regression'

    model = lr()

    def __init__(self, params, target = 'close', order = 2):
        self._params = params
        self._target = target
        self.poly = PolynomialFeatures(order)

    def set_caculation_func(self, func):
        self._caculation_func = func

    def get_key(self, param):
        return self.key + '.' + self._target + '.' + str(param)

    def enrich(self, data):
        for param in self._params:
            data[self.get_key(param)] = data[self._target].rolling(param).apply(lambda x: self.regression(x, self._caculation_func))
        return data

    def regression(self, window, func):
        x = np.linspace(0, 1, len(window)).reshape(-1, 1)
        self.poly.fit(x.reshape(-1, 1))
        X = self.poly.transform(x.reshape(-1, 1))
        y = np.array(window.tolist()).reshape(-1, 1)
        self.model.fit(X, y)
        return func(self.model, self.poly, window)

class ADX(Indicator):
    """
    ADX
    """

    key = 'adx'

    def __init__(self, params):
        self._params = params
        self._atr = ATR(self._params)

    def get_key(self, param):
        return self.key + '.' + str(param)

    def enrich(self, data):
        self._atr.enrich(data)
        data['up'] = data['high'] - data['high'].shift(1) #今天和昨天最高价之差
        data['down'] = data['low'].shift(1) - data['low'] #今天和昨天最低价之差
        data.loc[((data['up'] < 0) & (data['down'] < 0)) | (data['up'] == data['down']), 'DM+'] = 0
        data.loc[((data['up'] < 0) & (data['down'] < 0)) | (data['up'] == data['down']), 'DM-'] = 0
        data.loc[(data['up'] > data['down']), 'DM+'] = data['up']
        data.loc[(data['up'] > data['down']), 'DM-'] = 0
        data.loc[(data['up'] < data['down']), 'DM-'] = data['down']
        data.loc[(data['up'] < data['down']), 'DM+'] = 0
        for param in self._params:
            data['DM+.' + str(param)] = data['DM+'].rolling(param).mean()
            data['DM-.' + str(param)] = data['DM-'].rolling(param).mean()
            data['DI+.' + str(param)] = data['DM+.' + str(param)] / data[self._atr.get_key(param)]
            data['DI-.' + str(param)] = data['DM-.' + str(param)] / data[self._atr.get_key(param)]
            data['DI.SUM.' + str(param)] = data['DI+.' + str(param)] + data['DI-.' + str(param)]
            data['DI.SUB.' + str(param)] = abs(data['DI+.' + str(param)] - data['DI-.' + str(param)])
            data['DX.' + str(param)] = (data['DI.SUB.' + str(param)] * 100) / data['DI.SUM.' + str(param)]
            data[self.get_key(param)] = data['DX.' + str(param)].rolling(param).mean()
        return data

class OBV(Indicator):
    """
    OBV
    """

    key = 'obv'

    def get_key(self):
        return self.key

    def enrich(self, data):
        data['last_close'] = data['close'].shift(1)
        data['sign'] = data.apply(lambda x: self.get_trend_indicator(x), axis=1)
        data['signed_volume'] = data['sign'] * data['volume']
        signed_vol = np.array(data['signed_volume'])
        obv = np.cumsum(signed_vol)
        data[self.get_key()] = obv
        return data

    def get_trend_indicator(self, x):
        if (x['close'] > x['last_close']):
            return 1
        if (x['close'] < x['last_close']):
            return -1
        return 0

class RSI(Indicator):
    """
    RSI
    """

    key = 'rsi'

    def __init__(self, params):
        self._params = params

    def get_key(self, param):
        return self.key + '.' + str(param)

    def enrich(self, data):
        data['up'] = 0
        data['down'] = 0
        data['change'] = data['close'] - data['close'].shift(1)
        data.loc[data['change'] > 0, 'up'] = data['change']
        data.loc[data['change'] < 0, 'down'] = -data['change']
        for param in self._params:
            # data['au.' + str(param)] = data['up'].rolling(param).mean()
            # data['ad.' + str(param)] = data['down'].rolling(param).mean()
            data['au.' + str(param)] = data['up'].ewm(span=param, adjust=False).mean()
            data['ad.' + str(param)] = data['down'].ewm(span=param, adjust=False).mean()
            data[self.get_key(param)] = 100 - (100 / (1 + data['au.' + str(param)] / data['ad.' + str(param)]))
        return data


if __name__ == '__main__':
    # data = read_decompress('/Users/finley/Projects/stock-index-future/data/organised/future/IH/IH2209.pkl')
    data = read_decompress('E:\\data\\organized\\future\\tick\\IH\\IH2209.pkl')
    data = data[(data['datetime'] >= '2022-01-24 09:30:00') & (data['datetime'] <= '2022-01-24 10:30:00')]
    # data = MovingAverage([10]).enrich(data)
    # print(data)

    data = ExpMovingAverage([10]).enrich(data)
    print(data)

    # data = WeightedMovingAverage([10]).enrich(data)
    # print(data[['close', 'volume', 'weighted_moving_average.close.volume.10']])

    # data = StandardDeviation([10]).enrich(data)
    # print(data)

    # data = Variance([10]).enrich(data)
    # print(data)

    # data = Skewness([10]).enrich(data)
    # print(data)

    # data = Kurtosis([10]).enrich(data)
    # print(data)

    # data = Median([10]).enrich(data)
    # print(data)
    #
    # data = Quantile([10]).enrich(data)
    # print(data)

    # data = TR().enrich(data)
    # print(data)
    #
    # data = ATR([10]).enrich(data)
    # print(data)

    # data = LinearRegression([10]).enrich(data)
    # print(data)

    # data = PolynomialRegression([10]).enrich(data)
    # print(data)

    # data = ADX([14]).enrich(data)
    # print(data)

    # data = OBV().enrich(data)
    # print(data)

    # data = RSI([14]).enrich(data)
    # print(data)




