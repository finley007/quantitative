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
            data[self.get_key(param)] = data[self._target].ewm(alpha=2 / (param + 1), adjust=False).mean()
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

class ATR(Indicator):
    """
    ATR
    """

    key = 'atr'

    def __init__(self, params):
        self._params = params
        self._ma_indictor = MovingAverage(self._params, 'tr')

    def enrich(self, data):
        # 当日振幅
        data['current_amp'] = data['high'] - data['low']
        # 昨日真实涨幅
        data['last_rise'] = abs(data['high'] - data['close'].shift(1))
        # 昨日真实跌幅
        data['last_fall'] = abs(data['low'] - data['close'].shift(1))
        # 真实波幅
        data['tr'] = data.apply(lambda x: max(x['current_amp'], x['last_rise'], x['last_fall']), axis=1)
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

    def __init__(self, params, target = 'close'):
        self._params = params
        self._target = target

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
        y = np.array(window.tolist()).reshape(-1, 1)
        self.model.fit(x, y)
        return func(self.model, window)

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


if __name__ == '__main__':
    data = read_decompress('/Users/finley/Projects/stock-index-future/data/organised/future/IH/IH2209.pkl')
    data = data[(data['datetime'] >= '2022-01-24 09:30:00') & (data['datetime'] <= '2022-01-24 10:30:00')]
    # data = MovingAverage([10]).enrich(data)
    # print(data)
    #
    # data = ExpMovingAverage([10]).enrich(data)
    # print(data)
    #

    # data = StandardDeviation([10]).enrich(data)
    # print(data)

    # data = ATR([10]).enrich(data)
    # print(data)
    #
    # data = LinearRegression([10]).enrich(data)
    # print(data)

    # data = PolynomialRegression([10]).enrich(data)
    # print(data)

    data = ADX([14]).enrich(data)
    print(data)




