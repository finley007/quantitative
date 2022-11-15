#! /usr/bin/env python
# -*- coding:utf8 -*-
from abc import ABCMeta, abstractmethod

from scipy.stats import pearsonr


class Factor(metaclass=ABCMeta):
    """因子基类

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = ''
    version = '1.0'

    _params = []

    def get_params(self):
        return self._params

    def get_full_name(self):
        return self.factor_code + '-' + self.version + '[' \
               + (','.join(list(map(lambda x: str(x), self._params)))) + ']'

    def get_category(self):
        return self.factor_code.split('_')[1]

    # 两个因子相关性分析
    def compare(self, factor, data):
        data = self.caculate(data)
        data = factor.caculate(data)
        data = data.dropna()
        return pearsonr(data[self.factor_code], data[factor.factor_code])

    # 全局计算因子值
    @abstractmethod
    def caculate(self, data):
        pass

class StockTickFactor(Factor):
    """股票Tick因子基类，可以加载股票Tick数据

    Parameters
    ----------
    """

    @classmethod
    def get_stock_tick_data(self, data):
        return data

    # 全局计算因子值
    @abstractmethod
    def caculate(self, data):
        pass