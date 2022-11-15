#! /usr/bin/env python
# -*- coding:utf8 -*-
from factor.base_factor import Factor
from common.constants import TEST_PATH
from common.io import read_decompress

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
        data[self.factor_code] = (data['close'] - data['close'].rolling(window=self._params[0]).min())/(data['close'].rolling(window=self._params[0]).max()-data['close'].rolling(window=self._params[0]).min())
        return data

if __name__ == '__main__':
    #测试威廉因子
    william_factor = WilliamFactor([5,10])
    print(william_factor.factor_code)
    print(william_factor.version)
    print(william_factor.get_params())
    print(william_factor.get_category())
    print(william_factor.get_full_name())

    data = read_decompress(TEST_PATH + 'IF2203.pkl')
    print(data)
    print(william_factor.caculate(data)[0:50][['close','FCT_01_001_WILLIAM']])






