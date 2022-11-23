#! /usr/bin/env python
# -*- coding:utf8 -*-
import pandas as pd

from factor.base_factor import Factor
from common.constants import TEST_PATH
from common.localio import read_decompress
from common.visualization import draw_analysis_curve

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
            data[self.get_key(param)] = (data['close'] - data['close'].rolling(window=param).min())/(data['close'].rolling(window=param).max()-data['close'].rolling(window=param).min())
        return data

if __name__ == '__main__':
    #测试威廉因子
    william_factor = WilliamFactor([1000])
    print(william_factor.factor_code)
    print(william_factor.version)
    print(william_factor.get_params())
    print(william_factor.get_category())
    print(william_factor.get_full_name())
    print(william_factor.get_key(5))
    print(william_factor.get_keys())

    # data = read_decompress(TEST_PATH + '20200928.pkl')
    # print(william_factor.caculate(data)[0:50][['close'] + william_factor.get_keys()])
    # data.index = pd.DatetimeIndex(data['datetime'])
    # data = data[(data['datetime'] >= '2020-09-28 10:00:00') & (data['datetime'] <= '2020-09-28 10:30:00')]
    # draw_analysis_curve(data, show_signal=True, signal_keys=william_factor.get_keys())

    data = william_factor.load()
    print(data)





