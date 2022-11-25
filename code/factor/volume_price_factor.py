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
            data[self.get_key(param)] = (data['close'] - (data['high'].rolling(window=param).max()+data['low'].rolling(window=param).min())/2)/(data['high'].rolling(window=param).max()-data['low'].rolling(window=param).min())
            data.loc[data[self.get_key(param)].isnull(), self.get_key(param)] = 0
        return data

if __name__ == '__main__':
    #测试威廉因子
    william_factor = WilliamFactor([10])
    print(william_factor.factor_code)
    print(william_factor.version)
    print(william_factor.get_params())
    print(william_factor.get_category())
    print(william_factor.get_full_name())
    print(william_factor.get_key(5))
    print(william_factor.get_keys())

    data = read_decompress(TEST_PATH + 'IC2003.pkl')
    data = william_factor.caculate(data)
    print(data.loc[776842:776844])
    # data = data[(data['datetime'] >= '2019-08-28 13:45:00') & (data['datetime'] <= '2019-08-28 13:48:00')]
    # print(data[['close', 'datetime'] + william_factor.get_keys()])
    data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=william_factor.get_keys())

    # 测试加载数据
    # data = william_factor.load()
    # print(data)





