#! /usr/bin/env python
# -*- coding:utf8 -*-
from factor.base_factor import Factor, StockTickFactor
from common.constants import TEST_PATH
from common.io import read_decompress

"""现货类因子
分类编号：02
"""
class TotalCommissionRatioFactor(StockTickFactor):
    """总委比因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_001_TOTAL_COMMISSION_RATIO'
    version = '1.0'

    def __init__(self, params = []):
        StockTickFactor.__init__(self)
        self._params = params

    def caculate(self, data):
        return data

if __name__ == '__main__':
    #总委比因子
    total_commision = TotalCommissionRatioFactor([5,10])
    print(total_commision.factor_code)
    print(total_commision.version)
    print(total_commision.get_params())
    print(total_commision.get_category())
    print(total_commision.get_full_name())
    print(total_commision.get_stock_list_by_date('IF', '20220101'))
    print(total_commision.create_stock_tick_data_path('20220101'))







