#! /usr/bin/env python
# -*- coding:utf8 -*-
import pandas as pd
from datetime import datetime

from factor.base_factor import Factor, StockTickFactor
from common.constants import TEST_PATH, STOCK_TRANSACTION_START_TIME
from common.io import read_decompress
from common.aop import timing

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

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['weighted_average_bid_price', 'weighted_average_ask_price', 'total_bid_volume', 'total_ask_volume']
        return columns

    @timing
    def caculate(self, data):
        columns = data.columns.tolist() + [TotalCommissionRatioFactor.factor_code, 'time', 'second_remainder']
        new_data = pd.DataFrame(columns = columns)
        product = data.iloc[0]['product']
        date_list = list(set(data['date'].tolist()))
        date_list.sort()
        for date in date_list:
            stock_data_per_date = self.get_stock_tick_data(product, date)
            stock_data_per_date = stock_data_per_date[stock_data_per_date['time'] > STOCK_TRANSACTION_START_TIME]
            stock_data_per_date['ask_commission_amount'] = stock_data_per_date['total_ask_volume'] * stock_data_per_date['weighted_average_ask_price']
            stock_data_per_date['bid_commission_amount'] = stock_data_per_date['total_bid_volume'] * stock_data_per_date['weighted_average_bid_price']
            stock_data_per_date['total_commission_amount'] = stock_data_per_date['ask_commission_amount'] + stock_data_per_date['bid_commission_amount']
            stock_data_per_date[TotalCommissionRatioFactor.factor_code] = stock_data_per_date.apply(lambda x: 0 if x['total_commission_amount'] == 0 else x['ask_commission_amount']/x['total_commission_amount'], axis=1)
            stock_data_per_date = stock_data_per_date.rename(columns={'time':'cur_time'})
            stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')[TotalCommissionRatioFactor.factor_code].sum()
            df_stock_data_per_date = pd.DataFrame({TotalCommissionRatioFactor.factor_code : stock_data_per_date_group_by, 'time' : stock_data_per_date_group_by.index})
            #过滤对齐在3秒线的数据
            df_stock_data_per_date['second_remainder'] = df_stock_data_per_date.apply(lambda item: self.is_aligned(item), axis = 1)
            df_stock_data_per_date = df_stock_data_per_date[df_stock_data_per_date['second_remainder'] == 0]
            df_stock_data_per_date['datetime'] = date + ' ' + df_stock_data_per_date['time'] + '000000'

            cur_date_data = data[data['date'] == date]
            cur_date_data = cur_date_data.merge(df_stock_data_per_date, on=['datetime'], how='left')
            new_data = pd.concat([new_data, cur_date_data])
        return new_data

    def is_aligned(self, item):
        '''过滤对齐到3秒线

        Parameters
        ----------
        item

        Returns
        -------

        '''
        cur_time = datetime.strptime(item['time'], '%H:%M:%S.%f')
        return cur_time.second % 3

if __name__ == '__main__':
    #总委比因子
    total_commision = TotalCommissionRatioFactor([5,10])
    # print(total_commision.factor_code)
    # print(total_commision.version)
    # print(total_commision.get_params())
    # print(total_commision.get_category())
    # print(total_commision.get_full_name())
    # print(total_commision.get_stock_list_by_date('IH', '20210719'))
    # print(total_commision.create_stock_tick_data_path('20220101'))

    data = read_decompress(TEST_PATH + '20200928.pkl')
    data['product'] = 'IF'
    data['date'] = data['datetime'].str[0:10]

    print(data)
    print(TotalCommissionRatioFactor().caculate(data))
    data.index = pd.DatetimeIndex(data['datetime'])
    data = data[(data['datetime'] >= '2020-09-28 10:00:00') & (data['datetime'] <= '2020-09-28 10:30:00')]
    draw_analysis_curve(data, show_signal=True, signal_keys=william_factor.get_keys())







