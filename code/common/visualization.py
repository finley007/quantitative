#! /usr/bin/env python
# -*- coding:utf8 -*-
from matplotlib.font_manager import FontProperties
import mplfinance as mpf
import matplotlib.pyplot as plt
import matplotlib
import pandas as pd
import numpy as np

from common.io import read_decompress
from common.constants import TEST_PATH


def draw_analysis_curve(data, type='candle',
                        volume=True,
                        show_nontrading=False,
                        figratio=(20, 10),
                        figscale=1,
                        show_signal=False,
                        signal_keys=[]):
    """打印分析图

    :param data: 待分析数据源，必须包含’Open’, ‘High’, ‘Low’ 和 ‘Close’ 数据（注意:首字母是大写的），
    而且行索引必须是pandas.DatetimeIndex，行索引的名称必须是’Date‘(同理注意首字母大写)
    :type data: DataFrame
    :param type: 指定风格：‘bar’, ‘ohlc’, ‘candle’
    :param volume: 是否展示成交量
    :param show_nontrading: 是否展示非交易天数
    :param figratio: 图像横纵比
    :param add_plot: 绘制额外信息
    :return: NoneType

    Usage::

      >>> from visualization import draw_analysis_curve
      >>> draw_analysis_curve(data)
    """
    mc = mpf.make_marketcolors(up='r', down='g')
    s = mpf.make_mpf_style(marketcolors=mc)
    if (show_signal):
        add_plot = mpf.make_addplot(data[signal_keys])
        mpf.plot(data=data, type=type, volume=volume, figratio=figratio, figscale=figscale,
                 show_nontrading=show_nontrading, style=s, addplot=add_plot)
    else:
        mpf.plot(data=data, type=type, volume=volume, figratio=figratio, figscale=figscale,
                 show_nontrading=show_nontrading, style=s)

if __name__ == '__main__':
    data = read_decompress(TEST_PATH + '20200928.pkl')
    data.index = pd.DatetimeIndex(data['datetime'])
    print(data)
    data = data[(data['datetime'] >= '2020-09-28 10:00:00') & (data['datetime'] <= '2020-09-28 10:15:00')]
    print(data)
    draw_analysis_curve(data)
    print('result')