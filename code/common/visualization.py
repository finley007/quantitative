#! /usr/bin/env python
# -*- coding:utf8 -*-
import mplfinance as mpf
import matplotlib.pyplot as plt
import pandas as pd
from PIL import Image

from common.localio import read_decompress
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

def draw_histogram(data, bin_num, facecolor='blue', alpha=0.5, save_path=''):
    """画直方图

    :param data: 待分析数据源
    :param bin_num: 直方图立柱的数量
    :param facecolor: 直方图立柱的的颜色
    :param alpha: 透明度
    :return: NoneType

    """
    plt.hist(data, bin_num, facecolor=facecolor, alpha=alpha)
    if save_path != '':
        plt.savefig(save_path)
        plt.close()
    else:
        plt.show()

def draw_line(data, title='', xlabel='', ylabel='', plot_info={'x': 'x', 'y': [{'key': 'y', 'label': ''}]},
              show_grid=False, save_path='', sample_freqency=1):
    """画折线图

    :param data: 待分析数据源
    :type data: Dataframe
    :param title: 标题
    :param xlabel: 横坐标标识
    :param ylabel: 纵坐标标识
    :param plot_info: 数据源解析字典
    :param show_grid: 是否展示网格
    :param save_path: 保存路径
    :param sample_freqency: 数据采样频度 0.1
    :return: NoneType

    """
    plt.style.use('ggplot')
    fig = plt.figure(figsize=(12, 6))
    if sample_freqency != 1:
        sample = int(1/sample_freqency)
        data = data[[i % sample == 0 for i in range(len(data.index))]]
    for y in plot_info.get('y'):
        plt.plot(data[plot_info.get('x')], data[y.get('key')], label=y.get('label'))
    plt.legend()
    plt.grid(show_grid)
    if save_path != '':
        plt.savefig(save_path)
        plt.close()
    else:
        plt.show()


def join_two_images(img_1, img_2, path, flag='horizontal'):
    '''
    拼接连个图像

    Parameters
    ----------
    img_1
    img_2
    path ： 保存位置
    flag ： 横向拼接 vs 纵向拼接

    Returns
    -------

    '''
    img1 = Image.open(img_1)
    img2 = Image.open(img_2)
    size1, size2 = img1.size, img2.size
    if flag == 'horizontal':
        joint = Image.new("RGB", (size1[0] + size2[0], max([size1[1], size2[1]])))
        loc1, loc2 = (0, 0), (size1[0], 0)
        joint.paste(img1, loc1)
        joint.paste(img2, loc2)
        joint.save(path)
    else:
        joint = Image.new("RGB", (max([size1[0], size2[0]]), size1[1] + size2[1]))
        loc1, loc2 = (0, 0), (0, size1[1])
        joint.paste(img1, loc1)
        joint.paste(img2, loc2)
        joint.save(path)


if __name__ == '__main__':
    # data = read_decompress(TEST_PATH + '20200928.pkl')
    # data.index = pd.DatetimeIndex(data['datetime'])
    # print(data)
    # data = data[(data['datetime'] >= '2020-09-28 10:00:00') & (data['datetime'] <= '2020-09-28 10:15:00')]
    # print(data)
    # draw_analysis_curve(data)
    # print('result')

    #测试折线图
    data = {'a': [1, 2, 6, 4, 3], 'b': [2, 3, 4, 5, 6], 'c': [2, 3, 4, 3, 2]}
    df = pd.DataFrame(data)
    draw_line(df, '测试', xlabel='a', ylabel='b', plot_info={'x': 'a', 'y': [{'key': 'b', 'label': 'b'}, {'key': 'c', 'label': 'c'}]})
    data = {'a': ['a', 'b', 'c', 'd', 'e'], 'b': [2, 3, 4, 5, 6], 'c': [2, 3, 4, 3, 2]}
    df = pd.DataFrame(data)
    draw_line(df, '测试', xlabel='a', ylabel='b', plot_info={'x': 'a', 'y': [{'key': 'b', 'label': 'b'}, {'key': 'c', 'label': 'c'}]})
    print(df)