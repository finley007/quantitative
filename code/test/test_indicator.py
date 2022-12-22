import os

import pytest

import pandas as pd
import numpy as np

from common.localio import read_decompress
from common.visualization import draw_analysis_curve
from factor.indicator import MovingAverage, ExpMovingAverage, WeightedMovingAverage, StandardDeviation, Variance, Skewness, Kurtosis, Median, Quantile, TR, ATR, LinearRegression, RSI, OBV, ADX

"""
用来测试基本算子，
测试文件：%TEST%/indicator_test.csv
"""
TEST_PATH = '/Users/finley/Projects/stock-index-future/code/test/files' + os.path.sep
# TEST_PATH = 'E:\\data\\test\\' + os.path.sep
test_filename = 'indicator_test.csv'

@pytest.fixture()
def init_data():
    columns = ['datetime','open','close','high','low','volume','interest','moving_average','exp_moving_average','weighted_moving_average','weight', 'standard_deviation','variance'
               ,'skewness', 'kurtosis','median','quantile','tr_target','atr','linear_regression']
    data = pd.read_csv(TEST_PATH + test_filename)
    data = data[columns]
    return data


def test_moving_average(init_data):
    data = init_data
    data = MovingAverage([10]).enrich(data)
    assert len(data[np.isnan(data['moving_average.close.10'])]) == 9
    data = data.dropna()
    # data['moving_average_result'] = data['moving_average'] - data['moving_average.close.10']
    # print(data[data['moving_average'] != data['moving_average.close.10']].iloc[0:10][['moving_average', 'moving_average.close.10']])
    # print(len(data[data['moving_average'] != data['moving_average.close.10']]))
    # print(data[data['moving_average'] == data['moving_average.close.10']].iloc[0:10][['moving_average', 'moving_average.close.10']])
    # print(len(data[data['moving_average'] == data['moving_average.close.10']]))
    # print(data[data['moving_average_result'] > 0].iloc[0:10][['moving_average', 'moving_average.close.10','moving_average_result']])
    # print(data.iloc[0:50][['moving_average', 'moving_average.close.10','moving_average_result']])
    # print(len(data[data['moving_average_result'] > 0]))
    # number1 = data.iloc[1]['moving_average']
    # print(type(number1))
    # number2 = data.iloc[1]['moving_average.close.10']
    # print(type(number2))
    assert len(data[np.around(data['moving_average'], 2) != np.around(data['moving_average.close.10'], 2)]) == 0

def test_exp_moving_average(init_data):
    data = init_data
    data = ExpMovingAverage([10]).enrich(data)
    # data = data.dropna()
    # print(data[data['exp_moving_average'] != data['exp_moving_average.close.10']].iloc[0:10][['exp_moving_average', 'exp_moving_average.close.10']])
    # print(data.iloc[0:10][['exp_moving_average', 'exp_moving_average.close.10']])
    assert len(data[np.round(data['exp_moving_average'], 2) != np.round(data['exp_moving_average.close.10'], 2)]) == 0

def test_weighted_moving_average(init_data):
    data = init_data
    data = WeightedMovingAverage([10],weight='weight').enrich(data)
    assert len(data[np.isnan(data['weighted_moving_average.close.weight.10'])]) == 9
    data = data.dropna()
    # print(data[data['weighted_moving_average'] != data['weighted_moving_average.close.weight.10']].iloc[0:10][['weighted_moving_average', 'weighted_moving_average.close.weight.10']])
    assert len(data[np.around(data['weighted_moving_average'], 2) != np.around(data['weighted_moving_average.close.weight.10'], 2)]) == 0

def test_standard_deviation(init_data):
    data = init_data
    data = StandardDeviation([10]).enrich(data)
    assert len(data[np.isnan(data['standard_deviation.close.10'])]) == 9
    data = data.dropna()
    # print(data[data['standard_deviation'] != data['standard_deviation.close.10']].iloc[0:10][['standard_deviation', 'standard_deviation.close.10']])
    assert len(data[np.around(data['standard_deviation'], 2) != np.around(
        data['standard_deviation.close.10'], 2)]) == 0

def test_variance(init_data):
    data = init_data
    data = Variance([10]).enrich(data)
    assert len(data[np.isnan(data['variance.close.10'])]) == 9
    data = data.dropna()
    # print(data[data['variance'] != data['variance.close.10']].iloc[0:10][['variance', 'variance.close.10']])
    assert len(data[np.around(data['variance'], 2) != np.around(
        data['variance.close.10'], 2)]) == 0

def test_skewness(init_data):
    data = init_data
    data = Skewness([10]).enrich(data)
    assert len(data[np.isnan(data['skewness.close.10'])]) == 9
    data = data.dropna()
    # print(data[data['skewness'] != data['skewness.close.10']].iloc[0:10][['skewness', 'skewness.close.10']])
    assert len(data[np.around(data['skewness'], 2) != np.around(
        data['skewness.close.10'], 2)]) == 0

def test_kurtosis(init_data):
    data = init_data
    data = Kurtosis([10]).enrich(data)
    assert len(data[np.isnan(data['kurtosis.close.10'])]) == 9
    data = data.dropna()
    # print(data[data['kurtosis'] != data['kurtosis.close.10']].iloc[0:10][['kurtosis', 'kurtosis.close.10']])
    assert len(data[np.around(data['kurtosis'], 2) != np.around(
        data['kurtosis.close.10'], 2)]) == 0

def test_median(init_data):
    data = init_data
    data = Median([10]).enrich(data)
    assert len(data[np.isnan(data['median.close.10'])]) == 9
    data = data.dropna()
    # print(data[data['median'] != data['median.close.10']].iloc[0:10][['median', 'median.close.10']])
    assert len(data[np.around(data['median'], 2) != np.around(
        data['median.close.10'], 2)]) == 0

def test_quantile(init_data):
    data = init_data
    data = Quantile([10]).enrich(data)
    assert len(data[np.isnan(data['quantile.close.10.0.5'])]) == 9
    data = data.dropna()
    # print(data[data['quantile'] != data['quantile.close.10.0.5']].iloc[0:10][['quantile', 'quantile.close.10.0.5']])
    assert len(data[np.around(data['quantile'], 2) != np.around(
        data['quantile.close.10.0.5'], 2)]) == 0

def test_tr(init_data):
    data = init_data
    data = TR().enrich(data)
    # print(data[data['tr_target'] != data['tr']].iloc[0:10][['tr_target', 'tr']])
    assert len(data[np.around(data['tr_target'], 2) != np.around(
        data['tr'], 2)]) == 0

def test_atr(init_data):
    data = init_data
    data = ATR([10]).enrich(data)
    assert len(data[np.isnan(data['atr.10'])]) == 9
    data = data.dropna()
    # print(data[data['atr'] != data['atr.10']].iloc[0:10][['atr', 'atr.10']])
    assert len(data[np.around(data['atr'], 2) != np.around(
        data['atr.10'], 2)]) == 0

def test_linear_regression(init_data):
    data = init_data
    regression = LinearRegression([10])
    regression.set_caculation_func(caculation_function)
    data = regression.enrich(data)
    assert len(data[np.isnan(data['linear_regression.close.10'])]) == 9
    data = data.dropna()
    # print(data[np.around(data['linear_regression'], 2) != np.around(
    #     data['linear_regression.close.10'], 2)][['linear_regression','linear_regression.close.10']])
    assert len(data[np.around(data['linear_regression'], 2) != np.around(
        data['linear_regression.close.10'], 2)]) == 0

def caculation_function(model, window, variable):
    return model.coef_

def test_polynomial_regression(init_data):
    print('test_polynomial_regression')

def test_adx(init_data):
    """
    中国平安
    20221201 - 20221220
    Parameters
    ----------
    init_data

    Returns
    -------

    """
    data = read_decompress(TEST_PATH + '601318.SH.pkl')
    data['volume'] = data['vol']
    adx = ADX([14])
    data = adx.enrich(data)
    data = data[(data['trade_date'] >= '20221201') & (data['trade_date'] < '20221220')]
    print(data[['trade_date'] + adx.get_keys()])
    data.index = pd.DatetimeIndex(data['trade_date'])
    draw_analysis_curve(data, show_signal=True, signal_keys=adx.get_keys())

def test_obv():
    """
    中国平安
    20221201 - 20221220
    Parameters
    ----------
    init_data

    Returns
    -------

    """
    data = read_decompress(TEST_PATH + '601318.SH.pkl')
    data['volume'] = data['vol']
    obv = OBV()
    data = obv.enrich(data)
    data = data[(data['trade_date'] >= '20221201') & (data['trade_date'] < '20221220')]
    print(data[['trade_date','signed_volume',obv.get_key()]])
    data.index = pd.DatetimeIndex(data['trade_date'])
    draw_analysis_curve(data, show_signal=True, signal_keys=obv.get_key())

def test_rsi():
    """
    中国平安
    20221201 - 20221220
    Parameters
    ----------
    init_data

    Returns
    -------

    """
    data = read_decompress(TEST_PATH + '601318.SH.pkl')
    data['volume'] = data['vol']
    rsi = RSI([24])
    data = rsi.enrich(data)
    first_index = data.head(1).index.tolist()[0]
    data.loc[first_index, 'change'] = 12.99
    data = data[(data['trade_date'] >= '20221201') & (data['trade_date'] < '20221220')]
    print(data[['trade_date','close'] + rsi.get_keys()])
    data.index = pd.DatetimeIndex(data['trade_date'])
    draw_analysis_curve(data, show_signal=True, signal_keys=rsi.get_keys())


if __name__ == '__main__':
   # -s：打印print，-v打印用例执行的详细过程
   pytest.main(["-s","-v","test_indicator.py"])
