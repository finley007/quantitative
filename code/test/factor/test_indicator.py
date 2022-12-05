import os

import pytest

import pandas as pd
from factor.indicator import MovingAverage

"""
用来测试基本算子，
测试文件：%TEST%/indicator_test.csv
"""
TEST_PATH = 'E:\\data\\' + 'test' + os.path.sep
test_filename = 'indicator_test.csv'

@pytest.fixture()
def init_data():
    global data
    return pd.read_csv(TEST_PATH + test_filename)


def test_moving_average(init_data):
    data = init_data
    data = MovingAverage([10]).enrich(data)
    print(data)

def test_exp_moving_average():
    print('test_exp_moving_average')

def test_weighted_moving_average():
    print('test_weighted_moving_average')

def test_standard_deviation():
    print('test_standard_deviation')

def test_variance():
    print('test_variance')

def test_skewness():
    print('test_skewness')

def test_kurtosis():
    print('test_kurtosis')

def test_median():
    print('test_median')

def test_quantile():
    print('test_quantile')

def test_tr():
    print('test_tr')

def test_atr():
    print('test_atr')

def test_linear_regression():
    print('test_linear_regression')

def test_polynomial_regression():
    print('test_polynomial_regression')

def test_adx():
    print('test_adx')

def test_obv():
    print('test_obv')

def test_rsi():
    print('test_rsi')


if __name__ == '__main__':
   # -s：打印print，-v打印用例执行的详细过程
   pytest.main(["-s","-v","test_indicator.py"])
