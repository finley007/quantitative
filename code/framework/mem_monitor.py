#! /usr/bin/env python
# -*- coding:utf8 -*-
import gc
from memory_profiler import profile

from common.localio import read_decompress


@profile
# code for which memory has to
# be monitored
def my_func():
    x = [x for x in range(0, 1000)]
    y = [y * 100 for y in range(0, 1500)]
    del x
    return y

@profile
def read_data():
    return read_decompress('G:\\data\\model\\xgboost\\input\\data_20230315.pkl')


if __name__ == '__main__':
    for i in range(1, 3, 1):
        data = read_data()
        del data


