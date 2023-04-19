#! /usr/bin/env python
# -*- coding:utf8 -*-
import os
import gzip
import _pickle as cPickle
import pandas as pd
import time

stock_file_path = 'E:\\data\\test\\20200102\\20200102'

def list_files_in_path(path):
    return list(map(lambda x: x, list(set(os.listdir(path)))))

def read_decompress(path):
    with gzip.open(path, 'rb', compresslevel=1) as file_object:
        raw_data = file_object.read()
    return cPickle.loads(raw_data)

@profile
def load_stock_files(path):
    stock_files = list_files_in_path(path)
    print('Total stock: {0}'.format(len(stock_files)))
    stock_files.sort()
    data = pd.DataFrame()
    for stock in stock_files:
        print('Handle {0}'.format(stock))
        data = pd.concat([data, read_decompress(path + os.path.sep + stock)])
    return data

if __name__ == '__main__':
    t = time.perf_counter()
    data = load_stock_files(stock_file_path)
    print(len(data))
    print(data)
    print(f'Cost time: {time.perf_counter() - t:.8f} s')

