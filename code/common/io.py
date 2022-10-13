#! /usr/bin/env python
# -*- coding:utf8 -*-
import gzip
import os
import sys
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
sys.path.insert(0,parentdir) 
import common.constants as constants
import _pickle as cPickle
import pandas as pd

def list_files_in_path(path):
    """列出给定目录下的所有文件

    Parameters
    ----------
    path : 给定目录

    Returns
    -------
    list : 文件列表

    """
    return list(map(lambda x: x, list(set(os.listdir(path)))))

def read_decompress(path):
    """读取并解压

    Parameters
    ----------
    path : 给定目录

    Returns
    -------
    data : 文件内容

    """
    with gzip.open(path, 'rb', compresslevel=1) as file_object:
        raw_data = file_object.read()
    return cPickle.loads(raw_data)

def save_compress(data, path):
    """读取并解压

    Parameters
    ----------
    path : 给定目录
    data : 文件内容
    Returns
    -------

    """
    serialized = cPickle.dumps(data)
    with gzip.open(path, 'wb', compresslevel=1) as file_object:
        file_object.write(serialized)

if __name__ == '__main__':
    # print(list_files_in_path(constants.DATA_PATH + 'future/tick/IF/'))
    data = pd.read_csv('/Users/finley/Projects/stock-index-future/data/original/future/tick/IF/CFFEX.IF2212.csv')
    save_compress(data, '/Users/finley/Projects/stock-index-future/data/original/future/tick/IF/CFFEX.IF2212.pkl')

    