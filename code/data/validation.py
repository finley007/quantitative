#! /usr/bin/env python
# -*- coding:utf8 -*-
import re
from abc import ABCMeta, abstractmethod
import hashlib

import pandas as pd

from common.exception.exception import ValidationFailed
from common.io import read_decompress, list_files_in_path
from tools.tools import extract_tsccode


#和金字塔数据源比对
class Validator(metaclass=ABCMeta):
    """数据验证接口

    Parameters
    ----------
    data : DataFrame
        待处理数据.

    """

    @classmethod
    @abstractmethod
    def validate(self, data):
        """数据验证接口

        Parameters
        ----------
        data : DataFrame
            待处理数据.

        """
        pass

    @classmethod
    @abstractmethod
    def compare_validate(self, target_data, compare_data):
        """数据验证接口

        Parameters
        ----------
        target_data : DataFrame
            待验证数据.
        compare_data : DataFrame
            对比数据.
        """
        pass

class StockValidator(Validator):
    """股票过滤数据比较验证

    Parameters
    ----------
    data : DataFrame
        待处理数据.

    """

    @classmethod
    def validate(self, data):
        """数据验证接口
        1. 检查成交量是否递增
        2. 和通达信数据随机采样比对
        Parameters
        ----------
        data : DataFrame
            待处理数据.

        """
        print(data)

    @classmethod
    def compare_validate(self, target_data, compare_data):
        """数据比较验证接口
        1. 数量
        2. 逐行摘要对比
        Parameters
        ----------
        target_data : DataFrame
            待验证数据.
        compare_data : DataFrame
            对比数据.
        """
        if (len(target_data) != len(compare_data)):
            raise ValidationFailed('Different data length')
        target_sign_list = [hashlib.md5('|'.join(item).encode('gbk')).hexdigest() for item in [str(item) for item in target_data.values.tolist()]]
        compare_sign_list = [hashlib.md5('|'.join(item).encode('gbk')).hexdigest() for item in [str(item) for item in target_data.values.tolist()]]
        diff = list(set(target_sign_list).difference(set(compare_sign_list)))
        diff.extend(list(set(compare_sign_list).difference(set(target_sign_list))))
        if diff != []:
            raise ValidationFailed('Different content')
        return True





if __name__ == '__main__':
    month_folder_path = '/Users/finley/Projects/stock-index-future/data/original/stock_daily/stk_tick10_w_2017/stk_tick10_w_201701/'
    date = '20170103'
    stock_file_list = list_files_in_path(month_folder_path + '/' + date)
    for stock_file in stock_file_list:
        stock = extract_tsccode(stock_file)
        if not re.match('[0-9]{6}', stock):
            continue
        print('Validate for stock %s' % stock)
        data_csv = pd.read_csv(month_folder_path + '/' + date + '/' + stock_file, encoding='gbk')
        try:
            data_pkl = read_decompress(month_folder_path + '/' + date + '/pkl/' +  stock + '.pkl')
        except Exception as e:
            continue
        if StockValidator().compare_validate(data_pkl, data_csv):
            print('Validation pass for stock %s' % stock)

