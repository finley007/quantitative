#! /usr/bin/env python
# -*- coding:utf8 -*-
from abc import ABCMeta, abstractmethod

from common.exception.exception import ValidationFailed


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

class StockFilterValidator(Validator):
    """股票过滤数据比较验证

    Parameters
    ----------
    data : DataFrame
        待处理数据.

    """

    @classmethod
    @abstractmethod
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
    @abstractmethod
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
            raise ValidationFailed('Invalid data length')

