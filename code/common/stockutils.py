#! /usr/bin/env python
# -*- coding:utf8 -*-
from datetime import datetime, timedelta, time
import os
import pandas as pd

from common.constants import OFF_TIME_IN_SECOND, OFF_TIME_IN_MORNING, STOCK_DATA_PATH
from common.exception.exception import InvalidStatus
from common.timeutils import date_format_transform


def get_full_stockcode(stock_code):
    #获取完整股票代码 688005 -> sh688005
    if stock_code[0] == '6':
        return 'sh' + stock_code
    elif stock_code[0] == '0' or stock_code[0] == '3':
        return 'sz' + stock_code
    else:
        raise InvalidStatus('Unrecognized stock code {0}'.format(stock_code))

def get_full_stockcode_for_stock(stock_code):
    #获取完整股票代码 688005 -> 688005.SH
    if stock_code[0] == '6':
        return stock_code + '.SH'
    elif stock_code[0] == '0' or stock_code[0] == '3':
        return stock_code + '.SZ'
    else:
        raise InvalidStatus('Unrecognized stock code {0}'.format(stock_code))

def get_full_stockcode_for_tdx(stock_code):
    #获取完整股票代码 688005 -> SH#688005.txt
    if stock_code[0] == '6':
        return 'SH#' + stock_code + '.txt'
    elif stock_code[0] == '0' or stock_code[0] == '3':
        return 'SZ#' + stock_code + '.txt'
    else:
        raise InvalidStatus('Unrecognized stock code {0}'.format(stock_code))

def get_full_stockcode_for_duan(stock_code):
    if stock_code[0] == '6':
        return stock_code + '.XSHG'
    elif stock_code[0] == '0' or stock_code[0] == '3':
        return stock_code + '.XSHE'
    else:
        raise InvalidStatus('Unrecognized stock code {0}'.format(stock_code))

def get_rising_falling_limit(stock, date, is_st=False):
    """
    自20200824日起，科创办68xxxx和创业板300xxx股票涨跌幅调整为20%
    Parameters
    ----------
    stock
    date

    Returns
    -------

    """
    if stock[0:2] == '68' or stock[0:3] == '300':
        if date >= '20200824':
            return 0.2
    if stock[0:1] == '8':
        return 0.3
    if not is_st:
        return 0.1
    else:
        return 0.05

def approximately_equal_to(a, b, distance=0.001):
    """
    约等于，用于判断涨跌停
    Parameters
    ----------
    a
    b
    distance

    Returns
    -------

    """
    return abs(a - b) < distance


if __name__ == '__main__':
    # print(get_full_stockcode('605358'))
    # print(get_full_stockcode('300896'))
    # print(get_full_stockcode('002985'))
    # # print(get_full_stockcode('123456'))
    #
    # print(get_full_stockcode_for_stock('605358'))
    # print(get_full_stockcode_for_stock('300896'))
    # print(get_full_stockcode_for_stock('002985'))
    # # print(get_full_stockcode_for_stock('123456'))
    #
    # print(get_rising_falling_limit('300111', '20101201'))
    # print(get_rising_falling_limit('300111', '20200824'))
    # print(get_rising_falling_limit('300111', '20210101'))
    # print(get_rising_falling_limit('688311', '20101201'))
    # print(get_rising_falling_limit('688311', '20200824'))
    # print(get_rising_falling_limit('688311', '20210101'))
    # print(get_rising_falling_limit('600001', '20101201'))
    # print(get_rising_falling_limit('600001', '20200824'))
    # print(get_rising_falling_limit('600001', '20210101'))
    #
    # print(approximately_equal_to(0.1, 0.09))
    # print(approximately_equal_to(0.1, 0.0982))

    print(is_st('20180601','002122'))