#! /usr/bin/env python
# -*- coding:utf8 -*-
from datetime import datetime, timedelta, time
from common.constants import OFF_TIME_IN_SECOND, OFF_TIME_IN_MORNING
from common.exception.exception import InvalidStatus


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



if __name__ == '__main__':
    print(get_full_stockcode('605358'))
    print(get_full_stockcode('300896'))
    print(get_full_stockcode('002985'))
    # print(get_full_stockcode('123456'))

    print(get_full_stockcode_for_stock('605358'))
    print(get_full_stockcode_for_stock('300896'))
    print(get_full_stockcode_for_stock('002985'))
    # print(get_full_stockcode_for_stock('123456'))