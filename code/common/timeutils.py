#! /usr/bin/env python
# -*- coding:utf8 -*-
from datetime import datetime, timedelta, time
import time as tm
from common.constants import OFF_TIME_IN_SECOND, OFF_TIME_IN_MORNING
from common.persistence.dao import IndexConstituentConfigDao
from common.exception.exception import InvalidValue

def time_difference(str_start_time, str_end_time):
    """
    计算两个时间差值

    Parameters
    ----------
    start_time
    end_time

    Returns
    -------

    """
    start_time = datetime.strptime(str_start_time, "%H:%M:%S.%f")
    end_time = datetime.strptime(str_end_time, "%H:%M:%S.%f")
    if end_time < start_time:
        raise InvalidValue('Start time {0} should be large than end time {1}'.format(str_start_time, str_end_time))
    return (end_time - start_time).seconds

def datetime_advance(str_cur_time, step):
    """
    日期时间步进

    Parameters
    ----------
    str_cur_time：string 当前时间
    step：int 步进长度 单位：s

    Returns
    -------

    """
    cur_time = datetime.strptime(str_cur_time[0:21], "%Y-%m-%d %H:%M:%S.%f")
    if cur_time.time() == time.fromisoformat(OFF_TIME_IN_MORNING): # 处理中午停盘的时间
        cur_time = cur_time + timedelta(seconds=step) + timedelta(hours=1.5)
    else:
        cur_time = cur_time + timedelta(seconds=step)
    return datetime.strftime(cur_time, "%Y-%m-%d %H:%M:%S.%f000")


def time_advance(str_cur_time, step):
    """
    时间步进
    Parameters
    ----------
    str_cur_time：string 当前时间
    step：int 步进长度 单位：s

    Returns
    -------

    """
    cur_time = datetime.strptime(str_cur_time, "%H:%M:%S.%f")
    if cur_time.time() == time.fromisoformat(OFF_TIME_IN_MORNING): # 处理中午停盘的时间
        cur_time = cur_time + timedelta(seconds=step) + timedelta(hours=1.5)
    else:
        cur_time = cur_time + timedelta(seconds=step)
    return datetime.strftime(cur_time, "%H:%M:%S") + '.000'


def date_alignment(date):
    """
    毫秒对齐，如果毫秒数再0-4区间内则对齐到0，如果毫秒数在5-9区间内则对齐到5
    Parameters
    ----------
    date：string 待处理时间

    Returns
    -------

    """
    subsec = 0
    if int(date.split('.')[1][0]) > 4:
        subsec = 5
    return date.split('.')[0] + '.' + str(subsec) + '00000000'

def time_carry(hour, minute, second):
    """
    处理时间进位
    Parameters
    ----------
    hour：int 时
    minute：int 分
    second：int 秒

    Returns
    -------

    """
    if second == 60:
        second = 0
        minute = minute + 1
        if minute == 60:
            minute = 0
            hour = hour + 1
    return hour, minute, second

def date_format_transform(date):
    """
    时间格式转换 2022-12-08 <-> 20221208
    Parameters
    ----------
    date：string 待处理时间

    Returns
    -------

    """
    if len(date) == 8:
        return date[0:4] + '-' + date[4:6] + '-' + date[6:8]
    else:
        return date.replace('-','')


def add_milliseconds_suffix(time):
    """
    增加3位毫秒后缀

    Parameters
    ----------
    time：string 待处理时间

    Returns
    -------

    """
    if len(time) == 8:
        return time + '.000'
    else:
        return time

def get_last_or_next_trading_date_by_stock(stock, date, range_num = 1, backword=True, date_list=[]):
    """
    获取下一个或者上一个交易日，考虑停盘

    Parameters
    ----------
    date：string 当前日
    range：int 时间区间
    foward：

    Returns
    -------

    """
    if len(date_list) == 0:
        index_constituent_config_dao = IndexConstituentConfigDao()
        date_list = index_constituent_config_dao.query_trading_date_by_tscode(stock)
    if len(date_list) > 0 and date in date_list:
        index = date_list.index(date)
        for i in range(range_num):
            if backword:
                index = index - 1
                if index < 0:
                    return ''
            else:
                index = index + 1
                if index == len(date_list):
                    return ''
        return date_list[index]
    else:
        return ''

def get_current_time():
    return tm.strftime("%Y%m%d%H%M%S", tm.localtime())

if __name__ == '__main__':
    # print(date_format_transform('20221121'))
    # print(date_format_transform('2022-11-21'))
    # IndexConstituentConfigDao().query_trading_date_by_tscode('002642')
    # print(get_last_or_next_trading_date('20221212', range_num=2, date_list=['20221210','20221211','20221212','20221213','20221214']))
    # print(get_last_or_next_trading_date('20221212', range_num=3, date_list=['20221210','20221211','20221212','20221213','20221214']))
    # print(get_last_or_next_trading_date('20221212', range_num=2, backword=False, date_list=['20221210','20221211','20221212','20221213','20221214']))
    # print(get_last_or_next_trading_date('20221212', range_num=3, backword=False, date_list=['20221210','20221211','20221212','20221213','20221214']))
    # print(time_difference('11:29:57', '11:30:00'))
    print(get_current_time())