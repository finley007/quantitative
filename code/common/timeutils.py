#! /usr/bin/env python
# -*- coding:utf8 -*-
from datetime import datetime, timedelta, time
from common.constants import OFF_TIME_IN_SECOND, OFF_TIME_IN_MORNING

def time_advance(str_cur_time, step):
    #时间步进
    cur_time = datetime.strptime(str_cur_time[0:21], "%Y-%m-%d %H:%M:%S.%f")
    if cur_time.time() == time.fromisoformat(OFF_TIME_IN_MORNING): # 处理中午停盘的时间
        cur_time = cur_time + timedelta(seconds=step) + timedelta(hours=1.5)
    else:
        cur_time = cur_time + timedelta(seconds=step)
    return datetime.strftime(cur_time, "%Y-%m-%d %H:%M:%S.%f000")


def date_alignment(date):
    # 分秒对齐
    subsec = 0
    if int(date.split('.')[1][0]) > 4:
        subsec = 5
    return date.split('.')[0] + '.' + str(subsec) + '00000000'

def time_carry(hour, minute, second):
    # 时间进位
    if second == 60:
        second = 0
        minute = minute + 1
        if minute == 60:
            minute = 0
            hour = hour + 1
    return hour, minute, second

def date_format_transform(date):
    if len(date) == 8:
        return date[0:4] + '-' + date[4:6] + '-' + date[6:8]
    else:
        return date.replace('-','')


def add_milliseconds_suffix(time):
    if len(time) == 8:
        return time + '.000'
    else:
        return time

if __name__ == '__main__':
    print(date_format_transform('20221121'))
    print(date_format_transform('2022-11-21'))