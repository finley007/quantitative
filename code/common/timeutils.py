#! /usr/bin/env python
# -*- coding:utf8 -*-
from datetime import datetime, timedelta, time
from common.constants import OFF_TIME_IN_SECOND, OFF_TIME_IN_MORNING

def time_advance(str_cur_time, step):
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