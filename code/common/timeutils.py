#! /usr/bin/env python
# -*- coding:utf8 -*-
from datetime import datetime, timedelta, time
from common.constants import OFF_TIME_IN_SECOND, OFF_TIME_IN_MORNING

def time_advance(str_cur_time, step):
    cur_time = datetime.strptime(str_cur_time[0:21], "%Y-%m-%d %H:%M:%S.%f")
    next_time = cur_time + timedelta(seconds=step)
    if next_time.time() > time.fromisoformat(OFF_TIME_IN_MORNING):  # 处理中午停盘的时间
        cur_time = cur_time + timedelta(hours=1.5)
    return datetime.strftime(next_time, "%Y-%m-%d %H:%M:%S.%f000")