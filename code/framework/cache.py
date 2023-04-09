#! /usr/bin/env python
# -*- coding:utf8 -*-
from functools import lru_cache
import time

from framework.localconcurrent import ProcessExcecutor

@lru_cache
def my_func(num):
    print('start to call my function')
    time.sleep(5)
    print('complete calling my function')
    return 'done'


if __name__ == '__main__':
    #单进程
    # for i in range(1, 5, 1):
    #     print(my_func())

    #多进程下缓存无效
    results = ProcessExcecutor(2).execute(my_func, list(range(1, 6, 1)))
    for result in results:
        print(result)



