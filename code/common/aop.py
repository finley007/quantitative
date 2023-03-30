#! /usr/bin/env python
# -*- coding:utf8 -*-
import time
from common.log import get_logger

def timing(func):
    """
    自定义annotation用来给方法的执行时间计时
    Parameters
    ----------
    func

    Returns
    -------

    """
    def fun(*args, **kwargs):
        t = time.perf_counter()
        result = func(*args, **kwargs)
        get_logger().debug(f'Method-{func.__name__} cost time: {time.perf_counter() - t:.8f} s')
        return result
    return fun

def deamon(func):
    """
    该注解表示在实盘计算需要有后台进程更新
    Parameters
    ----------
    func

    Returns
    -------

    """

    def fun(*args, **kwargs):
        result = func(*args, **kwargs)
        return result
    return fun