#! /usr/bin/env python
# -*- coding:utf8 -*-
import time


def timing(func):
    def fun(*args, **kwargs):
        t = time.perf_counter()
        result = func(*args, **kwargs)
        print(f'Cost time: {time.perf_counter() - t:.8f} s')
        return result
    return fun