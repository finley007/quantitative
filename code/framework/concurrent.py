#! /usr/bin/env python
# -*- coding:utf8 -*-
import os
import time
from multiprocessing import Pool

from common.aop import timing


class ProcessRunner():

    def __init__(self, pool_size, is_async=True):
        self._pool_size = pool_size
        self._is_async = is_async
        self._ps = Pool(pool_size)

    def execute(self, runner, args=()):
        if self._is_async:
            self._ps.apply_async(runner, args=args)
        else:
            self._ps.apply(runner, args=args)

    def close(self):
        self._ps.close()
        self._ps.join()


def worker(arg1, arg2):
    print(arg1 + '|' + arg2)
    time.sleep(10)

if __name__ == '__main__':
    runner = ProcessRunner(5)
    runner.execute(worker, args=('a', 'b'))
    runner.execute(worker, args=('a', 'b'))
    runner.execute(worker, args=('a', 'b'))
    runner.execute(worker, args=('a', 'b'))
    runner.execute(worker, args=('a', 'b'))
    runner.execute(worker, args=('a', 'b'))
    time.sleep(100)
