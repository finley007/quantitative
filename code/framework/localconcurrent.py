#! /usr/bin/env python
# -*- coding:utf8 -*-
import os
import time
from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor, as_completed

from common.aop import timing


class ProcessRunner():
    """
    异步进程执行框架
    """

    def __init__(self, pool_size, is_async=True):
        print('Init process runner for {0}'.format(pool_size))
        self._pool_size = pool_size
        self._is_async = is_async
        self._ps = Pool(self._pool_size)
        self._results = []

    def execute(self, runner, args=()):
        if self._is_async:
            print('Execute job with async mode')
            self._results.append(self._ps.apply_async(runner, args=args))
        else:
            print('Execute job with sync mode')
            return self._ps.apply(runner, args=args)

    def close(self):
        print('Close process runner pool')
        self._ps.close()
        self._ps.join()

    def get_results(self):
        return self._results

class ThreadRunner():
    """
    异步多线程
    """

    def __init__(self, pool_size):
        print('Init thread runner for {0}'.format(pool_size))
        self._pool_size = pool_size

    def execute(self, task, list):
        with ThreadPoolExecutor(max_workers=self._pool_size) as executor:
            ans = [executor.submit(task, i) for i in list]
            results = []
            for res in as_completed(ans):
                results.append(res.result())
            return results

def worker(arg1, arg2):
    print(arg1 + '|' + arg2)
    time.sleep(10)
    return arg1 + '|' + arg2

def task(arg):
    print(arg)
    time.sleep(10)
    return arg

if __name__ == '__main__':
    # 多进程测试
    # runner = ProcessRunner(5, False)
    # print(runner.execute(worker, args=('a', 'A')))
    # print(runner.execute(worker, args=('b', 'B')))
    # print(runner.execute(worker, args=('c', 'C')))
    # print(runner.execute(worker, args=('d', 'D')))
    # print(runner.execute(worker, args=('e', 'E')))
    # print(runner.execute(worker, args=('f', 'F')))
    # runner.close()

    # runner = ProcessRunner(5)
    # runner.execute(worker, args=('a', 'A'))
    # runner.execute(worker, args=('b', 'B'))
    # runner.execute(worker, args=('c', 'C'))
    # runner.execute(worker, args=('d', 'D'))
    # runner.execute(worker, args=('e', 'E'))
    # runner.execute(worker, args=('f', 'F'))
    # results = runner.get_results()
    # for result in results:
    #     print(result.get())
    # runner.close()

    # 多线程测试
    print(ThreadRunner(5).execute(task, [0,1,2,3,4,5,6,7,8,9]))

