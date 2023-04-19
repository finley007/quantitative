#! /usr/bin/env python
# -*- coding:utf8 -*-
import os
import time

from multiprocessing import Pool, Manager
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
from common.aop import timing
from common.localio import read_decompress

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
            # print('Execute job with async mode')
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

class ProcessExcecutor():
    """
    异步进程执行框架2
    """

    def __init__(self, pool_size, is_async=True):
        print('Init process executor for {0}'.format(pool_size))
        self._pool_size = pool_size

    def execute(self, task, list):
        with ProcessPoolExecutor(max_workers=self._pool_size) as executor:
            ans = [executor.submit(task, i) for i in list]
            results = []
            for res in as_completed(ans):
                results.append(res.result())
            return results


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

global global_dict
def task(arg):
    print(arg)
    time.sleep(10)
    return arg

def df_task(ns):
    print(ns.data)


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
    #
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

    # global_dict = {1:'a',2:'b',3:'b',4:'d',5:'e',6:'f',7:'g',8:'h',9:'i',0:'j'}
    # param_list = [0,1,2,3,4,5,6,7,8,9]
    # param_list = list(map(lambda item: {item, global_dict}, param_list))
    # print(ProcessExcecutor(5).execute(task, param_list))

    # 多线程测试
    # print(ThreadRunner(5).execute(task, [0,1,2,3,4,5,6,7,8,9]))

    data = read_decompress('E:\\data\\organized\\stock\\tick\\stk_tick10_w_2020\\stk_tick10_w_202001\\20200102\\000001.pkl')
    mgr = Manager()
    ns = mgr.Namespace()
    ns.data = data
    runner = ProcessRunner(5, False)
    print(runner.execute(df_task, args=(ns,)))
    print(runner.execute(df_task, args=(ns,)))
    print(runner.execute(df_task, args=(ns,)))
    print(runner.execute(df_task, args=(ns,)))
    print(runner.execute(df_task, args=(ns,)))
    print(runner.execute(df_task, args=(ns,)))
    print(runner.execute(df_task, args=(ns,)))
    runner.close()

