import pytest
import time
import asyncio
import time

from concurrent.futures import ProcessPoolExecutor
from framework.localconcurrent import ProcessRunner, ProcessExcecutor
from framework.pagination import Pagination



data = {
    'IH2212': {
        "20220808" : ['000001','000002','000003','000004','000005', '000006','000007','000008','000009','000010'],
        "20220809" : ['000001','000002','000003','000004','000005', '000006','000007','000008','000009','000010'],
        "20220810" : ['000001','000002','000003','000004','000005', '000006','000007','000008','000009','000010'],
        "20220811" : ['000001','000002','000003','000004','000005', '000006','000007','000008','000009','000010'],
        "20220812" : ['000001','000002','000003','000004','000005', '000006','000007','000008','000009','000010'],
        "20220813" : ['000001','000002','000003','000004','000005', '000006','000007','000008','000009','000010'],
        "20220814" : ['000001','000002','000003','000004','000005', '000006','000007','000008','000009','000010']
    },
    'IC2212': {
        "20220808" : ['000001','000002','000003','000004','000005', '000006','000007','000008','000009','000010'],
        "20220809" : ['000001','000002','000003','000004','000005', '000006','000007','000008','000009','000010'],
        "20220810" : ['000001','000002','000003','000004','000005', '000006','000007','000008','000009','000010'],
        "20220811" : ['000001','000002','000003','000004','000005', '000006','000007','000008','000009','000010'],
        "20220812" : ['000001','000002','000003','000004','000005', '000006','000007','000008','000009','000010'],
        "20220813" : ['000001','000002','000003','000004','000005', '000006','000007','000008','000009','000010'],
        "20220814" : ['000001','000002','000003','000004','000005', '000006','000007','000008','000009','000010']
    },
    'IF2212': {
        "20220808" : ['000001','000002','000003','000004','000005', '000006','000007','000008','000009','000010'],
        "20220809" : ['000001','000002','000003','000004','000005', '000006','000007','000008','000009','000010'],
        "20220810" : ['000001','000002','000003','000004','000005', '000006','000007','000008','000009','000010'],
        "20220811" : ['000001','000002','000003','000004','000005', '000006','000007','000008','000009','000010'],
        "20220812" : ['000001','000002','000003','000004','000005', '000006','000007','000008','000009','000010'],
        "20220813" : ['000001','000002','000003','000004','000005', '000006','000007','000008','000009','000010'],
        "20220814" : ['000001','000002','000003','000004','000005', '000006','000007','000008','000009','000010']
    }
}

# def test_run_process_sequence():
#     t = time.perf_counter()
#     for key in data.keys():
#         handle_instrument(key)
#     print(f'cost time: {time.perf_counter() - t:.8f} s')


# def test_run_process_1_level_parallel():
#     t = time.perf_counter()
#     runner = ProcessRunner(5)
#     for key in data.keys():
#         runner.execute(handle_instrument, args={key})
#     results = runner.get_results()
#     for result in results:
#         print(result.get())
#     runner.close()
#     print(f'cost time: {time.perf_counter() - t:.8f} s')


# def test_run_process_2_levels_parallel():
#     t = time.perf_counter()
#     ProcessExcecutor(5).execute(batch_handle_instrument, ['IH2212', 'IC2212', 'IF2212'])
#     print(f'cost time: {time.perf_counter() - t:.8f} s')

def handle_instrument(instrument):
    date_list = data[instrument]
    for date in date_list:
        handle_date(instrument, date)

def batch_handle_instrument(instrument):
    date_list = data[instrument]
    date_list = list(map(lambda date : [instrument, date], date_list))
    print(date_list)
    ProcessExcecutor(5).execute(handle_date, date_list)

def handle_date(*arg):
    if len(arg) == 2:
        instrument = arg[0]
        date = arg[1]
    else:
        instrument = arg[0][0]
        date = arg[0][1]
    stock_list = data[instrument][date]
    for stock in stock_list:
        print(instrument + '_' + date + '_' + stock)
    time.sleep(1)

async def async_function(num):  # async修饰的异步函数，在该函数中可以添加await进行暂停并切换到其他异步函数中
    now_time = time.time()
    await asyncio.sleep(num)  # 当执行await future这行代码时（future对象就是被await修饰的函数），首先future检查它自身是否已经完成，如果没有完成，挂起自身，告知当前的Task（任务）等待future完成。
    return '协程花费时间：{}秒'.format(time.time() - now_time)

def run_coroutine_task(async_func):
    # 测试协程
    now_time = time.time()  # 程序运行时的时间戳
    loop = asyncio.get_event_loop()  # 通过get_event_loop方法获取事件循环对象
    tasks = [loop.create_task(async_function(num=num)) for num in range(1, 4)]  # 通过事件循环的create_task方法创建任务列表
    events = asyncio.wait(tasks)  # 通过asyncio.wait(tasks)将任务收集起来

    loop.run_until_complete(events)  # 等待events运行完毕
    for task in tasks:  # 遍历循环列表，将对应任务返回值打印出来
        print(task.result())
    loop.close()  # 结束循环

    print('总运行花费时常：{}秒'.format(time.time() - now_time))

def test_run_coroutine_task():
    run_coroutine_task(async_function)

if __name__ == '__main__':
    pytest.main(["-s","-v","test_process_parallel.py"])

