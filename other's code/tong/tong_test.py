"""
@Description: 此文件用于数据测试
@Author  : tong
@Time    : 2022/2/19
@File    : func_lib.py
"""

import os
import sys
import pandas as pd
import datetime
import math
import multiprocessing as mp


# signal.get_product_merged_signal('ret_24', 'AP00')
# DataFrame索引相关测试
'''
data = [['Alex', 10], ['Bob', 12], ['Clarke', 13]]
df = pd.DataFrame(data, columns=['Name', 'Age'])
df.set_index('Name', drop=True, inplace=True)
data2 = [['Alex', 10], ['Bob', 12], ['Clarke', 13]]
df2 = pd.DataFrame(data2, columns=['Name', 'Age'])
df2.set_index('Name', drop=True, inplace=True)
merge_df = pd.concat([df, df2], axis=0)
print(merge_df)
print(merge_df.index)
'''

# DatetimeIndex索引相关测试
# data_one = pd.read_pickle('E:/data/weisoft/factor_data/FCT_DEMARK_2/5m/ZQPM00-5m-FCT_DEMARK_2.pkl')
# data_one.set_index('datetime', inplace=True)
# print(data_one)
# print(data_one.index)
# merge_df = pd.DataFrame()
# merge_df = pd.concat([merge_df, data_one], axis=0)
# print(merge_df)
# merge_df = pd.concat([merge_df, data_one], axis=0)
# print(merge_df)
'''
data = [['2023/02/19', 'AP', 100], ['2023/02/18', 'AP', 101], ['2023/02/17', 'RB', 3001]]
df = pd.DataFrame(data, columns=['datetime', 'product', 'close'])
df.set_index('datetime', inplace=True)
print(df)
print(df.index)

data2 = [['2023/02/19', 'RB', 3002], ['2023/02/18', 'RB', 3003], ['2023/02/17', 'AP', 102]]
df2 = pd.DataFrame(data2, columns=['datetime', 'product', 'close'])
df2.set_index('datetime', inplace=True)
print(df2)
print(df2.index)

merge_df = pd.concat([df, df2], axis=0)
print(merge_df)
print(merge_df.index)

append_data = [['2023/02/19', 'AP', 0.02], ['2023/02/18', 'AP', 0.019], ['2023/02/17', 'RB', 0.018], ['2023/02/19', 'RB', 0.017], ['2023/02/18', 'RB', 0.016], ['2023/02/17', 'AP', 0.015]]
append_df = pd.DataFrame(append_data, columns=['datetime', 'product', 'ret'])
append_df.set_index('datetime', inplace=True)
print(append_df)
print(append_df.index)

# merge_df['ret'] = append_df['ret']
merge_df = pd.merge(merge_df, append_df, how='left', on=['datetime', 'product'])
print(merge_df)
print(merge_df.index)
'''

# 性能计算
# 记录开始时间
'''
starttime = datetime.datetime.now()
total_data = func_lib.load_file(constants.FACTOR_TOTAL_PATH + constants.CYCLE + '//total_result.pkl')
print(total_data)
# func_lib.save_file(total_data, 'D:/logs/aa.pkl')
# total_data['abc'] = total_data['ret.6'].rolling(1000).sum()
dates = total_data.index.get_level_values(0)
min_date = min(dates)
max_date = max(dates)
weights = []
for i in range(len(dates)):
    weight=(dates[i]-min_date)/(max_date-min_date)
    weights.append(weight)
# 记录结束时间
endtime = datetime.datetime.now()
spend_seconds = (endtime - starttime).seconds
print('Spend time: ' + str(spend_seconds) +' seconds.')
'''

# 可以定义任何函数
def f(name, param):
    result = 0
    for i in range(10):
        for num in param:
            result += math.sqrt(num * math.tanh(num) / math.log2(num) / math.log10(num))
    return {name: result}

if __name__ == '__main__':
    # 核心数量: cpu_count() 函数可以获得计算机的核心数量。
    num_cores = int(mp.cpu_count())
    print("本计算机总共有: " + str(num_cores) + " 核心")

    # 进程池: Pool() 函数创建了一个进程池类，用来管理多进程的生命周期和资源分配。
    #        这里进程池传入的参数是核心数量，意思是最多有多少个进程可以进行并行运算。

    param_dict = {'task1': list(range(10, 3000000)),
                  'task2': list(range(3000000, 6000000)),
                  'task3': list(range(6000000, 9000000)),
                  'task4': list(range(9000000, 12000000)),
                  'task5': list(range(12000000, 15000000)),
                  'task6': list(range(15000000, 18000000)),
                  'task7': list(range(18000000, 21000000)),
                  'task8': list(range(21000000, 24000000)),
                  'task9': list(range(24000000, 27000000)),
                  'task10': list(range(27000000, 30000000)),
                  'task11': list(range(30000000, 33000000)),
                  'task12': list(range(33000000, 36000000)),
                  'task13': list(range(36000000, 39000000)),
                  'task14': list(range(39000000, 42000000)),
                  'task15': list(range(42000000, 45000000)),
                  'task16': list(range(45000000, 48000000)),
                  'task17': list(range(10, 3000000)),
                  'task18': list(range(3000000, 6000000)),
                  'task19': list(range(6000000, 9000000)),
                  'task20': list(range(9000000, 12000000)),
                  'task21': list(range(12000000, 15000000)),
                  'task22': list(range(15000000, 18000000)),
                  'task23': list(range(18000000, 21000000)),
                  'task24': list(range(21000000, 24000000)),
                  'task25': list(range(24000000, 27000000)),
                  'task26': list(range(27000000, 30000000)),
                  'task27': list(range(30000000, 33000000)),
                  'task28': list(range(33000000, 36000000)),
                  'task29': list(range(36000000, 39000000)),
                  'task30': list(range(39000000, 42000000)),
                  'task31': list(range(42000000, 45000000)),
                  'task32': list(range(45000000, 48000000)),
                  'task33': list(range(10, 3000000)),
                  'task34': list(range(3000000, 6000000)),
                  'task35': list(range(6000000, 9000000)),
                  'task36': list(range(9000000, 12000000)),
                  'task37': list(range(12000000, 15000000)),
                  'task38': list(range(15000000, 18000000)),
                  'task39': list(range(18000000, 21000000)),
                  'task40': list(range(21000000, 24000000)),
                  'task41': list(range(24000000, 27000000)),
                  'task42': list(range(27000000, 30000000)),
                  'task43': list(range(30000000, 33000000)),
                  'task44': list(range(33000000, 36000000)),
                  'task45': list(range(36000000, 39000000)),
                  'task46': list(range(39000000, 42000000)),
                  'task47': list(range(42000000, 45000000)),
                  'task48': list(range(45000000, 48000000)),
                  'task49': list(range(10, 3000000)),
                  'task50': list(range(3000000, 6000000)),
                  'task51': list(range(6000000, 9000000)),
                  'task52': list(range(9000000, 12000000)),
                  'task53': list(range(12000000, 15000000)),
                  'task54': list(range(15000000, 18000000)),
                  'task55': list(range(18000000, 21000000)),
                  'task56': list(range(21000000, 24000000)),
                  'task57': list(range(24000000, 27000000)),
                  'task58': list(range(27000000, 30000000)),
                  'task59': list(range(30000000, 33000000)),
                  'task60': list(range(33000000, 36000000)),
                  'task61': list(range(36000000, 39000000)),
                  'task62': list(range(39000000, 42000000)),
                  'task63': list(range(42000000, 45000000)),
                  'task64': list(range(45000000, 48000000)),
                  'ttask1': list(range(10, 3000000)),
                  'ttask2': list(range(3000000, 6000000)),
                  'ttask3': list(range(6000000, 9000000)),
                  'ttask4': list(range(9000000, 12000000)),
                  'ttask5': list(range(12000000, 15000000)),
                  'ttask6': list(range(15000000, 18000000)),
                  'ttask7': list(range(18000000, 21000000)),
                  'ttask8': list(range(21000000, 24000000)),
                  'ttask9': list(range(24000000, 27000000)),
                  'ttask10': list(range(27000000, 30000000)),
                  'ttask11': list(range(30000000, 33000000)),
                  'ttask12': list(range(33000000, 36000000)),
                  'ttask13': list(range(36000000, 39000000)),
                  'ttask14': list(range(39000000, 42000000)),
                  'ttask15': list(range(42000000, 45000000)),
                  'ttask16': list(range(45000000, 48000000)),
                  'ttask17': list(range(10, 3000000)),
                  'ttask18': list(range(3000000, 6000000)),
                  'ttask19': list(range(6000000, 9000000)),
                  'ttask20': list(range(9000000, 12000000)),
                  'ttask21': list(range(12000000, 15000000)),
                  'ttask22': list(range(15000000, 18000000)),
                  'ttask23': list(range(18000000, 21000000)),
                  'ttask24': list(range(21000000, 24000000)),
                  'ttask25': list(range(24000000, 27000000)),
                  'ttask26': list(range(27000000, 30000000)),
                  'ttask27': list(range(30000000, 33000000)),
                  'ttask28': list(range(33000000, 36000000)),
                  'ttask29': list(range(36000000, 39000000)),
                  'ttask30': list(range(39000000, 42000000)),
                  'ttask31': list(range(42000000, 45000000)),
                  'ttask32': list(range(45000000, 48000000)),
                  'ttask33': list(range(10, 3000000)),
                  'ttask34': list(range(3000000, 6000000)),
                  'ttask35': list(range(6000000, 9000000)),
                  'ttask36': list(range(9000000, 12000000)),
                  'ttask37': list(range(12000000, 15000000)),
                  'ttask38': list(range(15000000, 18000000)),
                  'ttask39': list(range(18000000, 21000000)),
                  'ttask40': list(range(21000000, 24000000)),
                  'ttask41': list(range(24000000, 27000000)),
                  'ttask42': list(range(27000000, 30000000)),
                  'ttask43': list(range(30000000, 33000000)),
                  'ttask44': list(range(33000000, 36000000)),
                  'ttask45': list(range(36000000, 39000000)),
                  'ttask46': list(range(39000000, 42000000)),
                  'ttask47': list(range(42000000, 45000000)),
                  'ttask48': list(range(45000000, 48000000)),
                  'ttask49': list(range(10, 3000000)),
                  'ttask50': list(range(3000000, 6000000)),
                  'ttask51': list(range(6000000, 9000000)),
                  'ttask52': list(range(9000000, 12000000)),
                  'ttask53': list(range(12000000, 15000000)),
                  'ttask54': list(range(15000000, 18000000)),
                  'ttask55': list(range(18000000, 21000000)),
                  'ttask56': list(range(21000000, 24000000)),
                  'ttask57': list(range(24000000, 27000000)),
                  'ttask58': list(range(27000000, 30000000)),
                  'ttask59': list(range(30000000, 33000000)),
                  'ttask60': list(range(33000000, 36000000)),
                  'ttask61': list(range(36000000, 39000000)),
                  'ttask62': list(range(39000000, 42000000)),
                  'ttask63': list(range(42000000, 45000000)),
                  'ttask64': list(range(45000000, 48000000)),
                  'tttask1': list(range(10, 3000000)),
                  'tttask2': list(range(3000000, 6000000)),
                  'tttask3': list(range(6000000, 9000000)),
                  'tttask4': list(range(9000000, 12000000)),
                  'tttask5': list(range(12000000, 15000000)),
                  'tttask6': list(range(15000000, 18000000)),
                  'tttask7': list(range(18000000, 21000000)),
                  'tttask8': list(range(21000000, 24000000)),
                  'tttask9': list(range(24000000, 27000000)),
                  'tttask10': list(range(27000000, 30000000)),
                  'tttask11': list(range(30000000, 33000000)),
                  'tttask12': list(range(33000000, 36000000)),
                  'tttask13': list(range(36000000, 39000000)),
                  'tttask14': list(range(39000000, 42000000)),
                  'tttask15': list(range(42000000, 45000000)),
                  'tttask16': list(range(45000000, 48000000)),
                  'tttask17': list(range(10, 3000000)),
                  'tttask18': list(range(3000000, 6000000)),
                  'tttask19': list(range(6000000, 9000000)),
                  'tttask20': list(range(9000000, 12000000)),
                  'tttask21': list(range(12000000, 15000000)),
                  'tttask22': list(range(15000000, 18000000)),
                  'tttask23': list(range(18000000, 21000000)),
                  'tttask24': list(range(21000000, 24000000)),
                  'tttask25': list(range(24000000, 27000000)),
                  'tttask26': list(range(27000000, 30000000)),
                  'tttask27': list(range(30000000, 33000000)),
                  'tttask28': list(range(33000000, 36000000)),
                  'tttask29': list(range(36000000, 39000000)),
                  'tttask30': list(range(39000000, 42000000)),
                  'tttask31': list(range(42000000, 45000000)),
                  'tttask32': list(range(45000000, 48000000)),
                  'tttask33': list(range(10, 3000000)),
                  'tttask34': list(range(3000000, 6000000)),
                  'tttask35': list(range(6000000, 9000000)),
                  'tttask36': list(range(9000000, 12000000)),
                  'tttask37': list(range(12000000, 15000000)),
                  'tttask38': list(range(15000000, 18000000)),
                  'tttask39': list(range(18000000, 21000000)),
                  'tttask40': list(range(21000000, 24000000)),
                  'tttask41': list(range(24000000, 27000000)),
                  'tttask42': list(range(27000000, 30000000)),
                  'tttask43': list(range(30000000, 33000000)),
                  'tttask44': list(range(33000000, 36000000)),
                  'tttask45': list(range(36000000, 39000000)),
                  'tttask46': list(range(39000000, 42000000)),
                  'tttask47': list(range(42000000, 45000000)),
                  'tttask48': list(range(45000000, 48000000)),
                  'tttask49': list(range(10, 3000000)),
                  'tttask50': list(range(3000000, 6000000)),
                  'tttask51': list(range(6000000, 9000000)),
                  'tttask52': list(range(9000000, 12000000)),
                  'tttask53': list(range(12000000, 15000000)),
                  'tttask54': list(range(15000000, 18000000)),
                  'tttask55': list(range(18000000, 21000000)),
                  'tttask56': list(range(21000000, 24000000)),
                  'tttask57': list(range(24000000, 27000000)),
                  'tttask58': list(range(27000000, 30000000)),
                  'tttask59': list(range(30000000, 33000000)),
                  'tttask60': list(range(33000000, 36000000)),
                  'tttask61': list(range(36000000, 39000000)),
                  'tttask62': list(range(39000000, 42000000)),
                  'tttask63': list(range(42000000, 45000000)),
                  'tttask64': list(range(45000000, 48000000))
                  }
    # 异步调度: apply_async() 是进程池的一个调度函数。第一个参数是计算函数.第二个参数是需要传入计算函数的参数，这里传入了计算函数名字和计算调参。
    #          异步的意义是在调度之后，虽然计算函数开始运行并且可能没有结束，异步调度都会返回一个临时结果，并且通过列表生成器临时保存在一个列表-results里。

    cores = [1,2,4,8,12,16,32,60,80]
    real_cores = 0
    for i in cores:
        if i < num_cores:
            pool = mp.Pool(i)
            real_cores = i
        else:
            pool = mp.Pool(num_cores)
            real_cores = num_cores

        start_t = datetime.datetime.now()
        results = [pool.apply_async(f, args=(name, param)) for name, param in param_dict.items()]
        # 调度结果: 如果检查列表 results 里的类，会发现 apply_async() 返回的是 ApplyResult，也就是 调度结果类。
        #          简单来说，就是一个用来等待异步结果生成完毕的容器。
        # 获取结果: 调度结果 ApplyResult 类可以调用函数 get(), 这是一个非异步函数，
        #          也就是说 get() 会等待计算函数处理完毕，并且返回结果。
        #          这里的结果就是计算函数的 return。
        results = [p.get() for p in results]

        end_t = datetime.datetime.now()
        elapsed_sec = (end_t - start_t).total_seconds()
        print("进程数为%d"%real_cores + "计算共消耗: " + "{:.2f}".format(elapsed_sec) + " 秒")


