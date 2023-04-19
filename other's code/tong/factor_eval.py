from symbol import parameters
import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
from concurrent.futures import ThreadPoolExecutor

import sys
sys.path.append(os.getcwd())

import quant.constants as constants
import quant.lib.func_lib as func_lib
import quant.factor.deal_factor_data as deal_factor_data
import quant.learn.data_pre as data_pre


splite_group = 21
# plt并行问题
plt.switch_backend('agg')


# 并行回测因子在所有参数下面的数据，自动选择阈值
def multi_run_factor_test(factor):
    parameters = constants.FACTOR_PARAS[factor]
    if __name__ == '__main__':
        with ThreadPoolExecutor(max_workers=4) as excutor:
            for parameter in parameters:
                req = excutor.submit(run_factor_test_by_parameter, factor, parameter)
                print('paramter: ' + paramter + ' get response: ' + req + ' finish test.')


# 回测因子在所有参数下面的数据，自动选择阈值
def run_factor_test(factor, cycle):
    for parameter in factor.get_params():
        run_factor_test_by_parameter(factor, parameter, cycle)


# 回测因子在一个参数下面的数据，自动选择阈值
def run_factor_test_by_parameter(factor, parameter, cycle):
    big_data = data_pre.read_big_data(cycle)
    # 获取因子值，并转为绝对值
    factor_data_series = big_data[factor.get_key(parameter)]
    factor_abs_data_series = factor_data_series.abs()
    # 获取因子最大值，并等分划分因子取值范围阈值
    max_factor_value = factor_abs_data_series.max()
    quantile_list = np.linspace(0.0, 1.0, splite_group)
    threshold_list = []
    for quantile in quantile_list:
        threshold_list.append(factor_abs_data_series.quantile(quantile))
    # threshold_list = np.linspace(0.0, max_factor_value, splite_group)
    # threshold_list = np.append(threshold_list, max_factor_value)
    # 保存记录的总表
    total_result = pd.DataFrame(columns=["threshold", "trade_count", "long_trade_count", "short_trade_count", "win_rate", "profit_rate", "long_profit_rate"
                                        , "short_profit_rate", "average_profit"])
    result_dict = {}
    # 依次计算每组阈值
    for seq in range(0, splite_group - 1):
        small_threshold = threshold_list[seq]
        big_threshold = threshold_list[seq + 1]
        # 调用分析函数
        total_result = run_factor_test_by_threshold(factor, parameter, small_threshold, big_threshold, total_result, cycle)
    print(total_result)
    # 保存总的结果文件
    result_file_path = constants.FACTOR_CHECK_PATH + factor.factor_code + '/' + str(parameter) + '_splite_test_result.xlsx'
    result_dict[parameter] = total_result
    func_lib.to_excel(result_dict, result_file_path)


# 回测因子在一个参数下面的数据，手动传入阈值
def run_factor_test_by_threshold(factor, parameter, small_threshold, big_threshold, total_result, cycle):
    # 记录交易结果的数据
    result_df = pd.DataFrame()
    trade_count = 0
    long_trade_count = 0
    short_trade_count = 0
    win_trade_count = 0
    loss_trade_count = 0
    win_trade_money = 0
    lose_trade_money = 0
    long_win_trade_money = 0
    long_lose_trade_money = 0
    short_win_trade_money = 0
    short_lose_trade_money = 0

    # 每个品种依次回测
    for industry in constants.INDUSTRY_LIST:
        # 获取单个品种的预测值
        industry_factor_data = deal_factor_data.read_industry_factor_data(industry, factor, cycle)
        industry_factor_data.index = industry_factor_data.index - pd.Timedelta(hours=4)
        # 记录交易结果的数据
        industry_result_df = pd.DataFrame()
        industry_result_df['factor'] = industry_factor_data[factor.get_key(parameter)]
        industry_result_df['close'] = industry_factor_data['close']
        industry_result_df['open'] = industry_factor_data['open']
        industry_result_df['high'] = industry_factor_data['high']
        industry_result_df['low'] = industry_factor_data['low']
        industry_result_df['pre_factor'] = industry_factor_data[factor.get_key(parameter)].shift(1)
        industry_result_df['final_open'] = industry_factor_data['open'].shift(-constants.ret_length)
        # 每一行计算24周期后的收益率
        industry_result_df['yields_rate'] = (industry_result_df['final_open'] - industry_result_df['open']) / industry_result_df['open']
        # 交易方向
        industry_result_df['flag'] = 0
        industry_result_df['flag'][(industry_result_df['pre_factor'] >= small_threshold) & (industry_result_df['pre_factor'] <= big_threshold)] = 1
        industry_result_df['flag'][(industry_result_df['pre_factor'] <= -small_threshold) & (industry_result_df['pre_factor'] >= -big_threshold)] = -1
        industry_result_df['abs_flag'] = abs(industry_result_df['flag'])
        # 计算交易收益率
        industry_result_df['yields'] = industry_result_df['yields_rate'] * industry_result_df['flag']
        industry_result_df[industry] = industry_result_df['yields']
        # print(industry_result_df)
        # 更新统计数据
        trade_count = trade_count + industry_result_df['abs_flag'].sum()
        long_trade_count = long_trade_count + industry_result_df[industry_result_df['flag'] == 1]['flag'].sum()
        short_trade_count = short_trade_count - industry_result_df[industry_result_df['flag'] == -1]['flag'].sum()
        win_trade_count = win_trade_count + len(industry_result_df[industry_result_df['yields'] > 0])
        loss_trade_count = loss_trade_count + len(industry_result_df[industry_result_df['yields'] < 0])
        win_trade_money = win_trade_money + industry_result_df[industry_result_df['yields'] > 0]['yields'].sum()
        lose_trade_money = lose_trade_money - industry_result_df[industry_result_df['yields'] < 0]['yields'].sum()
        # 分开统计多头空头盈亏比
        long_win_trade_money = long_win_trade_money + industry_result_df[(industry_result_df['yields'] > 0) & (industry_result_df['flag'] == 1)]['yields'].sum()
        short_win_trade_money = short_win_trade_money + industry_result_df[(industry_result_df['yields'] > 0) & (industry_result_df['flag'] == -1)]['yields'].sum()
        long_lose_trade_money = long_lose_trade_money - industry_result_df[(industry_result_df['yields'] < 0) & (industry_result_df['flag'] == 1)]['yields'].sum()
        short_lose_trade_money = short_lose_trade_money - industry_result_df[(industry_result_df['yields'] < 0) & (industry_result_df['flag'] == -1)]['yields'].sum()
        # 将各个品种的收益率放到总结果表格
        result_df = pd.merge(result_df, industry_result_df[industry], how='outer', left_index=True, right_index=True)
    
    # 汇总结果
    result_df.fillna(0, inplace=True)
    all_industry_candle_profit = np.sum(result_df, axis=1)
    all_industry_total_profit = all_industry_candle_profit.cumsum()
    print('======================Backtest Result======================')
    # print(result_df)
    result_info = '总交易次数 : ' + str(trade_count) + '\n' + '做多: ' + str(long_trade_count) + '\n' + '做空: ' + str(short_trade_count) + '\n' \
                    + '胜率: '  + str(win_trade_count/trade_count) + '\n' + '盈利因子 :' + str(win_trade_money/lose_trade_money) + '\n' \
                    + '多头盈利因子 :' + str(long_win_trade_money/long_lose_trade_money) + '\n' + '空头盈利因子 :' + str(short_win_trade_money/short_lose_trade_money) + '\n' \
                    + '平均每笔盈利: ' + str(all_industry_total_profit[-1]/trade_count)
    # 往总记录表中插入数据
    total_result = total_result.append([{'threshold': str(small_threshold) + '-' + str(big_threshold), 'trade_count': trade_count, 'long_trade_count': long_trade_count, 'short_trade_count': short_trade_count, 
                                        'win_rate':(win_trade_count/trade_count), 'profit_rate':(win_trade_money/lose_trade_money), 'long_profit_rate':(long_win_trade_money/long_lose_trade_money), 
                                        "short_profit_rate":(short_win_trade_money/short_lose_trade_money), "average_profit":(all_industry_total_profit[-1]/trade_count)}], ignore_index=True)
    print(result_info)
    # 保存文本文件
    if not os.path.exists(constants.BACKTEST_PATH + factor.factor_code + '/'):
        os.makedirs(constants.BACKTEST_PATH + factor.factor_code + '/')
    with open(constants.BACKTEST_PATH + factor.factor_code + '/' + factor.get_key(parameter) + '-' + str(small_threshold) + '-' + str(big_threshold) + '-result.txt', 'w') as f:
        f.write(result_info)
        f.close()
    fig = plt.figure(figsize=(20, 16))
    plt.plot(all_industry_total_profit.index, all_industry_total_profit.values)
    plt.savefig(constants.BACKTEST_PATH + factor.factor_code + '/' + factor.get_key(parameter) + '-' + str(small_threshold) + '-' + str(big_threshold) + '-result.png')
    return total_result



# 模拟调用
# multi_run_factor_test('FCT_Demark_1')
