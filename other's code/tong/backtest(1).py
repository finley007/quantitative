"""
@Description: 对预测文件进行回溯测试
@Author  : tong
@Time    : 2023/3/11
@File    : backtest.py
"""

import pandas as pd
import numpy as np
import datetime
import matplotlib.pyplot as plt
import sys
import os

sys.path.append(os.getcwd())

import ml.constants as constants
import ml.data.quot as quot
import ml.data.signal as signal
import ml.data.factor as factor
import ml.util.func_lib as func_lib

class Backtest:
    """
        模型回测
    """
    # Backtest对象的构造函数，初始化其变量
    def __init__(self,
                 signal_name: str,
                 test_name: str,
                 cycle: str = constants.CYCLE,
                 ret: int = constants.RET_LEGNTH):
        self.signal = signal_name
        self.cycle = cycle
        self.ret = ret
        self.name = test_name

    # 根据生成的预测值，开仓持有N个周期，计算业绩，大于标准差开仓
    def run(self):
        # 回测参数设置
        SIGNAL_MA_LENGTH = 500
        SIGNAL_THRETHOLD_DISTANCE = 1.5

        # 初始化类
        quot_obj = quot.Quot(self.cycle)
        signal_obj = signal.Signal(self.signal, self.cycle)
        factor_obj = factor.Factor('FCT_amount', self.cycle)

        # 初始化统计用变量
        total_result = pd.DataFrame()
        total_long_result_df = pd.DataFrame()
        total_short_result_df = pd.DataFrame()
        total_trade = 0
        total_win_count = 0
        total_lose_count = 0
        total_long_trade = 0
        total_short_trade = 0
        total_long_profit = 0
        total_short_profit = 0
        total_long_win_money = 0
        total_long_lose_money = 0
        total_short_win_money = 0
        total_short_lose_money = 0
        # 保存结果的DataFrame
        report_df = pd.DataFrame(columns=['product', 'profit', 'long_profit', 'short_profit', 'win_lose_rate', 'long_win_lose_rate', 'short_win_lose_rate', 'count', 'long_count', 'short_count', 'avg_profit', 'long_avg_profit', 'short_avg_profit', 'win_rate'])

        # 逐个文件进行回测
        for product in constants.PRODUCT_LIST:
            # 获取预测值文件和行情文件
            signal_data = signal_obj.get_product_merged_signal(product)
            quot_data = quot_obj.get_product_ret(product)
            factor_data = factor_obj.get_factor_data_by_product(product)
            # 转换到北京时间为准
            signal_data.index = signal_data.index - pd.Timedelta(hours=4)

            # 组合DataFrame
            industry_df = pd.DataFrame()
            # 首先把当前信号值signal加入数据，因为用open开仓，所以需要用上一根k线信号开仓，故加入pre_signal数据
            industry_df['signal'] = signal_data['signal']
            industry_df['pre_signal'] = industry_df['signal'].shift(1)
            # 把回测用到的行情基本数据加入数据
            industry_df['open'] = quot_data['open']
            industry_df['close'] = quot_data['close']
            # 把因子成交额（单位亿/天）加入数据
            industry_df['daily_amount'] = factor_data['FCT_amount-20']
            # rate列用于保存未来收益率，final_open是中间字段用于保存N个周期后的开盘价
            industry_df['final_open'] = industry_df['open'].shift(-self.ret)
            industry_df['rate'] = (industry_df['final_open'] - industry_df['open']) / industry_df['open']
            # signal_ma列用于保存信号值的绝对值平均值
            industry_df['pre_signal_abs'] = industry_df['signal'].abs()
            industry_df['signal_ma'] = industry_df['pre_signal_abs'].rolling(SIGNAL_MA_LENGTH).sum()/SIGNAL_MA_LENGTH
            # flag用于保存是否开平仓操作信号
            industry_df['flag'] = 0
            industry_df.loc[industry_df['pre_signal'] > industry_df['signal_ma']*SIGNAL_THRETHOLD_DISTANCE, ['flag']] = 1
            industry_df.loc[industry_df['pre_signal'] < -SIGNAL_THRETHOLD_DISTANCE*industry_df['signal_ma'], ['flag']] = -1
            # 按照成交额过滤交易
            industry_df.loc[industry_df['daily_amount'] < 50, ['flag']] = 0
            # abs_flag保存该k线是否进行交易
            industry_df['abs_flag'] = abs(industry_df['flag'])
            # return记录该k线收益率，为0则在该k线不交易
            industry_df['return'] = industry_df['flag'] * industry_df['rate']
            # 把单个k线结果进行多空过滤，准备加入全品种总表
            industry_df[product] = industry_df['return']
            industry_df[product+'-long'] = industry_df['return']
            industry_df.loc[industry_df['flag'] == -1, [product+'-long']] = 0
            industry_df[product+'-short'] = industry_df['return']
            industry_df.loc[industry_df['flag'] == 1, [product+'-short']] = 0
            # 计算单品种统计指标，并存入总报告表，交易次数太少的品种可能是僵尸品种过滤掉
            if industry_df['abs_flag'].sum() < 50:
                continue
            # 计算平均利润等指标
            product_profit = industry_df['return'].sum()
            product_count = industry_df['abs_flag'].sum()
            product_avg_profit = product_profit / product_count
            product_win_lose_rate = industry_df.loc[industry_df['return'] > 0, ['return']].sum()[0] / abs(industry_df.loc[industry_df['return'] < 0, ['return']].sum()[0])
            product_win_count = industry_df.loc[industry_df['return'] > 0].shape[0]
            product_lose_count = industry_df.loc[industry_df['return'] < 0].shape[0]
            product_win_rate = product_win_count / (product_win_count + product_lose_count)
            # 为多头计算平均利润等指标
            product_long_profit = industry_df[product+'-long'].sum()
            product_long_count = industry_df.loc[industry_df['flag'] == 1].shape[0]
            product_long_avg_profit = product_long_profit / product_long_count
            product_long_win_lose_rate = industry_df.loc[industry_df[product+'-long'] > 0, [product+'-long']].sum()[0] / abs(industry_df.loc[industry_df[product+'-long'] < 0, [product+'-long']].sum()[0])
            # 为空头计算平均利润等指标
            product_short_profit = industry_df[product+'-short'].sum()
            product_short_count = industry_df.loc[industry_df['flag'] == -1].shape[0]
            product_short_avg_profit = product_short_profit / product_short_count
            product_short_win_lose_rate = industry_df.loc[industry_df[product+'-short'] > 0, [product+'-short']].sum()[0] / abs(industry_df.loc[industry_df[product+'-short'] < 0, [product+'-short']].sum()[0])
            # 把品种结果放入总表
            report_df = report_df.append({'product': product, 'profit': product_profit, 'avg_profit': product_avg_profit, 'win_rate': product_win_rate, 'win_lose_rate': product_win_lose_rate, 'count': product_count,
                                          'long_profit': product_long_profit, 'long_count': product_long_count, 'long_avg_profit': product_long_avg_profit, 'long_win_lose_rate': product_long_win_lose_rate,
                                          'short_profit': product_short_profit, 'short_count': product_short_count, 'short_avg_profit': product_short_avg_profit, 'short_win_lose_rate': product_short_win_lose_rate
                                          }, ignore_index=True)

            # 汇总单品种结果
            # 把交易明细列放入总表
            total_result = pd.merge(total_result, industry_df[product], how='outer', left_index=True, right_index=True)
            # 把多头交易明细列放入多头总表
            total_long_result_df = pd.merge(total_long_result_df, industry_df[product+'-long'], how='outer', left_index=True, right_index=True)
            # 把空头交易明细列放入多头总表
            total_short_result_df = pd.merge(total_short_result_df, industry_df[product+'-short'], how='outer', left_index=True, right_index=True)
            # 把品种总次数等加入全品种总次数等数据
            total_trade = product_count + total_trade
            total_win_count = total_win_count + product_win_count
            total_lose_count = total_lose_count + product_lose_count
            total_long_trade = industry_df[industry_df['flag'] == 1]['flag'].sum() + total_long_trade
            total_short_trade = abs(industry_df[industry_df['flag'] == -1]['flag'].sum()) + total_short_trade
            total_long_profit = product_long_profit + total_long_profit
            total_short_profit = product_short_profit + total_short_profit
            total_long_win_money = total_long_win_money + industry_df.loc[industry_df[product+'-long'] > 0, [product+'-long']].sum()[0]
            total_long_lose_money = total_long_lose_money + abs(industry_df.loc[industry_df[product+'-long'] < 0, [product+'-long']].sum()[0])
            total_short_win_money = total_short_win_money + industry_df.loc[industry_df[product+'-short'] > 0, [product+'-short']].sum()[0]
            total_short_lose_money = total_short_lose_money + abs(industry_df.loc[industry_df[product+'-short'] < 0, [product+'-short']].sum()[0])
            # 为了检查数据计算正确性，导出csv进行人工检查
            industry_df.to_csv('D:/logs/check/' + product + '.csv')
        total_result = total_result.fillna(0)
        # 计算全品种交易指标
        print('================== total result =================== ')
        # 绘制资金曲线
        sum_result = np.sum(total_result, axis=1)
        long_sum_result = np.sum(total_long_result_df, axis=1)
        short_sum_result = np.sum(total_short_result_df, axis=1)
        all_sum_reuslt = sum_result.cumsum()
        long_all_sum_result = long_sum_result.cumsum()
        short_all_sum_result = short_sum_result.cumsum()

        # 统计总结果
        avg_profit = all_sum_reuslt[-1]/total_trade
        win_lose_rate = (total_long_win_money + total_short_win_money) / (total_long_lose_money + total_short_lose_money)
        total_long_avg_profit = total_long_profit / total_long_trade
        total_short_avg_profit = total_short_profit / total_short_trade
        total_long_win_lose_rate = total_long_win_money / total_long_lose_money
        total_short_win_lose_rate = total_short_win_money / total_short_lose_money
        total_win_rate = total_win_count / (total_win_count + total_lose_count)
        # 对品种按利润进行排序
        report_df.sort_values(by="win_lose_rate" , inplace=True, ascending=False)
        report_df = report_df.reset_index(drop=True)
        # 把全品种数据放到第一行
        total_raw_df = pd.DataFrame([{'product': '全品种', 'profit': all_sum_reuslt[-1], 'long_profit': total_long_profit, 'short_profit': total_short_profit, 
                                      'win_lose_rate': win_lose_rate, 'long_win_lose_rate': total_long_win_lose_rate, 'short_win_lose_rate': total_short_win_lose_rate,
                                      'count': total_trade, 'long_count': total_long_trade, 'short_count': total_short_trade, 
                                      'avg_profit': avg_profit, 'long_avg_profit': total_long_avg_profit, 'short_avg_profit': total_short_avg_profit, 'win_rate': total_win_rate,
                                      }])
        report_df = pd.concat([total_raw_df, report_df], axis=0 ,ignore_index=True)
        report_df.style.applymap('font-weight: bold',subset=pd.IndexSlice[report_df.index[report_df.index==0], :])
        # report_df = report_df.append({'product': '全品种', 'profit': all_sum_reuslt[-1], 'avg_profit': avg_profit, 'win_rate': total_win_rate, 'win_lose_rate': win_lose_rate, 'count': total_trade,
        #                               'long_profit': total_long_profit, 'long_count': total_long_trade, 'long_avg_profit': total_long_avg_profit, 'long_win_lose_rate': total_long_win_lose_rate,
        #                               'short_profit': total_short_profit, 'short_count': total_short_trade, 'short_avg_profit': total_short_avg_profit, 'short_win_lose_rate': total_short_win_lose_rate
        #                               }, ignore_index=True)
        # 将列名改为中文名
        report_df=report_df.rename(columns={'product':'品种', 'profit':'总利润', 'avg_profit':'平均利润', 'win_rate':'胜率', 'win_lose_rate':'盈利因子', 'count': '交易次数',
                                            'long_profit': '多头利润', 'long_count': '多头交易次数', 'long_avg_profit': '多头平均利润', 'long_win_lose_rate': '多头盈利因子',
                                            'short_profit': '空头利润', 'short_count': '空头交易次数', 'short_avg_profit': '空头平均利润', 'short_win_lose_rate': '空头盈利因子'})
        print(report_df)
        # 保存文件并画图
        current_time = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
        report_folder = constants.BACKTEST_REPORT_PATH + self.cycle + '\\' + self.signal + '\\'
        func_lib.mkdir(report_folder)
        report_path = report_folder + 'report' + current_time + '.pkl'
        func_lib.save_file(report_df, report_path)
        # 展示的文字 
        show_content = '实验名: ' + self.name + '\n'
        show_content = show_content + '实验时间: ' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\n'
        show_content = show_content + '总利润: ' + '{:.2f}'.format(all_sum_reuslt[-1]) + '\n'
        show_content = show_content + '多头总利润: ' + '{:.2f}'.format(total_long_profit) + '\n'
        show_content = show_content + '空头总利润: ' + '{:.2f}'.format(total_short_profit) + '\n'
        show_content = show_content + '交易次数: ' + str(total_trade) + '\n'
        show_content = show_content + '多头交易次数: ' + str(total_long_trade) + '\n'
        show_content = show_content + '空头交易次数: ' + str(total_short_trade) + '\n'
        show_content = show_content + '盈利因子: ' + '{:.3f}'.format(win_lose_rate) + '\n'
        show_content = show_content + '多头盈利因子: ' + '{:.3f}'.format(total_long_win_lose_rate) + '\n'
        show_content = show_content + '空头盈利因子: ' + '{:.3f}'.format(total_short_win_lose_rate) + '\n'
        show_content = show_content + '平均利润: ' + '{:.7f}'.format(avg_profit) + '\n'
        show_content = show_content + '多头平均利润: ' + '{:.7f}'.format(total_long_avg_profit) + '\n'
        show_content = show_content + '空头平均利润: ' + '{:.7f}'.format(total_short_avg_profit) + '\n'
        # 图画
        plt.figure(figsize=(14, 8))
        plt.title(self.signal + ' - ' + self.cycle)
        plt.xlabel('日期', fontproperties="SimHei")
        plt.ylabel('收益', fontproperties="SimHei")
        plt.text(all_sum_reuslt.index[1], all_sum_reuslt[-1]*0.6, show_content, fontproperties="SimHei")
        plt.plot(all_sum_reuslt)
        plt.plot(long_all_sum_result, color='darkred')
        plt.plot(short_all_sum_result, color='darkgreen')
        plt.show()
