#! /usr/bin/env python
# -*- coding:utf8 -*-
from abc import ABCMeta, abstractmethod
from scipy.stats import pearsonr
import numpy as np
import pandas as pd
import os
import datetime
import matplotlib.pyplot as plt
from xgboost import plot_tree

from common.exception.exception import ValidationFailed
from common.localio import save_compress
from common.constants import STOCK_INDEX_PRODUCTS, XGBOOST_MODEL_PATH, TEST_PATH

class ModelEvaluator(metaclass=ABCMeta):
    """
    模型评估基类
    """
    def evaluate(self, predict_y, real_y):
        self.validate(predict_y, real_y)
        return self.do_evaluate(predict_y, real_y)

    @abstractmethod
    def do_evaluate(self, predict_y, real_y):
        pass

    def validate(self, predict_y, real_y):
        if len(predict_y) != len(real_y):
            raise ValidationFailed('The predict vector {0} is different with real vector {1}'.format(str(len(predict_y)), str(len(real_y))))

class CorrelationEvaluator(ModelEvaluator):
    """
    相关性评估
    """
    def do_evaluate(self, predict_y, real_y):
        return pearsonr(predict_y, real_y)

class WinningRateEvaluator(ModelEvaluator):
    """
    胜率评估
    """
    def do_evaluate(self, predict_y, real_y):
        val_list = np.array(predict_y) * np.array(real_y)
        return len(val_list[np.where(val_list > 0)])/len(val_list[np.where(val_list != 0)])

class LossFunctionEvaluator(ModelEvaluator):
    """
    损失函数评估
    """
    def do_evaluate(self, predict_y, real_y):
        nr_predict_y = np.array(predict_y)
        nr_real_y = np.array(real_y)
        return np.sum(np.square(nr_predict_y - nr_real_y))/len(nr_predict_y)

class BackTestEvaluator(ModelEvaluator):
    """
    回测函数
    """

    def __init__(self, model_name, version, data, config, model):
        self._model_name = model_name
        self._version = version
        self._data = data
        self._config = config
        self._model = model
        self.PREDICT_Y_ROLLING_LENGTH = 4800
        self.PREDICT_Y_WIDTH = 2
        self.RET_PERIOD = config.get_target()

    def evaluate(self, data_path = '', fig_path = ''):
        self.do_evaluate(data_path, fig_path)

    def do_evaluate(self, data_path, fig_path):
        all_sum_result, long_all_sum_result, short_all_sum_result, report_df, total_product_statistics = self.caculate_backtest_result()
        if data_path == '':
            data_path = XGBOOST_MODEL_PATH + os.path.sep + 'simulation' + os.path.sep + self._model_name + '_' + self._version + '_simulation.csv'
        if fig_path == '':
            fig_path = XGBOOST_MODEL_PATH + os.path.sep + 'simulation' + os.path.sep + self._model_name + '_' + self._version + '_simulation.png'
        self.create_diagram(all_sum_result, long_all_sum_result, report_df, short_all_sum_result, total_product_statistics, data_path, fig_path)
        # plot_tree(self._model, rankdir='LR')

    def create_diagram(self, all_sum_result, long_all_sum_result, report_df, short_all_sum_result, total_product_statistics, data_path, fig_path):
        # 保存文件并画图
        # save_compress(report_df, data_path)
        report_df.to_csv(data_path)

        # 展示的文字
        show_content = '实验名: ' + self._model_name + '_' + self._version + '\n'
        show_content = show_content + '实验时间: ' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\n'
        show_content = show_content + '总利润: ' + '{:.2f}'.format(total_product_statistics['profit']) + '\n'
        show_content = show_content + '多头总利润: ' + '{:.2f}'.format(total_product_statistics['long_profit']) + '\n'
        show_content = show_content + '空头总利润: ' + '{:.2f}'.format(total_product_statistics['short_profit']) + '\n'
        show_content = show_content + '交易次数: ' + str(total_product_statistics['count']) + '\n'
        show_content = show_content + '多头交易次数: ' + str(total_product_statistics['long_count']) + '\n'
        show_content = show_content + '空头交易次数: ' + str(total_product_statistics['short_count']) + '\n'
        show_content = show_content + '盈利因子: ' + '{:.3f}'.format(total_product_statistics['win_lose_rate']) + '\n'
        show_content = show_content + '多头盈利因子: ' + '{:.3f}'.format(
            total_product_statistics['long_win_lose_rate']) + '\n'
        show_content = show_content + '空头盈利因子: ' + '{:.3f}'.format(
            total_product_statistics['short_win_lose_rate']) + '\n'
        show_content = show_content + '平均利润: ' + '{:.7f}'.format(total_product_statistics['avg_profit']) + '\n'
        show_content = show_content + '多头平均利润: ' + '{:.7f}'.format(
            total_product_statistics['long_avg_profit']) + '\n'
        show_content = show_content + '空头平均利润: ' + '{:.7f}'.format(
            total_product_statistics['short_avg_profit']) + '\n'

        # 图画
        plt.figure(figsize=(14, 8))
        plt.title(self._model_name + '_' + self._version)
        plt.xlabel('日期', fontproperties="SimHei")
        plt.ylabel('收益', fontproperties="SimHei")
        x = all_sum_result.index[1]
        y = all_sum_result.max() * 0.4
        plt.text(x, y, show_content, fontproperties="SimHei")
        plt.plot(all_sum_result)
        plt.plot(long_all_sum_result, color='darkred')
        plt.plot(short_all_sum_result, color='darkgreen')
        plt.savefig(fig_path)
        plt.show()


    def caculate_backtest_result(self):
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
        report_df = pd.DataFrame(
            columns=['product', 'profit', 'long_profit', 'short_profit', 'win_lose_rate', 'long_win_lose_rate',
                     'short_win_lose_rate', 'count', 'long_count', 'short_count', 'avg_profit', 'long_avg_profit',
                     'short_avg_profit', 'win_rate'])
        for product in STOCK_INDEX_PRODUCTS:
            prod_data = self.prepare_back_test_data(product)

            # 计算平均利润等指标
            product_profit = prod_data['ret'].sum()
            product_count = prod_data['abs_flag'].sum()
            product_avg_profit = product_profit / product_count
            # 盈亏比
            product_win_lose_rate = prod_data.loc[prod_data['ret'] > 0, ['ret']].sum()[0] / abs(
                prod_data.loc[prod_data['ret'] < 0, ['ret']].sum()[0])
            product_win_count = prod_data.loc[prod_data['ret'] > 0].shape[0]
            product_lose_count = prod_data.loc[prod_data['ret'] < 0].shape[0]
            if (product_win_count + product_lose_count) > 0:
                product_win_rate = product_win_count / (product_win_count + product_lose_count)
            else:
                product_win_rate = 0

            # 为多头计算平均利润等指标
            product_long_profit = prod_data[product + '-long'].sum()
            product_long_count = prod_data.loc[prod_data['flag'] == 1].shape[0]
            product_long_avg_profit = product_long_profit / product_long_count
            product_long_win_lose_rate = prod_data.loc[prod_data[product + '-long'] > 0, [product + '-long']].sum()[
                                             0] / abs(
                prod_data.loc[prod_data[product + '-long'] < 0, [product + '-long']].sum()[0])
            # 为空头计算平均利润等指标
            product_short_profit = prod_data[product + '-short'].sum()
            product_short_count = prod_data.loc[prod_data['flag'] == -1].shape[0]
            product_short_avg_profit = product_short_profit / product_short_count
            product_short_win_lose_rate = prod_data.loc[prod_data[product + '-short'] > 0, [product + '-short']].sum()[
                                              0] / abs(
                prod_data.loc[prod_data[product + '-short'] < 0, [product + '-short']].sum()[0])

            report_df = report_df.append(
                {'product': product, 'profit': product_profit, 'avg_profit': product_avg_profit,
                 'win_rate': product_win_rate, 'win_lose_rate': product_win_lose_rate, 'count': product_count,
                 'long_profit': product_long_profit, 'long_count': product_long_count,
                 'long_avg_profit': product_long_avg_profit, 'long_win_lose_rate': product_long_win_lose_rate,
                 'short_profit': product_short_profit, 'short_count': product_short_count,
                 'short_avg_profit': product_short_avg_profit, 'short_win_lose_rate': product_short_win_lose_rate
                 }, ignore_index=True)

            # 汇总单品种结果
            # 把交易明细列放入总表
            total_result = pd.merge(total_result, prod_data[product], how='outer', left_index=True, right_index=True)
            # 把多头交易明细列放入多头总表
            total_long_result_df = pd.merge(total_long_result_df, prod_data[product + '-long'], how='outer',
                                            left_index=True, right_index=True)
            # 把空头交易明细列放入多头总表
            total_short_result_df = pd.merge(total_short_result_df, prod_data[product + '-short'], how='outer',
                                             left_index=True, right_index=True)
            # 把品种总次数等加入全品种总次数等数据
            total_trade = product_count + total_trade
            total_win_count = total_win_count + product_win_count
            total_lose_count = total_lose_count + product_lose_count
            total_long_trade = prod_data[prod_data['flag'] == 1]['flag'].sum() + total_long_trade
            total_short_trade = abs(prod_data[prod_data['flag'] == -1]['flag'].sum()) + total_short_trade
            total_long_profit = product_long_profit + total_long_profit
            total_short_profit = product_short_profit + total_short_profit
            total_long_win_money = total_long_win_money + \
                                   prod_data.loc[prod_data[product + '-long'] > 0, [product + '-long']].sum()[0]
            total_long_lose_money = total_long_lose_money + abs(
                prod_data.loc[prod_data[product + '-long'] < 0, [product + '-long']].sum()[0])
            total_short_win_money = total_short_win_money + \
                                    prod_data.loc[prod_data[product + '-short'] > 0, [product + '-short']].sum()[0]
            total_short_lose_money = total_short_lose_money + abs(
                prod_data.loc[prod_data[product + '-short'] < 0, [product + '-short']].sum()[0])
        total_result = total_result.fillna(0)

        # 计算全品种交易指标
        print('================== total result =================== ')
        # 绘制资金曲线
        sum_result = np.sum(total_result, axis=1)
        long_sum_result = np.sum(total_long_result_df, axis=1)
        short_sum_result = np.sum(total_short_result_df, axis=1)
        all_sum_result = sum_result.cumsum()
        long_all_sum_result = long_sum_result.cumsum()
        short_all_sum_result = short_sum_result.cumsum()
        # 统计总结果
        avg_profit = all_sum_result.iloc[-1] / total_trade
        win_lose_rate = (total_long_win_money + total_short_win_money) / (
                    total_long_lose_money + total_short_lose_money)
        total_long_avg_profit = total_long_profit / total_long_trade
        total_short_avg_profit = total_short_profit / total_short_trade
        total_long_win_lose_rate = total_long_win_money / total_long_lose_money
        total_short_win_lose_rate = total_short_win_money / total_short_lose_money
        total_win_rate = total_win_count / (total_win_count + total_lose_count)
        # 对品种按利润进行排序
        report_df.sort_values(by="win_lose_rate", inplace=True, ascending=False)
        report_df = report_df.reset_index(drop=True)
        # 把全品种数据放到第一行
        total_product_statistics = {'product': '全品种', 'profit': all_sum_result.iloc[-1],
                                    'long_profit': total_long_profit, 'short_profit': total_short_profit,
                                    'win_lose_rate': win_lose_rate, 'long_win_lose_rate': total_long_win_lose_rate,
                                    'short_win_lose_rate': total_short_win_lose_rate,
                                    'count': total_trade, 'long_count': total_long_trade,
                                    'short_count': total_short_trade,
                                    'avg_profit': avg_profit, 'long_avg_profit': total_long_avg_profit,
                                    'short_avg_profit': total_short_avg_profit, 'win_rate': total_win_rate,
                                    }
        total_raw_df = pd.DataFrame([total_product_statistics])
        report_df = pd.concat([total_raw_df, report_df], axis=0, ignore_index=True)
        report_df.style.applymap('font-weight: bold', subset=pd.IndexSlice[report_df.index[report_df.index == 0], :])
        # 将列名改为中文名
        report_df = report_df.rename(
            columns={'product': '品种', 'profit': '总利润', 'avg_profit': '平均利润', 'win_rate': '胜率',
                     'win_lose_rate': '盈利因子', 'count': '交易次数',
                     'long_profit': '多头利润', 'long_count': '多头交易次数', 'long_avg_profit': '多头平均利润',
                     'long_win_lose_rate': '多头盈利因子',
                     'short_profit': '空头利润', 'short_count': '空头交易次数', 'short_avg_profit': '空头平均利润',
                     'short_win_lose_rate': '空头盈利因子'})
        return all_sum_result, long_all_sum_result, short_all_sum_result, report_df, total_product_statistics

    def prepare_back_test_data(self, product):
        prod_data = self._data[self._data['product'] == product]
        prod_data.index = list(map(lambda time: datetime.datetime.strptime(time[0:19], "%Y-%m-%d %H:%M:%S"),
                                   prod_data['datetime'].tolist()))
        prod_data['predict_y_abs'] = prod_data['predict_y'].abs()
        prod_data['predict_y_abs_mean'] = prod_data['predict_y_abs'].rolling(self.PREDICT_Y_ROLLING_LENGTH).mean()
        prod_data['flag'] = 0
        # 开多条件
        prod_data.loc[prod_data['predict_y'] > (prod_data['predict_y_abs_mean'] * self.PREDICT_Y_WIDTH), ['flag']] = 1
        # 开空条件
        prod_data.loc[prod_data['predict_y'] < -(prod_data['predict_y_abs_mean'] * self.PREDICT_Y_WIDTH), ['flag']] = -1
        prod_data['abs_flag'] = abs(prod_data['flag'])
        prod_data['ret'] = prod_data[self.RET_PERIOD] * prod_data['flag']
        prod_data[product] = prod_data['ret']
        prod_data[product + '-long'] = prod_data['ret']
        prod_data.loc[prod_data['flag'] == -1, [product + '-long']] = 0
        prod_data[product + '-short'] = prod_data['ret']
        prod_data.loc[prod_data['flag'] == 1, [product + '-short']] = 0
        return prod_data


if __name__ == '__main__':
    predict_y = [1, 2, 3, 4, 5]
    real_y = [2, 4, 6, 8, 10]
    print(CorrelationEvaluator().evaluate(predict_y, real_y))

    predict_y = [1, -2, -3, 4, 0]
    real_y = [2, 4, -6, 8, 10]
    print(WinningRateEvaludator().evaluate(predict_y, real_y))