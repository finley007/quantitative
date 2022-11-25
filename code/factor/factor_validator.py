#! /usr/bin/env python
# -*- coding:utf8 -*-
import os
import re
import time
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages

from common.aop import timing
from common.constants import FUTURE_TICK_ORGANIZED_DATA_PATH, CONFIG_PATH, FACTOR_PATH, STOCK_INDEX_PRODUCTS, \
    REPORT_PATH
from common.exception.exception import InvalidStatus
from common.localio import read_decompress, list_files_in_path, save_compress
from common.persistence.dbutils import create_session
from common.persistence.po import FutureInstrumentConfig
from common.visualization import draw_line, join_two_images
from factor.volume_price_factor import WilliamFactor
from common.exception.exception import ValidationFailed

class FactorValidator():
    """因子校验基类

    Parameters
    ----------
    """

    def __init__(self, validator_list = []):
        self._validator_list = validator_list

    @timing
    def validate(self, factor_list):
        """
        在基类中组装因子校验链表，依此执行校验

        Parameters
        ----------
        factor_list

        Returns
        -------

        """
        if len(factor_list) == 0:
            raise InvalidStatus('Empty factor list')

        #遍历检测器，执行检查
        if len(self._validator_list) > 0:
            for validator in self._validator_list:
                validator.validate(factor_list)

class BasicValidator(FactorValidator):
    """
    一些基本检查
    1. 非空检查

    """

    @timing
    def validate(self, factor_list):
        error_factor = []
        for factor in factor_list:
            for product in STOCK_INDEX_PRODUCTS:
                data = factor.load(product)
                if data.isnull().values.any():
                    error_factor.append(product + '-' + factor.get_full_name())
        if len(error_factor) > 0:
            error_factor_list = '|'.join(error_factor)
            raise ValidationFailed(error_factor_list + ' has nan value')

class StatisticsAnalysis(FactorValidator):
    """
    生成统计信息

    """
    @timing
    def validate(self, factor_list):
        factor_report_path = REPORT_PATH + 'factor' + os.path.sep + 'statistics'
        if not os.path.exists(factor_report_path):
            os.makedirs(factor_report_path)
        columns = ['count', 'mean', 'std', 'min', 'max', '1%', '5%', '10%', '50%', '90%', '95%', '99%', 'abs_mean', 'ic']
        content = []
        index = []
        for factor in factor_list:
            for param in factor.get_params():
                for product in STOCK_INDEX_PRODUCTS:
                    data = factor.load(product)
                    count = len(data)
                    mean = data[factor.get_key(param)].mean()
                    std = data[factor.get_key(param)].std()
                    min = data[factor.get_key(param)].min()
                    max = data[factor.get_key(param)].max()
                    quantile1 = data[factor.get_key(param)].quantile(0.01)
                    quantile5 = data[factor.get_key(param)].quantile(0.05)
                    quantile10 = data[factor.get_key(param)].quantile(0.1)
                    quantile50 = data[factor.get_key(param)].quantile(0.5)
                    quantile90 = data[factor.get_key(param)].quantile(0.9)
                    quantile95 = data[factor.get_key(param)].quantile(0.95)
                    quantile99 = data[factor.get_key(param)].quantile(0.99)
                    abs_mean = ''
                    ic = ''
                    content.append([count, mean, std, min, max, quantile1, quantile5, quantile10, quantile50, quantile90, quantile95, quantile99, abs_mean, ic])
                    index = index + [product]
        report = pd.DataFrame(content, columns=columns, index=index)
        report.to_csv(factor_report_path + os.path.sep + factor.get_key(param) + '_report.csv')


class StabilityValidator(FactorValidator):
    """因子稳定性检查类
    
    """

    @timing
    def validate(self, factor_list):
        factor_diagram_path = REPORT_PATH + 'factor'
        if not os.path.exists(factor_diagram_path):
            os.makedirs(factor_diagram_path)
        for factor in factor_list:
            for param in factor.get_params():
                for product in STOCK_INDEX_PRODUCTS:
                    data = factor.load(product)
                    mean = data.groupby('date')[factor.get_key(param)].mean()
                    std = data.groupby('date')[factor.get_key(param)].std()
                    groupby_data = pd.DataFrame({
                        'mean' : mean,
                        'std' : std
                    })
                    groupby_data['date'] = groupby_data.index
                    mean_path = factor_diagram_path + os.path.sep + product + '_' + factor.get_key(param) + '_mean.png'
                    std_path = factor_diagram_path + os.path.sep + product + '_' + factor.get_key(param) + '_std.png'
                    final_path = factor_diagram_path + os.path.sep + product + '_' + factor.get_key(param) + '.png'
                    draw_line(groupby_data, factor.get_key(param), 'Date', 'Mean',
                              {'x': 'date', 'y': [{'key': 'mean', 'label': 'Mean'}]}, save_path = mean_path)
                    draw_line(groupby_data, factor.get_key(param), 'Date', 'Std',
                              {'x': 'date', 'y': [{'key': 'std', 'label': 'Std'}]}, save_path = std_path)
                    png_file = join_two_images(mean_path, std_path, final_path, flag = 'vertical')
                    os.remove(mean_path)
                    os.remove(std_path)



if __name__ == '__main__':
    #测试因子检测基类
    factor_validator = FactorValidator([
        BasicValidator(),
        StatisticsAnalysis(),
        StabilityValidator()
    ])
    factor_validator.validate([WilliamFactor()])

    #检查有问题的因子
    # data = WilliamFactor().load('IC')
    # print(data)
    # print(data[data.isnull().values==True])
    # print(data.isnull().stack()[lambda x:x].index.tolist())
    # print(data[['datetime', 'FCT_01_001_WILLIAM']])


