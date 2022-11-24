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
from common.visualization import draw_line
from factor.volume_price_factor import WilliamFactor

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
        #获取k线文件列模板
        if len(self._validator_list) > 0:
            for validator in self._validator_list:
                validator.validate(factor_list)


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
                    pdf = PdfPages(factor_diagram_path + os.path.sep + product + '_' + factor.get_key(param) + '.pdf')
                    draw_line(groupby_data, factor.get_key(param), 'Date', 'Mean',
                              {'x': 'date', 'y': [{'key': 'mean', 'label': 'Mean'}]}, pdf = pdf)
                    draw_line(groupby_data, factor.get_key(param), 'Date', 'Std',
                              {'x': 'date', 'y': [{'key': 'std', 'label': 'Std'}]}, pdf = pdf)
                    pdf.close()


if __name__ == '__main__':
    #测试因子检测基类
    factor_validator = FactorValidator([StabilityValidator()])
    factor_validator.validate([WilliamFactor()])


