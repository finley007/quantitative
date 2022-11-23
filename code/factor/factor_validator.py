#! /usr/bin/env python
# -*- coding:utf8 -*-
import os
import re
import time
import pandas as pd

from common.aop import timing
from common.constants import FUTURE_TICK_ORGANIZED_DATA_PATH, CONFIG_PATH, FACTOR_PATH
from common.exception.exception import InvalidStatus
from common.localio import read_decompress, list_files_in_path, save_compress
from common.persistence.dbutils import create_session
from common.persistence.po import FutureInstrumentConfig
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
        for factor in factor_list:
            data = factor.load()
            print(data.columns)


if __name__ == '__main__':
    #测试因子检测基类
    factor_validator = FactorValidator([StabilityValidator()])
    factor_validator.validate([WilliamFactor()])


