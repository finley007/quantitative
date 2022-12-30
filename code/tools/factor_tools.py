#! /usr/bin/env python
# -*- coding:utf8 -*-
import time
import xlrd


from common.persistence.po import FactorConfig, FactorOperationHistory
from common.persistence.dao import FactorConfigDao
from common.persistence.dbutils import create_session
from common.constants import FACTOR_TYPE_DETAILS
from common.reflection import get_all_class, create_instance
from factor.factor_caculator import FactorCaculator

def create_factor_files(factor_list=[]):
    """

    Parameters
    ----------
    factor_list: list (code_version) 如果为空表述要吹所有factor_List表中的因子

    Returns
    -------

    """
    factor_config_dto = FactorConfigDao()
    factor_caculator = FactorCaculator()
    session = create_session()
    all_factors = factor_config_dto.get_all_factors()
    if len(factor_list) > 0:
        all_factors = list(filter(lambda po: po.get_full_name() in factor_list, all_factors))
    for factor_po in all_factors:
        print('Create factor file for {0}'.format(factor_po.get_full_name()))
        module_name = FACTOR_TYPE_DETAILS[factor_po.type]['package']
        all_class = get_all_class(module_name)
        filter_class = list(filter(lambda clz: module_name in str(clz), all_class))
        for clz in filter_class:
            factor = create_instance(module_name, clz.__name__)
            factor_full_name = factor.factor_code + '_' + factor.version
            if factor_full_name == factor_po.get_full_name():
                try:
                    t = time.perf_counter()
                    factor_caculator.caculate([factor])
                    time_cost = time.perf_counter() - t
                    factor_operation_history = FactorOperationHistory(factor.get_full_name(), 1, 0, time_cost)
                    session.add(factor_operation_history)
                    session.commit()
                except Exception as e:
                    time_cost = time.perf_counter() - t
                    factor_operation_history = FactorOperationHistory(factor.get_full_name(), 1, 0, time_cost, str(e))
                    session.add(factor_operation_history)
                    session.commit()



def init_factor_list():
    """
    根据因子列表文档，生成因子表数据
    Returns
    -------

    """
    data = xlrd.open_workbook('D:\\liuli\\workspace\\quantitative\\docs\\因子\\因子列表.xls')
    for sheet_name in data.sheet_names():
        type = sheet_name.split('_')[1]
        sheet = data.sheet_by_name(sheet_name)
        for i in range(1, sheet.nrows):
            session = create_session()
            factor_config = FactorConfig(sheet.row_values(i)[2], sheet.row_values(i)[6], type, sheet.row_values(i)[0], sheet.row_values(i)[1], sheet.row_values(i)[4], sheet.row_values(i)[5])
            session.add(factor_config)
            session.commit()
            print(str(sheet.row_values(i)))


if __name__ == '__main__':
    # create_factor_files(['FCT_01_003_LINEAR_PER_ATR_1.0','FCT_01_004_LINEAR_DEVIATION_1.0','FCT_01_005_QUADRATIC_DEVIATION_1.0','FCT_01_006_CUBIC_DEVIATION_1.0 ','FCT_01_007_PRICE_MOMENTUM_1.0'])
    create_factor_files(['FCT_02_001_TOTAL_COMMISSION_RATIO_1.0'])

    # init_factor_list()