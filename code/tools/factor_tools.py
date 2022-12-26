#! /usr/bin/env python
# -*- coding:utf8 -*-

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
        all_factors = list(map(lambda po: po.get_full_name() in factor_list, all_factors))
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
                    # factor_caculator.caculate([factor])
                    time_cost = time.perf_counter() - t
                    factor_operation_history = FactorOperationHistory(factor.get_full_name(), 1, 0, time_cost)
                    session.add(factor_operation_history)
                    session.commit()
                except Exception as e:
                    time_cost = time.perf_counter() - t
                    factor_operation_history = FactorOperationHistory(factor.get_full_name(), 1, 0, time_cost, str(e))
                    session.add(factor_operation_history)
                    session.commit()



if __name__ == '__main__':
    create_factor_files()