#! /usr/bin/env python
# -*- coding:utf8 -*-
import time
import xlrd
import uuid
import pandas as pd
import datetime
import os

from common.persistence.po import FactorConfig, FactorOperationHistory
from common.persistence.dao import FactorConfigDao, FutureConfigDao
from common.persistence.dbutils import create_session
from common.constants import FACTOR_TYPE_DETAILS, REPORT_PATH, STOCK_INDEX_PRODUCTS, RET_PERIOD, XGBOOST_MODEL_PATH
from common.reflection import get_all_class, create_instance
from factor.factor_caculator import FactorCaculator
from common.log import get_logger
from common.aop import timing
from common.localio import save_compress

@timing
def create_factor_files(factor_list=[], current_transaction_id='',  need_resume=True):
    """

    Parameters
    ----------
    factor_list: list (code_version) 如果为空表述要吹所有factor_List表中的因子

    Returns
    -------

    """
    factor_config_dao = FactorConfigDao()
    factor_caculator = FactorCaculator()
    session = create_session()
    # 获取数据库factor_configuration表中所有的因子配置
    all_factors = factor_config_dao.get_all_factors()
    if len(factor_list) > 0:
        # 根据当前参数过滤需要执行的因子
        all_factors = list(filter(lambda po: po.get_full_name() in factor_list, all_factors))
    for factor_po in all_factors:
        get_logger().info('Create factor file for {0}'.format(factor_po.get_full_name()))
        module_name = FACTOR_TYPE_DETAILS[factor_po.type]['package']
        # 获取代码中所有的因子类
        all_classes = get_all_class(module_name)
        all_classes = list(set(all_classes))
        filter_classes = list(filter(lambda clz: module_name in str(clz), all_classes))
        for clz in filter_classes:
            try:
                if factor_po.parameter != '':
                    factor = create_instance(module_name, clz.__name__, to_params(factor_po.parameter))
                else:
                    factor = create_instance(module_name, clz.__name__)
            except Exception as e:
                continue
            factor_full_name = factor.factor_code + '_' + factor.version
            if factor_full_name == factor_po.get_full_name():
                if current_transaction_id == '':
                    id = uuid.uuid4()
                else:
                    id = current_transaction_id
                try:
                    t = time.perf_counter()
                    factor_caculator.caculate(id, [factor], need_resume=need_resume)
                    time_cost = time.perf_counter() - t
                    factor_operation_history = FactorOperationHistory(id, factor.get_full_name(), 1, 0, time_cost)
                    session.add(factor_operation_history)
                    session.commit()
                except Exception as e:
                    get_logger().error(e)
                    time_cost = time.perf_counter() - t
                    factor_operation_history = FactorOperationHistory(id, factor.get_full_name(), 1, 0, time_cost, str(e))
                    session.add(factor_operation_history)
                    session.commit()

@timing
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
@timing
def create_factor_single_instrument_performance_report(instrument, factor_list=[]):
    """
    生成单合约因子性能测试文件gu
    路径：E:\data\report\factor\performance
    文件格式：csv
    文件名：合约_合约日期范围_时间.csv
    列名：分类编码 分类名称 因子编码 因子版本 因子参数 执行时间
    Returns
    -------

    """
    factor_config_dao = FactorConfigDao()
    factor_caculator = FactorCaculator()
    future_config_dao = FutureConfigDao()
    all_factors = factor_config_dao.get_all_factors()
    if len(factor_list) > 0:
        all_factors = list(filter(lambda po: po.get_full_name() in factor_list, all_factors))
    get_logger().info('Create single instrument performance report for instrument {0} for {1} factors'.format(instrument, str(len(all_factors))))
    result_list = []
    handled_factors = set()
    for factor_po in all_factors:
        get_logger().info('Handle factor for {0}'.format(factor_po.get_full_name()))
        module_name = FACTOR_TYPE_DETAILS[factor_po.type]['package']
        all_class = get_all_class(module_name)
        filter_class = list(filter(lambda clz: module_name in str(clz), all_class))
        for clz in filter_class:
            try:
                if factor_po.parameter != '':
                    factor = create_instance(module_name, clz.__name__, to_params(factor_po.parameter))
                else:
                    factor = create_instance(module_name, clz.__name__)
            except Exception as e:
                continue
            factor_full_name = factor.factor_code + '_' + factor.version
            if factor_full_name == factor_po.get_full_name() and factor_full_name not in handled_factors:
                t = time.perf_counter()
                process_code = uuid.uuid4()
                factor_caculator.caculate(process_code, [factor], include_instrument_list=[instrument], performance_test=True)
                time_cost = time.perf_counter() - t
                factor_type = factor.get_category()
                params = '|'.join(list(map(lambda param: str(param), factor.get_params())))
                result = [str(factor_type), str(FACTOR_TYPE_DETAILS[factor_type]['name']), factor.factor_code, factor.version, params, str(time_cost)]
                result_list.append(result)
                handled_factors.add(factor_full_name)
    report = pd.DataFrame(result_list, columns=['category_code', 'category_name', 'factor_code', 'factor_version', 'factor_params', 'time_cost'])
    start_date = future_config_dao.get_start_end_date_by_instrument(instrument)[0]
    end_date = future_config_dao.get_start_end_date_by_instrument(instrument)[0]
    cur_time = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
    file_name = instrument + '_' + start_date + '_' + end_date + '_' + cur_time + '_single_instrument_performance.csv'
    report.to_csv(REPORT_PATH + 'factor' + os.path.sep + 'performance' + os.path.sep + file_name, encoding='gbk')

@timing
def prepare_training_data(data_identification, factor_list=[], products=STOCK_INDEX_PRODUCTS):
    """
    组合因子，准备训练数据
    Returns
    -------

    """
    factors = []
    factor_config_dao = FactorConfigDao()
    # 获取数据库factor_configuration表中所有的因子配置
    all_factors = factor_config_dao.get_all_factors()
    if len(factor_list) > 0:
        # 根据当前参数过滤需要执行的因子
        all_factors = list(filter(lambda po: po.get_full_name() in factor_list, all_factors))
    for factor_po in all_factors:
        get_logger().info('Create factor file for {0}'.format(factor_po.get_full_name()))
        module_name = FACTOR_TYPE_DETAILS[factor_po.type]['package']
        # 获取代码中所有的因子类
        all_classes = get_all_class(module_name)
        all_classes = list(set(all_classes))
        filter_classes = list(filter(lambda clz: module_name in str(clz), all_classes))
        for clz in filter_classes:
            try:
                if factor_po.parameter != '':
                    factor = create_instance(module_name, clz.__name__, to_params(factor_po.parameter))
                else:
                    factor = create_instance(module_name, clz.__name__)
            except Exception as e:
                continue
            factor_full_name = factor.factor_code + '_' + factor.version
            if factor_full_name == factor_po.get_full_name():
                factors.append(factor)
    ret_columns = list(map(lambda period: 'ret.' + str(period), RET_PERIOD))
    ohlc_columns = ['open', 'close', 'high', 'low']
    final_data = pd.DataFrame()
    for product in products:
        product_data = pd.DataFrame()
        for factor in factors:
            data = factor.load(product, is_organized = True)
            if len(product_data) == 0:
                data = data[['datetime'] + factor.get_keys() + ret_columns + ohlc_columns]
                product_data = data
            else:
                data = data[['datetime'] + factor.get_keys()]
                product_data = pd.merge(product_data, data, on='datetime', how='outer')
        product_data['product'] = product
        final_data = pd.concat([final_data, product_data])
    get_logger().info('Total number of samples: {0}'.format(str(len(final_data))))
    get_logger().info('Data schema: {0}'.format(final_data.columns))
    save_compress(final_data, XGBOOST_MODEL_PATH + os.path.sep + 'input' + os.path.sep + data_identification + '.pkl')

def to_params(str):
    """
    生成因子参数
    Parameters
    ----------
    str

    Returns
    -------

    """
    if (str.find("|") != -1):
        params = list(map(lambda str: int(str), str.split("|")))
        return params
    try:
        return [int(str)]
    except ValueError:
        return [float(str)]

if __name__ == '__main__':
    # create_factor_files(['FCT_01_003_LINEAR_PER_ATR_1.0','FCT_01_004_LINEAR_DEVIATION_1.0','FCT_01_005_QUADRATIC_DEVIATION_1.0','FCT_01_006_CUBIC_DEVIATION_1.0 ','FCT_01_007_PRICE_MOMENTUM_1.0'])
    # create_factor_files(['FCT_02_013_BID_LARGE_AMOUNT_BILL_1.0'])

    # init_factor_list()

    # 因子性能测试
    # create_factor_single_instrument_performance_report('IF1712', factor_list=['FCT_01_038_DIFF_VOLUME_WEIGHTED_MA_OVER_MA_1.0'])
    # create_factor_single_instrument_performance_report('IF1712', factor_list=['FCT_01_001_WILLIAM_1.0','FCT_01_002_CLOSE_MINUS_MOVING_AVERAGE_1.0','FCT_01_003_LINEAR_PER_ATR_1.0','FCT_01_004_LINEAR_DEVIATION_1.0','FCT_01_005_QUADRATIC_DEVIATION_1.0','FCT_01_006_CUBIC_DEVIATION_1.0','FCT_01_007_PRICE_MOMENTUM_1.0','FCT_01_008_ADX_1.0','FCT_01_009_MIN_ADX_1.0','FCT_01_010_RESIDUAL_MIN_ADX_1.0','FCT_01_015_INTRADAY_INTENSITY_1.0','FCT_01_016_DELTA_INTRADAY_INTENSITY_1.0','FCT_01_017_PRICE_VARIANCE_RATIO_1.0','FCT_01_018_MIN_PRICE_VARIANCE_RATIO_1.0','FCT_01_019_MAX_PRICE_VARIANCE_RATIO_1.0','FCT_01_020_CHANGE_VARIANCE_RATIO_1.0','FCT_01_021_MIN_CHANGE_VARIANCE_RATIO_1.0','FCT_01_022_MAX_CHANGE_VARIANCE_RATIO_1.0','FCT_01_023_ATR_RATIO_1.0','FCT_01_024_DELTA_PRICE_VARIANCE_RATIO_1.0','FCT_01_025_DELTA_CHANGE_VARIANCE_RATIO_1.0','FCT_01_026_DELTA_ATR_RATIO_1.0','FCT_01_027_PRICE_SKEWNESS_1.0','FCT_01_028_CHANGE_SKEWNESS_1.0','FCT_01_029_PRICE_KURTOSIS_1.0','FCT_01_030_CHANGE_KURTOSIS_1.0','FCT_01_031_DELTA_PRICE_SKEWNESS_1.0','FCT_01_032_DELTA_CHANGE_SKEWNESS_1.0','FCT_01_033_DELTA_PRICE_KURTOSIS_1.0','FCT_01_034_DELTA_CHANGE_KURTOSIS_1.0','FCT_01_035_VOLUME_MOMENTUM_1.0','FCT_01_036_DELTA_VOLUME_MOMENTUM_1.0','FCT_01_037_VOLUME_WEIGHTED_MA_OVER_MA_1.0','FCT_01_038_DIFF_VOLUME_WEIGHTED_MA_OVER_MA_1.0','FCT_01_039_PRICE_VOLUME_FIT_1.0','FCT_01_040_DIFF_PRICE_VOLUME_FIT_1.0','FCT_01_041_DELTA_PRICE_VOLUME_FIT_1.0','FCT_01_042_POSITIVE_VOLUME_INDICATOR_1.0','FCT_01_043_DELTA_POSITIVE_VOLUME_INDICATOR_1.0','FCT_01_044_NEGATIVE_VOLUME_INDICATOR_1.0','FCT_01_045_DELTA_NEGATIVE_VOLUME_INDICATOR_1.0','FCT_01_046_PRODUCT_PRICE_VOLUME_1.0','FCT_01_047_SUM_PRICE_VOLUME_1.0','FCT_01_048_DELTA_PRODUCT_PRICE_VOLUME_1.0','FCT_01_049_DELTA_SUM_PRICE_VOLUME_1.0','FCT_01_050_N_DAY_HIGH_1.0','FCT_01_051_N_DAY_LOW_1.0','FCT_01_052_N_DAY_NARROWER_1.0','FCT_01_053_N_DAY_WIDER_1.0','FCT_01_054_ON_BALANCE_VOLUME_1.0','FCT_01_055_DELTA_ON_BALANCE_VOLUME_1.0','FCT_01_056_DETRENDED_RSI_1.0','FCT_01_057_THRESHOLDED_RSI_1.0','FCT_02_001_TOTAL_COMMISSION_RATIO_1.0','FCT_02_002_10_GRADE_COMMISSION_RATIO_1.0','FCT_02_003_10_GRADE_WEIGHTED_COMMISSION_RATIO_1.0','FCT_02_004_5_GRADE_COMMISSION_RATIO_1.0','FCT_02_005_5_GRADE_WEIGHTED_COMMISSION_RATIO_1.0','FCT_02_006_RISING_STOCK_RATIO_1.0','FCT_02_007_SPREAD_1.0','FCT_02_008_CALL_AUCTION_SECOND_STAGE_INCREASE_1.0','FCT_02_009_TWO_CALL_AUCTION_STAGE_DIFFERENCE_1.0','FCT_02_010_CALL_AUCTION_SECOND_STAGE_RETURN_VOLATILITY_1.0','FCT_02_011_FIRST_STAGE_COMMISSION_RATIO_1.0','FCT_02_012_SECOND_STAGE_COMMISSION_RATIO_1.0','FCT_02_013_ASK_LARGE_AMOUNT_BILL_1.0','FCT_02_014_BID_LARGE_AMOUNT_BILL_1.0','FCT_02_015_TOTAL_COMMISSION_RATIO_CHANGE_RATE_1.0'])
    # create_factor_single_instrument_performance_report('IF1712', factor_list=['FCT_01_001_WILLIAM_1.0', 'FCT_01_002_CLOSE_MINUS_MOVING_AVERAGE_1.0', 'FCT_01_003_LINEAR_PER_ATR_1.0', 'FCT_01_004_LINEAR_DEVIATION_1.0', 'FCT_01_005_QUADRATIC_DEVIATION_1.0', 'FCT_01_006_CUBIC_DEVIATION_1.0', 'FCT_01_007_PRICE_MOMENTUM_1.0', 'FCT_01_008_ADX_1.0', 'FCT_01_009_MIN_ADX_1.0', 'FCT_01_010_RESIDUAL_MIN_ADX_1.0'])
    # create_factor_single_instrument_performance_report('IC1803', factor_list=['FCT_02_002_10_GRADE_COMMISSION_RATIO_2.0'])
    # create_factor_single_instrument_performance_report('IF1712', factor_list=['FCT_02_025_FALLING_LIMIT_STOCK_PROPORTION_1.0'])
    # create_factor_single_instrument_performance_report('IF1712', factor_list=['FCT_AUXILIARY_FILE_GENERATION_1.0'])
    # create_factor_single_instrument_performance_report('IF1712', factor_list=['FCT_02_014_ASK_LARGE_AMOUNT_BILL_1.0'])
    # create_factor_single_instrument_performance_report('IF1712', factor_list=['FCT_02_033_10_GRADE_COMMISSION_VOLATILITY_RATIO_1.0'])
    # create_factor_single_instrument_performance_report('IF1712', factor_list=['FCT_02_055_AMOUNT_ASK_5_GRADE_COMMISSION_RATIO_STD_1.0'])
    # create_factor_single_instrument_performance_report('IF1712', factor_list=['FCT_02_016_AMOUNT_AND_COMMISSION_RATIO_2.0'])
    # create_factor_single_instrument_performance_report('IF1712', factor_list=['FCT_02_031_AMOUNT_ASK_5_GRADE_COMMISSION_RATIO_1.0'])
    # create_factor_single_instrument_performance_report('IF1712', factor_list=['FCT_02_052_AMOUNT_BID_5_GRADE_COMMISSION_RATIO_STD_1.0'])
    # create_factor_single_instrument_performance_report('IF1712', factor_list=['FCT_02_030_AMOUNT_BID_5_GRADE_COMMISSION_RATIO_1.0'])
    # create_factor_single_instrument_performance_report('IF1712', factor_list=['FCT_02_002_10_GRADE_COMMISSION_RATIO_2.0'])
    # create_factor_single_instrument_performance_report('IF1712', factor_list=['FCT_02_036_10_GRADE_COMMISSION_RATIO_DIFFERENCE_1.0'])
    # create_factor_single_instrument_performance_report('IF1712', factor_list=['FCT_02_040_10_GRADE_COMMISSION_RATIO_MEAN_1.0'])
    # create_factor_single_instrument_performance_report('IF1712', factor_list=['FCT_02_004_5_GRADE_COMMISSION_RATIO_2.0'])
    create_factor_single_instrument_performance_report('IF1712', factor_list=['FCT_02_019_DAILY_ACCUMULATED_LARGE_ORDER_RATIO_1.0'])

    # prepare_training_data('data_20230425', ['FCT_01_001_WILLIAM_1.0', 'FCT_01_002_CLOSE_MINUS_MOVING_AVERAGE_1.0',
    #                                         'FCT_02_002_10_GRADE_COMMISSION_RATIO_2.0', 'FCT_02_036_10_GRADE_COMMISSION_RATIO_DIFFERENCE_1.0', 'FCT_02_041_10_GRADE_COMMISSION_RATIO_STD_1.0', 'FCT_02_040_10_GRADE_COMMISSION_RATIO_MEAN_1.0',
    #                                         'FCT_02_004_5_GRADE_COMMISSION_RATIO_2.0', 'FCT_02_037_5_GRADE_COMMISSION_RATIO_DIFFERENCE_1.0', 'FCT_02_043_5_GRADE_COMMISSION_RATIO_STD_1.0', 'FCT_02_042_5_GRADE_COMMISSION_RATIO_MEAN_1.0',
    #                                         'FCT_02_003_10_GRADE_WEIGHTED_COMMISSION_RATIO_1.0', 'FCT_02_044_10_GRADE_WEIGHTED_COMMISSION_RATIO_DIFFERENCE_1.0', 'FCT_02_045_10_GRADE_WEIGHTED_COMMISSION_RATIO_MEAN_1.0',
    #                                         'FCT_02_017_RISING_FALLING_AMOUNT_RATIO_1.0',
    #                                         'FCT_02_007_SPREAD_1.0'
    #                                         ])