#! /usr/bin/env python
# -*- coding:utf8 -*-
import os

from common.config import Config
import psutil

config = Config()

#路径信息
DATA_PATH = config.get('common', 'data_path')
FUTURE_TICK_DATA_PATH = config.get('common', 'data_path') + 'original' + os.path.sep + 'future' + os.path.sep + 'tick' + os.path.sep
FUTURE_TICK_TEMP_DATA_PATH = config.get('common', 'data_path') + 'temp' + os.path.sep + 'future' + os.path.sep + 'tick' + os.path.sep
FUTURE_TICK_COMPARE_DATA_PATH = config.get('common', 'data_path') + 'compare' + os.path.sep + 'future' + os.path.sep + 'tick' + os.path.sep
FUTURE_TICK_REPORT_DATA_PATH = config.get('common', 'data_path') + 'report' + os.path.sep + 'future' + os.path.sep + 'tick' + os.path.sep
# FUTURE_TICK_ORGANIZED_DATA_PATH = config.get('common', 'data_path') + 'organized' + os.path.sep + 'future' + os.path.sep + 'tick' + os.path.sep
FUTURE_TICK_ORGANIZED_DATA_PATH = 'E:\\data\\' + 'organized' + os.path.sep + 'future' + os.path.sep + 'tick' + os.path.sep
STOCK_DATA_PATH = config.get('common', 'data_path') + '/original/stock/'
STOCK_TICK_DATA_PATH = config.get('common', 'data_path') + '/original/stock/tick/'
STOCK_TICK_TEMP_DATA_PATH = config.get('common', 'data_path') + '/temp/stock/tick/'
STOCK_TICK_ORGANIZED_DATA_PATH = 'E:\\data\\' + 'organized' + os.path.sep + 'stock' + os.path.sep + 'tick' + os.path.sep
STOCK_TICK_COMBINED_DATA_PATH = 'G:\\data\\' + 'organized' + os.path.sep + 'stock' + os.path.sep + 'tick' + os.path.sep
STOCK_TICK_COMPARE_DATA_PATH = 'E:\\data\\' + 'compare' + os.path.sep + 'stock' + os.path.sep + 'tick' + os.path.sep
TEMP_PATH = config.get('common', 'data_path') + os.path.sep + 'temp' + os.path.sep
# TEST_PATH = config.get('common', 'data_path') + os.path.sep + 'test' + os.path.sep
TEST_PATH = 'G:\\data\\' + 'test' + os.path.sep
# CONFIG_PATH = config.get('common', 'data_path') + os.path.sep + 'config' + os.path.sep
CONFIG_PATH = 'E:\\data\\' + 'config' + os.path.sep
# FACTOR_PATH = config.get('common', 'data_path') + os.path.sep + 'factor' + os.path.sep
FACTOR_PATH = 'G:\\data\\' + 'factor' + os.path.sep
# REPORT_PATH = config.get('common', 'data_path') + os.path.sep + 'report' + os.path.sep
REPORT_PATH = 'G:\\data\\' + 'report' + os.path.sep
#第三方数据路径信息
#通达信
TDX_PATH = config.get('common', 'data_path') + 'original' + os.path.sep + 'stock' + os.path.sep + 'tdx' + os.path.sep
MODEL_PATH = 'G:\\data\\' + 'model' + os.path.sep
XGBOOST_MODEL_PATH = MODEL_PATH + 'xgboost'

#数据库信息
DB_CONNECTION = config.get('common', 'db_connection')

#物理信息
CPU_CORE_NUMBER = psutil.cpu_count()

#时间信息
OFF_TIME_IN_SECOND = 5400
OFF_TIME_IN_MORNING = '11:30:00'

STOCK_START_TIME = '09:15:00'
STOCK_TRANSACTION_START_TIME = '09:30:00'
STOCK_TRANSACTION_NOON_START_TIME = '13:00:00'
STOCK_TRANSACTION_NOON_END_TIME = '11:30:00'
STOCK_TRANSACTION_END_TIME = '15:00:00'
STOCK_OPEN_CALL_AUACTION_2ND_STAGE_END_TIME = '09:25:00'
STOCK_OPEN_CALL_AUACTION_1ST_STAGE_START_TIME = '09:15:00'
STOCK_OPEN_CALL_AUACTION_2ND_STAGE_START_TIME = '09:20:00'
STOCK_CLOSE_CALL_AUACTION_START_TIME = '14:57:00'
STOCK_VALID_DATA_STARTTIME = '09:32:00'

#业务信息和数据字典
STOCK_INDEX_PRODUCTS = ['IC', 'IH', 'IF']
STOCK_INDEX_INFO = {
    'IC': {
        "STOCK_COUNT": 500,
        "POINT_PRICE": 200
    },
    'IF': {
        "STOCK_COUNT": 300,
        "POINT_PRICE": 300
        },
    'IH': {
        "STOCK_COUNT": 50,
        "POINT_PRICE": 300
        },
}
YEAR_LIST = ['2017','2018','2019','2020','2021','2022']
STOCK_FILE_PREFIX = 'stk_tick10_w_'

FILE_TYPE_CSV = '.csv'

FUTURE_TICK_FILE_PREFIX = 'CFFEX'
FUTURE_TICK_SAMPLE_INTERVAL = 0.5

STOCK_TICK_SAMPLE_INTERVAL = 3

RESULT_SUCCESS = 'success'
RESULT_FAIL = 'fail'

FACTOR_TYPE_VP = '01'
FACTOR_TYPE_SP = '02'
FACTOR_TYPE_DETAILS = {
    '01': {
        'name':'量价类',
        'package':'factor.volume_price_factor'
    },
    '02': {
        'name':'现货类',
        'package':'factor.spot_goods_factor'
    }
}
FACTOR_STANDARD_FIELD_TYPE = 'float64'

RET_PERIOD = [1, 2, 5, 10, 20, 30]

REDIS_KEY_STOCK_FILE = 'stock_file'