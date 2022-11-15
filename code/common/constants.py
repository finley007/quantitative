#! /usr/bin/env python
# -*- coding:utf8 -*-
import os

from common.config import Config
import psutil

config = Config()

DATA_PATH = config.get('common', 'data_path')
FUTURE_TICK_DATA_PATH = config.get('common', 'data_path') + 'original' + os.path.sep + 'future' + os.path.sep + 'tick' + os.path.sep
FUTURE_TICK_TEMP_DATA_PATH = config.get('common', 'data_path') + 'temp' + os.path.sep + 'future' + os.path.sep + 'tick' + os.path.sep
FUTURE_TICK_COMPARE_DATA_PATH = config.get('common', 'data_path') + 'compare' + os.path.sep + 'future' + os.path.sep + 'tick' + os.path.sep
FUTURE_TICK_REPORT_DATA_PATH = config.get('common', 'data_path') + 'report' + os.path.sep + 'future' + os.path.sep + 'tick' + os.path.sep
# FUTURE_TICK_ORGANIZED_DATA_PATH = config.get('common', 'data_path') + 'organized' + os.path.sep + 'future' + os.path.sep + 'tick' + os.path.sep
FUTURE_TICK_ORGANIZED_DATA_PATH = 'E:\\data\\' + 'organized' + os.path.sep + 'future' + os.path.sep + 'tick' + os.path.sep
STOCK_TICK_DATA_PATH = config.get('common', 'data_path') + '/original/stock/tick/'
STOCK_TICK_TEMP_DATA_PATH = config.get('common', 'data_path') + '/temp/stock/tick/'
STOCK_TICK_ORGANIZED_DATA_PATH = 'E:\\data\\' + 'organized' + os.path.sep + 'stock' + os.path.sep + 'tick' + os.path.sep
TEMP_PATH = config.get('common', 'data_path') + os.path.sep + 'temp' + os.path.sep
# TEST_PATH = config.get('common', 'data_path') + os.path.sep + 'test' + os.path.sep
TEST_PATH = 'E:\\data\\' + 'test' + os.path.sep
# CONFIG_PATH = config.get('common', 'data_path') + os.path.sep + 'config' + os.path.sep
CONFIG_PATH = 'E:\\data\\' + 'config' + os.path.sep
# FACTOR_PATH = config.get('common', 'data_path') + os.path.sep + 'factor' + os.path.sep
FACTOR_PATH = 'E:\\data\\' + 'factor' + os.path.sep

DB_CONNECTION = config.get('common', 'db_connection')

FILE_TYPE_CSV = '.csv'

FUTURE_TICK_FILE_PREFIX = 'CFFEX'
FUTURE_TICK_SAMPLE_INTERVAL = 0.5

STOCK_TICK_SAMPLE_INTERVAL = 3

OFF_TIME_IN_SECOND = 5400
OFF_TIME_IN_MORNING = '11:30:00'

STOCK_START_TIME = '09:15:00'
STOCK_TRANSACTION_START_TIME = '09:30:00'
STOCK_TRANSACTION_END_TIME = '15:00:00'

RESULT_SUCCESS = 'success'
RESULT_FAIL = 'fail'

CPU_CORE_NUMBER = psutil.cpu_count()