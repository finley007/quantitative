#! /usr/bin/env python
# -*- coding:utf8 -*-
import os

from common.config import Config
import psutil

config = Config()

DATA_PATH = config.get('common', 'data_path')
FUTURE_TICK_DATA_PATH = config.get('common', 'data_path') + 'original' + os.path.sep + 'future' + os.path.sep + 'tick' + os.path.sep
FUTURE_TICK_TEMP_DATA_PATH = config.get('common', 'data_path') + '/temp/future/tick/'
FUTURE_TICK_COMPARE_DATA_PATH = config.get('common', 'data_path') + '/compare/future/tick/'
FUTURE_TICK_REPORT_DATA_PATH = config.get('common', 'data_path') + '/report/future/tick/'
STOCK_TICK_DATA_PATH = config.get('common', 'data_path') + '/original/stock/tick/'
STOCK_TICK_TEMP_DATA_PATH = config.get('common', 'data_path') + '/temp/stock/tick/'
TEMP_PATH = config.get('common', 'data_path') + '/temp/'
CONFIG_PATH = config.get('common', 'data_path') + '/config/'

DB_CONNECTION = config.get('common', 'db_connection')

FILE_TYPE_CSV = '.csv'

TICK_FILE_PREFIX = 'CFFEX'
TICK_SAMPLE_INTERVAL = 0.5
OFF_TIME_IN_SECOND = 5400
OFF_TIME_IN_MORNING = '11:30:00'

TRANSACTION_START_TIME = '09:15:00'

RESULT_SUCCESS = 'success'
RESULT_FAIL = 'fail'

CPU_CORE_NUMBER = psutil.cpu_count()