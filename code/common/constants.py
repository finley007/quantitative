#! /usr/bin/env python
# -*- coding:utf8 -*-

from common.config import Config

config = Config()

DATA_PATH = config.get('common', 'data_path')
FUTURE_TICK_DATA_PATH = config.get('common', 'data_path') + '/original/future/tick/'
FUTURE_TICK_TEMP_DATA_PATH = config.get('common', 'data_path') + '/temp/future/tick/'
STOCK_TICK_DATA_PATH = config.get('common', 'data_path') + '/original/stock_daily/{0}/'
STOCK_TICK_TEMP_DATA_PATH = config.get('common', 'data_path') + '/temp/stock_daily/{0}/'
TEMP_PATH = config.get('common', 'data_path') + '/temp/'

FILE_TYPE_CSV = '.csv'

TICK_FILE_PREFIX = 'CFFEX'
TICK_SAMPLE_INTERVAL = 0.5

TRANSACTION_START_TIME = '09:15:00'

RESULT_SUCCESS = 'success'
RESULT_FAIL = 'fail'