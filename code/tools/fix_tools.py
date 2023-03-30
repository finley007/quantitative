#! /usr/bin/env python
# -*- coding:utf8 -*-
import os
import re
import time
from datetime import datetime, timedelta
from abc import ABCMeta, abstractmethod
import pickle
import pandas as pd

from common import constants
from common.aop import timing
from common.constants import FUTURE_TICK_DATA_PATH, FUTURE_TICK_FILE_PREFIX, FUTURE_TICK_COMPARE_DATA_PATH, \
    STOCK_TICK_DATA_PATH, CONFIG_PATH, FUTURE_TICK_TEMP_DATA_PATH, FUTURE_TICK_ORGANIZED_DATA_PATH, RESULT_SUCCESS, \
    STOCK_TICK_ORGANIZED_DATA_PATH, STOCK_TRANSACTION_NOON_END_TIME, STOCK_TRANSACTION_NOON_START_TIME, STOCK_TRANSACTION_END_TIME, \
    STOCK_TRANSACTION_START_TIME, RESULT_FAIL, STOCK_FILE_PREFIX, STOCK_CLOSE_CALL_AUACTION_START_TIME, STOCK_OPEN_CALL_AUACTION_2ND_STAGE_END_TIME
from common.localio import list_files_in_path, save_compress, read_decompress
from common.persistence.dbutils import create_session
from common.persistence.po import StockValidationResult, FutrueProcessRecord, StockProcessRecord, FactorValidationResult, FutureValidationResult
from common.persistence.dao import IndexConstituentConfigDao
from data.process import FutureTickDataColumnTransform, StockTickDataColumnTransform, StockTickDataCleaner, DataCleaner, \
    FutureTickDataProcessorPhase1, FutureTickDataProcessorPhase2, StockTickDataEnricher
from data.validation import StockFilterCompressValidator, FutureTickDataValidator, StockTickDataValidator, StockOrganizedDataValidator, Validator, \
    DtoStockValidationResult, DtoFutureValidationResult
from framework.localconcurrent import ProcessRunner
from common.timeutils import date_alignment, add_milliseconds_suffix, datetime_advance, time_advance
from data.access import StockDataAccess
from common.timeutils import add_milliseconds_suffix, time_difference
from common.log import get_logger
from common.pandasutils import get_index_dict_by_value


def check_issue_stock_process_data(record_id):
    """
    检查给定的stock_process_record记录

    Parameters
    ----------
    record_id

    Returns
    -------

    """
    session = create_session()
    check_list = session.execute(
        'select date, tscode from stock_process_record where id = :id',
        {'id': record_id}).fetchall()
    if len(check_list) > 0:
        date = check_list[0][0]
        stock = check_list[0][1]
        print(check_list)
        original_stock_file_path = STOCK_TICK_DATA_PATH + os.path.sep + add_folder_prefix(date[0:4]) + os.path.sep + add_folder_prefix(date[0:6]) + os.path.sep + date + os.path.sep + stock + '.pkl'
        data = read_decompress(original_stock_file_path)
        data = StockTickDataColumnTransform().process(data)
        data = StockTickDataCleaner().process(data)
        validation_result = StockTickDataValidator(True).validate(data)
        print(validation_result)

def check_issue_stock_validation_data(record_id):
    """
    检查给定的stock_validation_result记录

    Parameters
    ----------
    record_id

    Returns
    -------

    """
    session = create_session()
    check_list = session.execute(
        'select date, tscode from stock_validation_result where id = :id',
        {'id': record_id}).fetchall()
    if len(check_list) > 0:
        date = check_list[0][0].replace('-','')
        stock = check_list[0][1]
        stock = stock.split('.')[0]
        print(check_list)
        original_stock_file_path = STOCK_TICK_DATA_PATH + os.path.sep + add_folder_prefix(date[0:4]) + os.path.sep + add_folder_prefix(date[0:6]) + os.path.sep + date + os.path.sep + stock + '.pkl'
        data = read_decompress(original_stock_file_path)
        data = StockTickDataColumnTransform().process(data)
        data = StockTickDataCleaner().process(data)
        validation_result = StockTickDataValidator(True).validate(data)
        print(validation_result)

def add_folder_prefix(folder):
    return STOCK_FILE_PREFIX + folder

def fix_stock_tick_data(year, month, date_list=[]):
    """
    根据股指成分股重新生成压缩数据

    Parameters
    ----------
    year
    month
    date_list

    Returns
    -------

    """
    root_path = STOCK_TICK_DATA_PATH
    file_prefix = STOCK_FILE_PREFIX
    stocks_abstract_50 = pd.read_pickle(CONFIG_PATH + os.path.sep + '50_stocks_abstract.pkl')
    stocks_abstract_300 = pd.read_pickle(CONFIG_PATH + os.path.sep + '300_stocks_abstract.pkl')
    stocks_abstract_500 = pd.read_pickle(CONFIG_PATH + os.path.sep + '500_stocks_abstract.pkl')
    month_folder_path = root_path + file_prefix + year + os.path.sep + file_prefix + year + month
    if len(date_list) == 0:
        date_list = list_files_in_path(month_folder_path)
    date_list = sorted(date_list)
    for date in date_list:
        if not re.match('[0-9]{8}', date):
            continue
        stocks_50 = get_index_stock_list(date, stocks_abstract_50)
        stocks_300 = get_index_stock_list(date, stocks_abstract_300)
        stocks_500 = get_index_stock_list(date, stocks_abstract_500)
        stock_list = list(set(stocks_50 + stocks_300 + stocks_500))
        exists_stock_file_list = list_files_in_path(month_folder_path + os.path.sep + date)
        exists_stock_list = list(map(lambda item: item.split('.')[0], exists_stock_file_list))
        to_be_added = list(set(stock_list).difference(set(exists_stock_list)))
        source_path = 'E:\\data\\original\\stock\\tick' + os.path.sep + file_prefix + year + os.path.sep + file_prefix + year + month + os.path.sep + date
        stock_file_list = list_files_in_path(source_path)
        for stock_file in stock_file_list:
            if os.path.getsize(source_path + os.path.sep + stock_file) > 0:
                if extract_tsccode(stock_file) in to_be_added:
                    print('Stock %s need to be added' % extract_tsccode(stock_file))
                    data = pd.read_csv(source_path + os.path.sep + stock_file,
                                       encoding='gbk')
                    save_compress(data, month_folder_path + os.path.sep + date + os.path.sep + extract_tsccode(
                        stock_file) + '.pkl')
                else:
                    print('Stock %s exists' % extract_tsccode(stock_file))
                os.remove(source_path + os.path.sep + stock_file)

def get_index_stock_list(date, abstract):
    for date_range in abstract.keys():
        if in_date_range(date, date_range):
            return abstract[date_range]

def in_date_range(date, str_date_range):
    date_range = str_date_range.split('_')
    return date <= date_range[1] and date >= date_range[0]

def extract_tsccode(filename):
    return filename.split('_')[0][2:]


def validate_stock_organized_data(validate_code, include_year_list=[]):
    """
    检查股票已生成数据

    Parameters
    ----------
    validate_code
    include_year_list

    Returns
    -------

    """
    session = create_session()
    checked_list = session.execute('select concat(date, tscode) from stock_validation_result where validation_code = :vcode', {'vcode': validate_code})
    checked_set = set(map(lambda item : item[0], checked_list))
    runner = ProcessRunner(10)
    year_folder_list = list_files_in_path(STOCK_TICK_ORGANIZED_DATA_PATH + os.path.sep)
    year_folder_list.sort()
    for year_folder in year_folder_list:
        years = re.search('[0-9]{4}', year_folder)
        if not years:
            continue
        elif len(include_year_list) > 0 and years.group() not in include_year_list:
            continue
        month_folder_list = list_files_in_path(STOCK_TICK_ORGANIZED_DATA_PATH + os.path.sep + year_folder + os.path.sep)
        month_folder_list.sort()
        for month_folder in month_folder_list:
            if not re.search('[0-9]{6}', month_folder):
                continue
            # runner.execute(validate_stock_organized_by_month, args=(validate_code, checked_set, year_folder, month_folder))
            validate_stock_organized_by_month(validate_code, checked_set, year_folder, month_folder)
    time.sleep(100000)

def validate_stock_organized_by_month(validate_code, checked_set, year_folder, month_folder):
    """
    按月检查股票已生成数据，用于多进程并行运算

    Parameters
    ----------
    validate_code
    checked_set
    year_folder
    month_folder

    Returns
    -------

    """
    session = create_session()
    date_folder_list = list_files_in_path(
        STOCK_TICK_ORGANIZED_DATA_PATH + os.path.sep + year_folder + os.path.sep + month_folder + os.path.sep)
    date_folder_list.sort()
    get_logger().info('Handle date for year {0} and month {1}: {2}'.format(year_folder, month_folder, '|'.join(date_folder_list)))
    for date in date_folder_list:
        if not re.search('[0-9]{8}', date):
            continue
        stock_file_list = list_files_in_path(
            STOCK_TICK_ORGANIZED_DATA_PATH + os.path.sep + year_folder + os.path.sep + month_folder + os.path.sep + date + os.path.sep)
        stock_file_list.sort()
        get_logger().debug('Handle date {0} for stock: {1}'.format(date, '|'.join(stock_file_list)))
        for stock in stock_file_list:
            if date + stock.split('.')[0] not in checked_set:
                try:
                    organized_stock_file_path = STOCK_TICK_ORGANIZED_DATA_PATH + os.path.sep + year_folder + os.path.sep + month_folder + os.path.sep + date + os.path.sep + stock
                    data = read_decompress(organized_stock_file_path)
                except Exception as e:
                    get_logger().error('Load file: {0} error'.format(organized_stock_file_path))
                    continue
                validation_result = BidOrAskMissingDataValidator().validate(data)
                stock_validation_result = StockValidationResult(validate_code, validation_result, len(data))
                session.add(stock_validation_result)
                session.commit()
            else:
                get_logger().debug("{0} {1} has been handled".format(date, stock))


def fix_stock_organized_data(validation_code, include_year_list=[]):
    """
    修复股票已生成数据

    Parameters
    ----------
    validation_code
    include_year_list

    Returns
    -------

    """
    session = create_session()
    checked_list = session.execute("select concat(date, tscode) from stock_validation_result where validation_code = :vcode and result = 1", {'vcode': validation_code})
    checked_set = set(map(lambda item : item[0], checked_list))
    runner = ProcessRunner(20)
    year_folder_list = list_files_in_path(STOCK_TICK_ORGANIZED_DATA_PATH + os.path.sep)
    for year_folder in year_folder_list:
        years = re.search('[0-9]{4}', year_folder)
        if not years:
            continue
        elif len(include_year_list) > 0 and years.group() not in include_year_list:
            continue
        month_folder_list = list_files_in_path(STOCK_TICK_ORGANIZED_DATA_PATH + os.path.sep + year_folder + os.path.sep)
        for month_folder in month_folder_list:
            if not re.search('[0-9]{6}', month_folder):
                continue
            runner.execute(fix_stock_organized_data_by_month, args=(validation_code, checked_set, year_folder, month_folder))
            # fix_stock_organized_data_by_month(validation_code, checked_set, year_folder, month_folder)
    time.sleep(100000)

def fix_future_organized_data(validation_code, product_list=[], instrument_list=[], date_list=[]):
    """
    修复期货已生成数据

    Parameters
    ----------
    validation_code
    include_year_list

    Returns
    -------

    """
    session = create_session()
    checked_list = session.execute("select concat(date, instrument) from future_validation_result where validation_code = :vcode and result = 1", {'vcode': validation_code})
    checked_set = set(map(lambda item : item[0], checked_list))
    runner = ProcessRunner(20)
    product_folder_list = list_files_in_path(FUTURE_TICK_ORGANIZED_DATA_PATH + os.path.sep)
    for product_folder in product_folder_list:
        if len(product_list) > 0 and product_folder not in product_list:
            continue
        # runner.execute(fix_future_organized_data_by_product, args=(validation_code, checked_set, product_folder, instrument_list, date_list))
        fix_future_organized_data_by_product(validation_code, checked_set, product_folder, instrument_list, date_list)
    time.sleep(100000)

def fix_future_organized_data_by_product(validation_code, checked_set, product_folder, instrument_list, date_list):
    """
    按合约修复期货已生成数据，主要用于多进程并发执行

    Parameters
    ----------
    validation_code
    checked_set
    product_folder
    instrument_list


    Returns
    -------

    """
    session = create_session()
    instrument_file_list = list_files_in_path(FUTURE_TICK_ORGANIZED_DATA_PATH + os.path.sep + product_folder + os.path.sep)
    instrument_file_list.sort()
    for instrument_file in instrument_file_list:
        instrument = instrument_file.split('.')[0]
        get_logger().debug('Handle instrument {0}'.format(instrument))
        if len(instrument_list) > 0 and instrument not in instrument_list:
            continue
        organized_future_file_path = FUTURE_TICK_ORGANIZED_DATA_PATH + os.path.sep + product_folder + os.path.sep + instrument_file
        data = read_decompress(organized_future_file_path)
        data['date'] = data['datetime'].str[0:10]
        data['instrument'] = instrument
        date_list = list(set(data['date'].tolist()))
        date_list.sort()
        for date in date_list:
            if date + instrument in checked_set:
                if len(include_date_list) > 0 and date not in include_date_list:
                    continue
                data = BidOrAskMissingDataFixer().fix(data)
                if len(data) > 0:
                    save_compress(data, organized_stock_file_path)
                    validation_result = session.query(StockValidationResult).filter(
                        StockValidationResult.validation_code == validation_code,
                        StockValidationResult.tscode == stock.split('.')[0], StockValidationResult.date == date).one()
                    validation_result.result = 2  # 2表示修复过
                    validation_result.modified_time = datetime.now()
                    session.commit()


def fix_stock_organized_data_by_month(validation_code, checked_set, year_folder, month_folder):
    """
    按月修复股票已生成数据，主要用于多进程并发执行

    Parameters
    ----------
    validation_code
    checked_set
    year_folder
    month_folder

    Returns
    -------

    """
    session = create_session()
    date_folder_list = list_files_in_path(
        STOCK_TICK_ORGANIZED_DATA_PATH + os.path.sep + year_folder + os.path.sep + month_folder + os.path.sep)
    date_folder_list.sort()
    get_logger().info(
        'Handle date for year {0} and month {1}: {2}'.format(year_folder, month_folder, '|'.join(date_folder_list)))
    for date in date_folder_list:
        if not re.search('[0-9]{8}', date):
            continue
        stock_file_list = list_files_in_path(
            STOCK_TICK_ORGANIZED_DATA_PATH + os.path.sep + year_folder + os.path.sep + month_folder + os.path.sep + date + os.path.sep)
        stock_file_list.sort()
        get_logger().debug('Handle date {0} for stock: {1}'.format(date, '|'.join(stock_file_list)))
        for stock in stock_file_list:
            if date + stock.split('.')[0] in checked_set:
                try:
                    organized_stock_file_path = STOCK_TICK_ORGANIZED_DATA_PATH + os.path.sep + year_folder + os.path.sep + month_folder + os.path.sep + date + os.path.sep + stock
                    data = read_decompress(organized_stock_file_path)
                except Exception as e:
                    get_logger().error('Load file: {0} error'.format(organized_stock_file_path))
                    continue
                get_logger().debug('Fix for {0} and {1}'.format(date, stock))
                data = BidOrAskMissingDataFixer().fix(data)
                if len(data) > 0:
                    save_compress(data, organized_stock_file_path)
                    validation_result = session.query(StockValidationResult).filter(StockValidationResult.validation_code == validation_code, StockValidationResult.tscode == stock.split('.')[0], StockValidationResult.date == date).one()
                    validation_result.result = 2 #2表示修复过
                    validation_result.modified_time = datetime.now()
                    session.commit()

class DataFixer(metaclass=ABCMeta):

    @abstractmethod
    def fix(self, data):
        pass

class CollectionBiddingDuplicateDataFixer(DataFixer):

    def fix(self, data):
        """
        修复方法：直接在原始数据集找到这些缺失的数据，转换之后插入目标数据集，重新按时间排序
        Parameters
        ----------
        data

        Returns
        -------

        """
        stock = data.iloc[0]['tscode']
        stock = stock[0:6]
        date = data.iloc[0]['date']
        data_access = StockDataAccess()
        #获取原始数据
        original_data = data_access.access(date, stock)
        #获取丢失的数据
        original_data = original_data[(original_data['时间'] >= '09:15:00.000') & (original_data['时间'] < '09:25:00.000')]
        original_data_group_by = original_data.groupby('时间').count()
        original_data_group_by = original_data_group_by[original_data_group_by['代码'] > 1]
        original_data = original_data[original_data['时间'].apply(lambda time: time in original_data_group_by.index.tolist())]
        #转换
        original_data = StockTickDataColumnTransform().process(original_data)
        data = pd.concat([data, original_data])
        data = data.sort_values(axis=0, by='time')
        return data

class ClosingDuplicateDataFixer(DataFixer):

    def fix(self, data):
        """
        修复方法：检查当前目标数据集15：00是否有成交量不为0的数据，如果确实就补上
        Parameters
        ----------
        data

        Returns
        -------

        """
        stock = data.iloc[0]['tscode']
        stock = stock[0:6]
        date = data.iloc[0]['date']
        temp_data = data[data['time'] == '15:00:00.000']
        if len(temp_data) != 0:
            temp_data = temp_data[temp_data['volume'] > 0]
            if len(temp_data) > 0:
                # 数据合法：尾盘有数据，且包含成交量
                return data
            else:
                data = data.drop(data.index[data['time'] == '15:00:00.000'])
        #获取原始数据
        data_access = StockDataAccess()
        original_data = data_access.access(date, stock)
        #转换
        original_data = StockTickDataColumnTransform().process(original_data)
        # 获取丢失的数据
        original_data = original_data[(original_data['time'] == '15:00:00.000') & (original_data['volume'] > 0)]
        data = pd.concat([data, original_data])
        data = data.sort_values(axis=0, by='time')
        return data

class ZeroTransactionNumberFixer(DataFixer):

    def fix(self, data):
        """
        修复方法：
        选取不为transaction_number不为0的第一个时间点
        大于第一个时间点之后，对transaction_number做反向差分，选出值大于0的，构造成一个dict
        transaction_number为0的获取不大于当前index且存在与dict里的key值
        Parameters
        ----------
        data

        Returns
        -------

        """
        stock = data.iloc[0]['tscode']
        stock = stock[0:6]
        date = data.iloc[0]['date']
        data = data.reset_index()
        temp_data = data[(data['transaction_number'] != 0)]
        check_time = temp_data.iloc[0]['time']
        temp_data = data[(data['time'] >= check_time)]
        temp_data['delta_transaction_number'] = temp_data['transaction_number'] - temp_data['transaction_number'].shift(-1)
        temp_data = temp_data[temp_data['delta_transaction_number'] > 0]['transaction_number']
        dict = temp_data.to_dict()
        data['temp_index'] = data.index
        data['transaction_number'] = data.apply(lambda item: self.fix_transaction_number(item, dict, check_time), axis=1)
        return data

    def fix_transaction_number(self, item, dict, check_time):
        time = item['time']
        index = item['temp_index']
        transaction_number = item['transaction_number']
        if time <= check_time or transaction_number != 0:
            return transaction_number
        key_list = list(dict.keys())
        key_list.sort()
        tgt_index = 0
        for key in key_list:
            if key < index:
                tgt_index = key
            else:
                break
        tansaction_number = dict[tgt_index]
        return tansaction_number

class CloseRecordMissingFixer(DataFixer):

    def fix(self, data):
        """
        修复方法：
        检查生成数据是否收盘时刻数据缺失，如果有做如下处理：
        1. 如果最后一条有成交的时间在15：00或者15：00之前，则直接将原始数据的15：00插入
        2. 如果最后一条有成交的时间在15：00之后则先不处理
        3. 检查11:30之前的最后一条数据的时间，按3秒钟拷贝
        Parameters
        ----------
        data

        Returns
        -------

        """
        stock = data.iloc[0]['tscode']
        stock = stock[0:6]
        date = data.iloc[0]['date']
        temp_data = data[(data['time'] == add_milliseconds_suffix(STOCK_TRANSACTION_END_TIME))]
        data_access = StockDataAccess()
        if len(temp_data) == 0:
            original_data = data_access.access(date, stock)
            original_data = StockTickDataColumnTransform().process(original_data)
            # 处理成交量全天为0的数据
            if len(original_data[original_data['volume'] > 0]) == 0:
                original_data['volume'] = original_data['daily_accumulated_volume'] - original_data['daily_accumulated_volume'].shift(1)
                original_data['amount'] = original_data['daily_amount'] - original_data['daily_amount'].shift(1)
            temp_data = original_data[original_data['volume'] > 0]
            try:
                last_volume_time = temp_data.iloc[-1]['time']
            except Exception as e:
                get_logger().warning('Special case found for {0} and {1}'.format(date, stock))
                return pd.DataFrame()
            if last_volume_time <= add_milliseconds_suffix(STOCK_TRANSACTION_END_TIME):
                # 获取丢失的数据
                original_data = original_data[(original_data['time'] == add_milliseconds_suffix(STOCK_TRANSACTION_END_TIME))]
                data = pd.concat([data, original_data])
                data = data.sort_values(axis=0, by='time')
                return data
            else:
                # 如2022/5/31 300253这条数据：全天成交量为0，而且收盘集合竞价成交在15：00之后，把这条数据挪到15：00
                original_data = original_data[(original_data['time'] == add_milliseconds_suffix(last_volume_time))]
                original_data['time'] = add_milliseconds_suffix(STOCK_TRANSACTION_END_TIME)
                data = pd.concat([data, original_data])
                data = data.sort_values(axis=0, by='time')
                return data
        temp_data = data[(data['time'] == add_milliseconds_suffix(STOCK_TRANSACTION_NOON_END_TIME))]
        if len(temp_data) == 0:
            original_data = data_access.access(date, stock)
            original_data = StockTickDataColumnTransform().process(original_data)
            try:
                last_record_in_morning = original_data[(original_data['time'] <= add_milliseconds_suffix(STOCK_TRANSACTION_NOON_END_TIME))].iloc[-1]
            except Exception as e:
                get_logger().warning('Special case found for {0} and {1}'.format(date, stock))
                return pd.DataFrame()
            last_time_in_morning = last_record_in_morning['time']
            delta_time_sec = time_difference(last_time_in_morning, add_milliseconds_suffix(STOCK_TRANSACTION_NOON_END_TIME))
            if delta_time_sec > 0:
                miss_data = pd.DataFrame(columns=data.columns.tolist())
                step = constants.STOCK_TICK_SAMPLE_INTERVAL
                while step <= delta_time_sec:
                    last_record_in_morning['time'] = time_advance(last_time_in_morning, step)
                    miss_data = miss_data.append(last_record_in_morning)
                    step = step + constants.STOCK_TICK_SAMPLE_INTERVAL
                data = data.append(miss_data)
                data = data.sort_values(by=['time'])
                data = data.reset_index(drop=True)
                return data
            if delta_time_sec == 0:
                miss_data = pd.DataFrame(columns=data.columns.tolist())
                miss_data = miss_data.append(last_record_in_morning)
                data = data.append(miss_data)
                data = data.sort_values(by=['time'])
                data = data.reset_index(drop=True)
                return data
        return pd.DataFrame()

class NoVolumeDataFixer(DataFixer):

    def fix(self, data):
        """
        修复方法：
        直接用当日累计成交量和成交额做差分来实现
        Parameters
        ----------
        data

        Returns
        -------

        """
        stock = data.iloc[0]['tscode']
        stock = stock[0:6]
        date = data.iloc[0]['date']
        data = data.reset_index(drop=True)
        empty_records = len(data[data['volume'] == 0])
        all_records = len(data)
        if all_records > 0 and empty_records / all_records > 0.999:
            data.loc[data['volume'] == 0, 'volume'] = data['daily_accumulated_volume'] - data['daily_accumulated_volume'].shift(1)
            data.loc[data['amount'] == 0, 'amount'] = data['daily_amount'] - data['daily_amount'].shift(1)
            return data
        return pd.DataFrame()

class NotClearedAfterCompletionFixer(DataFixer):

    def fix(self, data):
        """
        修复方法：
        比较日累计成交量增量和当前成交量，日累计成交量增量为0，但是成交量不为0，则需要置0
        Parameters
        ----------
        data

        Returns
        -------

        """
        stock = data.iloc[0]['tscode']
        stock = stock[0:6]
        date = data.iloc[0]['date']
        temp_data = data.copy(deep=True)
        temp_data['delta_daily_accumulated_volume'] = temp_data['daily_accumulated_volume'] - temp_data['daily_accumulated_volume'].shift(1)
        if len(temp_data[(temp_data['delta_daily_accumulated_volume'] == 0) & (temp_data['volume'] > 0)]) > 0:
            data.loc[(temp_data['delta_daily_accumulated_volume'] == 0) & (temp_data['volume'] > 0), ['volume','amount']] = 0
        return data

class RepeatRecordFixer(DataFixer):

    def fix(self, data):
        """
        修复方法：
        重复数据，按时间找出所有重复数据的index，遍历这些index，如果当前daily_accumulated_volume最早的index不等于当前的index就删除该条记录
        Parameters
        ----------
        data

        Returns
        -------

        """
        data = data.reset_index(drop=True)
        temp_data = data[(data['time'] <= STOCK_TRANSACTION_END_TIME + '.000') & (
                    data['time'] >= STOCK_TRANSACTION_START_TIME + '.000') & (
                                     (data['time'] >= STOCK_TRANSACTION_NOON_START_TIME + '.000') | (
                                         data['time'] <= STOCK_TRANSACTION_NOON_END_TIME + '.000'))]
        temp_data = temp_data.groupby('time')[['time']].count()
        temp_data = temp_data[temp_data['time'] > 1]
        if len(temp_data) > 0:
            for index_time in temp_data.index:
                tdata = data[data['time'] == index_time]
                if len(tdata) > 0:
                    for index in tdata.index:
                        cur_daily_accumulated_volume = data.loc[index]['daily_accumulated_volume']
                        if type(cur_daily_accumulated_volume) != int:
                            cur_daily_accumulated_volume = cur_daily_accumulated_volume.tolist()[0]
                        min_index = min(data[data['daily_accumulated_volume'] == cur_daily_accumulated_volume].index)
                        if min_index != index:
                            data = data.drop(index)
        return data

class TenGradeFiveGradeDataFixer(DataFixer):

    def fix(self, data):
        """
        增加10档和5档委比计算结果
        Parameters
        ----------
        data

        Returns
        -------

        """
        data['10_grade_bid_amount'] = data.apply(lambda item: self.amount_sum(item, 'bid', 11), axis=1)
        data['10_grade_ask_amount'] = data.apply(lambda item: self.amount_sum(item, 'ask', 11), axis=1)
        data['5_grade_bid_amount'] = data.apply(lambda item: self.amount_sum(item, 'bid', 6), axis=1)
        data['5_grade_ask_amount'] = data.apply(lambda item: self.amount_sum(item, 'ask', 6), axis=1)
        return data

    def amount_sum(self, item, type, rank):
        sum = 0
        for i in range(1, rank):
            sum = sum + ((item[type + '_price' + str(i)]) * (item[type + '_volume' + str(i)]))
        return sum

class DefaultDataFixer(DataFixer):

    def __init__(self):
        self._constituent_config_dao = IndexConstituentConfigDao()
    def fix(self, data):
        """
        修复方法：
        将index_constituent_config对应的日期和股票置成状态2，数据异常
        Parameters
        ----------
        data

        Returns
        -------

        """
        stock = data.iloc[0]['tscode']
        stock = stock[0:6]
        date = data.iloc[0]['date']
        self._constituent_config_dao.update_status(date, stock, 2)
        return data

class BidOrAskMissingDataFixer(DataFixer):
    """
    利用段兄给的数据源修复10档委买委卖数据
    数据路径：D:\liuli\data\patch\stock\patch_for_ten_grade_commission
    """

    def __init__(self):
        self._path = 'D:\\liuli\\data\\patch\\stock\\patch_for_ten_grade_commission\\'
        self._columns = ['bid_price1', 'bid_volume1', 'bid_price2', 'bid_volume2','bid_price3', 'bid_volume3','bid_price4', 'bid_volume4','bid_price5', 'bid_volume5','bid_price6', 'bid_volume6','bid_price7', 'bid_volume7','bid_price8', 'bid_volume8','bid_price9', 'bid_volume9','bid_price10', 'bid_volume10',
                             'ask_price1', 'ask_volume1', 'ask_price2', 'ask_volume2', 'ask_price3', 'ask_volume3','ask_price4', 'ask_volume4','ask_price5', 'ask_volume5','ask_price6', 'ask_volume6','ask_price7', 'ask_volume7','ask_price8', 'ask_volume8','ask_price9', 'ask_volume9','ask_price10', 'ask_volume10']
        self.column_mapping = {
            'bid_price1' : 'bid0_price',
            'bid_price2' : 'bid1_price',
            'bid_price3' : 'bid2_price',
            'bid_price4' : 'bid3_price',
            'bid_price5' : 'bid4_price',
            'bid_price6' : 'bid5_price',
            'bid_price7' : 'bid6_price',
            'bid_price8' : 'bid7_price',
            'bid_price9' : 'bid8_price',
            'bid_price10' : 'bid9_price',
            'bid_volume1': 'bid0_volume',
            'bid_volume2': 'bid1_volume',
            'bid_volume3': 'bid2_volume',
            'bid_volume4': 'bid3_volume',
            'bid_volume5': 'bid4_volume',
            'bid_volume6': 'bid5_volume',
            'bid_volume7': 'bid6_volume',
            'bid_volume8': 'bid7_volume',
            'bid_volume9': 'bid8_volume',
            'bid_volume10': 'bid9_volume',
            'ask_price1': 'ask0_price',
            'ask_price2': 'ask1_price',
            'ask_price3': 'ask2_price',
            'ask_price4': 'ask3_price',
            'ask_price5': 'ask4_price',
            'ask_price6': 'ask5_price',
            'ask_price7': 'ask6_price',
            'ask_price8': 'ask7_price',
            'ask_price9': 'ask8_price',
            'ask_price10': 'ask9_price',
            'ask_volume1': 'ask0_volume',
            'ask_volume2': 'ask1_volume',
            'ask_volume3': 'ask2_volume',
            'ask_volume4': 'ask3_volume',
            'ask_volume5': 'ask4_volume',
            'ask_volume6': 'ask5_volume',
            'ask_volume7': 'ask6_volume',
            'ask_volume8': 'ask7_volume',
            'ask_volume9': 'ask8_volume',
            'ask_volume10': 'ask9_volume'
        }
        self._constituent_config_dao = IndexConstituentConfigDao()

    def fix(self, data):
        """
        修复方法：
        加载修复数据源填充到当前数据集中
        Parameters
        ----------
        data

        Returns
        -------

        """
        stock = data.iloc[0]['tscode']
        stock = stock[0:6]
        date = data.iloc[0]['date']
        target_file = date + '.h5'
        file_path = self._path + target_file
        try:
            daily_patch_data = pd.read_hdf(file_path)
        except Exception as e:
            return pd.DataFrame()
        def get_full_stock(stock_code):
            if stock_code[0] == '6':
                return stock_code + '.XSHG'
            elif stock_code[0] == '0' or stock_code[0] == '3':
                return stock_code + '.XSHE'
            else:
                raise InvalidStatus('Unrecognized stock code {0}'.format(stock_code))

        stock_code = get_full_stock(stock)
        try:
            patch_data = daily_patch_data.loc[stock_code]
        except Exception as e:
            get_logger().error('The stock: {} is missing in patch data'.format(stock_code))
        data = data.reset_index(drop=True)
        check_data = data[(data['time'] >= add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)) & (data['time'] < add_milliseconds_suffix(STOCK_CLOSE_CALL_AUACTION_START_TIME))][self._columns]
        index_dict = get_index_dict_by_value(check_data, 0, self._columns)
        for index in index_dict.keys():
            time = data.loc[index]['time']
            new_index = str(int(time[0:2]))+time[3:5]+time[6:8]
            columns = index_dict[index]
            new_columns = list(map(lambda col: self.column_mapping[col], columns))
            try:
                values = patch_data.loc[int(new_index), new_columns].tolist()
                data.loc[index, columns] = values
            except Exception as e:
                get_logger().error('Patch data missing for index {0}, use last record for patching'.format(str(index)))
                if index == 0:
                    return pd.DataFrame()
                else:
                    data.loc[index, columns] = data.loc[index-1, columns]
        self._constituent_config_dao.update_status(date, stock, 0)
        return data

class FutrueDataMissingFixer(DataFixer):
    """
    利用琦哥给的数据修复期货的tick数据
    数据路径：D:\liuli\data\patch\future\missed_data
    """

    def __init__(self):
        self._path = 'D:\\liuli\\data\\patch\\future\\missed_data\\'

    def fix(self, data, date):
        """
        修复方法：
        加载修复数据源填充到当前数据集中
        Parameters
        ----------
        data

        Returns
        -------

        """
        instrument = data['instrument'].iloc[0]
        data['date'] = data['datetime'].str[0:10]
        patch_filename = instrument + '.CCFX' + date + ' 13_50_10.pkl'
        patch_data_path = self._path + patch_filename
        patch_data = pd.DataFrame(pd.read_pickle(patch_data_path))
        patch_data.to_csv('E:\\data\\temp\\patch_data.csv')
        patch_data['datetime'] = patch_data.apply(lambda item: self.time_transform(item), axis=1)
        print(data[data['datetime'] == '2017-05-04 13:50:45.000000000'])
        print(patch_data[patch_data['datetime'] == '2017-05-04 13:50:45.000000000'])
        return data

    def time_transform(self, item):
        time = str(item['time'])
        time = time[0:4] + '-' + time[4:6] + '-' + time[6:8] + ' ' + time[8:10] + ':' + time[10:12] + ':' + time[12:] + '00000000'
        return time

def fix_stock_organized_data_daily(data):
    """
    具体的修复执行逻辑

    Parameters
    ----------
    data

    Returns
    -------

    """
    # 是否有中午休盘时的数据
    # if len(data[(data['time'] > add_milliseconds_suffix(STOCK_TRANSACTION_NOON_END_TIME)) & (
    #         data['time'] < add_milliseconds_suffix(STOCK_TRANSACTION_NOON_START_TIME))]) > 0:
    #     data = data.drop(data.index[(data['time'] > add_milliseconds_suffix(STOCK_TRANSACTION_NOON_END_TIME)) & (
    #         data['time'] < add_milliseconds_suffix(STOCK_TRANSACTION_NOON_START_TIME))])

    # 是否有补齐时未清零的数据
    # temp_data = data.copy(deep=True)
    # temp_data['delta_daily_accumulated_volume'] = temp_data['daily_accumulated_volume'] - temp_data['daily_accumulated_volume'].shift(1)
    # if len(temp_data[(temp_data['delta_daily_accumulated_volume'] == 0) & (temp_data['volume'] > 0)]) > 0:
    #     data.loc[(temp_data['delta_daily_accumulated_volume'] == 0) & (temp_data['volume'] > 0), ['volume','amount','transaction_number']] = 0

    # 是否有11：30：00的数据
    # if len(data[data['time'] == '11:30:00.000']) == 0:
    #     date = data.iloc[0]['date'].replace('-','')
    #     tscode = data.iloc[0]['tscode'][0:6]
    #     original_path = STOCK_TICK_DATA_PATH + os.path.sep + add_folder_prefix(date[0:4]) + os.path.sep + add_folder_prefix(date[0:6]) + os.path.sep + date + os.path.sep + tscode + '.pkl'
    #     original_data = read_decompress(original_path)
    #     original_data = StockTickDataColumnTransform().process(original_data)
    #     if len(original_data[original_data['time'] == '11:30:00.000']) > 0:
    #         temp_data = original_data[(original_data['time'] == '11:30:00.000') & (original_data['volume'] > 0)]
    #         if len(temp_data) > 0:
    #             # 如果时间点有多条记录优先选择有成交量的
    #             print(temp_data)
    #             data.append(temp_data.iloc[0])
    #         else:
    #             temp_data = original_data[(original_data['time'] == '11:30:00.000')]
    #             if len(temp_data) > 0:
    #                 data = data.append(temp_data.iloc[0])

    # 处理重复数据
    # data = data.reset_index(drop=True)
    # temp_data = data[(data['time'] <= STOCK_TRANSACTION_END_TIME + '.000') & (
    #             data['time'] >= STOCK_TRANSACTION_START_TIME + '.000') & (
    #                              (data['time'] >= STOCK_TRANSACTION_NOON_START_TIME + '.000') | (
    #                                  data['time'] <= STOCK_TRANSACTION_NOON_END_TIME + '.000'))]
    # temp_data = temp_data.groupby('time')[['time']].count()
    # temp_data = temp_data[temp_data['time'] > 1]
    # if len(temp_data) > 0:
    #     for index_time in temp_data.index:
    #         tdata = data[data['time'] == index_time]
    #         if len(tdata) > 0:
    #             for index in tdata.index:
    #                 cur_daily_accumulated_volume = data.loc[index]['daily_accumulated_volume']
    #                 if type(cur_daily_accumulated_volume) != int:
    #                     cur_daily_accumulated_volume = cur_daily_accumulated_volume.tolist()[0]
    #                 min_index = min(data[data['daily_accumulated_volume'] == cur_daily_accumulated_volume].index)
    #                 if min_index != index:
    #                     data = data.drop(index)

    # 处理收盘集合竞价
    # 从原始数据获取收盘集合竞价成交记录
    date = data.iloc[0]['date'].replace('-', '')
    tscode = data.iloc[0]['tscode'][0:6]
    original_path = STOCK_TICK_DATA_PATH + os.path.sep + add_folder_prefix(date[0:4]) + os.path.sep + add_folder_prefix(
        date[0:6]) + os.path.sep + date + os.path.sep + tscode + '.pkl'
    original_data = read_decompress(original_path)
    original_data = StockTickDataColumnTransform().process(original_data)
    original_data = StockTickDataCleaner().process(original_data)
    original_data = original_data[original_data['volume'] > 0]
    if len(original_data) > 0:
        closing_call_action_record = original_data.iloc[-1]
        need_insert = True
        if len(data[data['time'] == constants.STOCK_TRANSACTION_END_TIME + '.000']) > 0:
            data = data.drop(data.index[data['time'] == constants.STOCK_TRANSACTION_END_TIME + '.000'])
            need_insert = False # 已有15：00：00的记录 无需插值
        closing_call_action_record['time'] = constants.STOCK_TRANSACTION_END_TIME + '.000'
        data = data.append(closing_call_action_record)
        print(data.columns.tolist())
        data = data.drop(data.index[data['time'] > constants.STOCK_TRANSACTION_END_TIME + '.000'])
        # 插值
        if need_insert:
            miss_data = pd.DataFrame(columns=data.columns.tolist())
            print(miss_data.columns.tolist())
            str_last_time = data[data['time'] < constants.STOCK_TRANSACTION_END_TIME + '.000']['time'].max()
            last_time = datetime.strptime(str_last_time, "%H:%M:%S.%f")
            final_time = datetime.strptime(constants.STOCK_TRANSACTION_END_TIME + '.000', "%H:%M:%S.%f")
            time_interval = (final_time - last_time).total_seconds()
            step = 3
            while step < time_interval:
                item = data[data['time'] == str_last_time].iloc[0]
                str_cur_time = item['time']
                time = time_advance(str_cur_time, step)
                item['time'] = time
                item['volume'] = 0
                item['amount'] = 0
                item['transaction_number'] = 0
                item['realtime'] = datetime.strptime(time, "%H:%M:%S.%f")
                item['delta_time'] = timedelta(seconds = 3)
                item['delta_time_sec'] = 3
                miss_data = miss_data.append(item)
                step = step + constants.STOCK_TICK_SAMPLE_INTERVAL
            data = data.append(miss_data)

    data = data.sort_values(by=['time'])
    data = data.reset_index(drop=True)
    return data


def validate_stock_original_data(validate_code, include_year_list=[]):
    """
    检查股票原始数据

    Parameters
    ----------
    validate_code
    include_year_list

    Returns
    -------

    """
    session = create_session()
    checked_list = session.execute(
        'select concat(date, tscode) from stock_validation_result where validation_code = :vcode',
        {'vcode': validate_code})
    checked_set = set(map(lambda item: item[0], checked_list))
    runner = ProcessRunner(10)
    year_folder_list = list_files_in_path(STOCK_TICK_DATA_PATH + os.path.sep)
    year_folder_list.sort()
    for year_folder in year_folder_list:
        years = re.search('[0-9]{4}', year_folder)
        if not years:
            continue
        elif len(include_year_list) > 0 and years.group() not in include_year_list:
            continue
        month_folder_list = list_files_in_path(STOCK_TICK_DATA_PATH + os.path.sep + year_folder + os.path.sep)
        month_folder_list.sort()
        for month_folder in month_folder_list:
            if not re.search('[0-9]{6}', month_folder):
                continue
            # runner.execute(validate_stock_original_by_month, args=(validate_code, checked_set, year_folder, month_folder))
            validate_stock_original_by_month(validate_code, checked_set, year_folder, month_folder)
    time.sleep(100000)

def validate_future_original_data(validate_code, product_list=[], instrument_list=[], include_date_list=[]):
    """
    检查期货原始数据

    Parameters
    ----------
    validate_code
    include_year_list

    Returns
    -------

    """
    session = create_session()
    checked_list = session.execute(
        'select concat(date, instrument) from future_validation_result where validation_code = :vcode', {'vcode': validate_code})
    checked_set = set(map(lambda item: item[0], checked_list))
    runner = ProcessRunner(10)
    product_folder_list = list_files_in_path(FUTURE_TICK_DATA_PATH + os.path.sep)
    product_folder_list.sort()
    for product_folder in product_folder_list:
        if len(product_list) > 0 and product_folder not in product_list:
            continue
        runner.execute(validate_future_original_by_product, args=(validate_code, checked_set, product_folder, instrument_list, include_date_list))
        # validate_future_original_by_product(validate_code, checked_set, product_folder, instrument_list, include_date_list)
    time.sleep(100000)

def validate_future_original_by_product(validate_code, checked_set, product_folder, instrument_list=[], include_date_list=[]):
    """
    按品种检查期货数据，主要用于多进程并行

    Parameters
    ----------
    validate_code
    checked_set
    year_folder
    month_folder

    Returns
    -------

    """
    session = create_session()
    product = product_folder
    instrument_file_list = list_files_in_path(FUTURE_TICK_DATA_PATH + os.path.sep + product_folder + os.path.sep)
    instrument_file_list.sort()
    for instrument_file in instrument_file_list:
        instrument = instrument_file.split('.')[1]
        get_logger().debug('Handle instrument {0}'.format(instrument))
        if len(instrument_list) > 0 and instrument not in instrument_list:
            continue
        original_future_file_path = FUTURE_TICK_DATA_PATH + os.path.sep + product_folder + os.path.sep + instrument_file
        data = pd.read_csv(original_future_file_path)
        data = FutureTickDataColumnTransform(product, instrument).process(data)
        data = DataCleaner().process(data)
        data['date'] = data['datetime'].str[0:10]
        data['instrument'] = instrument
        date_list = list(set(data['date'].tolist()))
        date_list.sort()
        for date in date_list:
            if date + instrument not in checked_set:
                if len(include_date_list) > 0 and date not in include_date_list:
                    continue
                date_data = data[data['date'] == date].copy()
                validation_result = FutureDataMissingValidator().validate(date_data)
                future_validation_result = FutureValidationResult(validate_code, validation_result, len(date_data))
                session.add(future_validation_result)
                session.commit()
            else:
                get_logger().debug("Data:{0} instrument:{1} has been handled".format(date, instrument))

class FutureDataMissingValidator(FutureTickDataValidator):
    """
    检查原始期货数据


    Parameters
    ----------
    data : DataFrame
        待处理数据.

    """

    @classmethod
    def validate(self, data):
        date = data['date'].iloc[0]
        instrument = data['instrument'].iloc[0]
        result = DtoFutureValidationResult(RESULT_SUCCESS, [], instrument, date)
        get_logger().debug('Check date:{0} for instrument:{1}'.format(date, instrument))
        data['time'] = data['datetime'].apply(lambda item: datetime.strptime(item[11:23], '%H:%M:%S.%f'))
        data['delta_time'] = data['time'].shift(-1) - data['time']
        data['delta_time_sec'] = data['delta_time'].apply(lambda item: self.to_seconds(self, item))
        filter = data[(data['delta_time_sec'] > 300) & (data['delta_time_sec'] < 5400)]
        if len(filter) > 0:
            result.result = RESULT_FAIL
            result.error_details = ['Date {0} for instrument {1} has {2} issue count'.format(date, instrument, str(len(filter)))]
            result.issue_count = len(filter)
        return result

    def to_seconds(self, item):
        return item.total_seconds()
    @classmethod
    def compare_validate(self, target_data, compare_data):
        return True

    @classmethod
    def convert(self, target_data, compare_data):
        pass

def validate_stock_original_by_month(validate_code, checked_set, year_folder, month_folder):
    """
    按年检查股票原始数据，主要用于多进程并行执行

    Parameters
    ----------
    validate_code
    checked_set
    year_folder
    month_folder

    Returns
    -------

    """
    session = create_session()
    date_folder_list = list_files_in_path(
        STOCK_TICK_DATA_PATH + os.path.sep + year_folder + os.path.sep + month_folder + os.path.sep)
    date_folder_list.sort()
    get_logger().info(
        'Handle date for year {0} and month {1}: {2}'.format(year_folder, month_folder, '|'.join(date_folder_list)))
    for date in date_folder_list:
        if not re.search('[0-9]{8}', date):
            continue
        stock_file_list = list_files_in_path(
            STOCK_TICK_DATA_PATH + os.path.sep + year_folder + os.path.sep + month_folder + os.path.sep + date + os.path.sep)
        stock_file_list.sort()
        get_logger().debug('Handle date {0} for stock: {1}'.format(date, '|'.join(stock_file_list)))
        for stock in stock_file_list:
            if date + stock.split('.')[0] not in checked_set:
                try:
                    original_stock_file_path = STOCK_TICK_DATA_PATH + os.path.sep + year_folder + os.path.sep + month_folder + os.path.sep + date + os.path.sep + stock
                    data = read_decompress(original_stock_file_path)
                except Exception as e:
                    get_logger().error('Load file: {0} error'.format(original_stock_file_path))
                    continue
                print(date + ':' + stock)
                try:
                    data = StockTickDataColumnTransform().process(data)
                except Exception as e:
                    get_logger().error('Invalid data for {0} : {1}'.format(date, stock))
                    continue
                data = StockTickDataCleaner().process(data)
                # 停盘数据
                if len(data) == 0:
                    get_logger().warning('Empty data for {0} : {1}'.format(date, stock))
                    continue
                validation_result = TransactionTimeDataMissingValidator().validate(data)
                stock_validation_result = StockValidationResult(validate_code, validation_result, len(data))
                session.add(stock_validation_result)
                session.commit()
            else:
                get_logger().debug("{0} {1} has been handled".format(date, stock))

class ClosingCallAuctionValidator(Validator):
    """检查收盘集合竞价数据是不是来迟了

    Parameters
    ----------
    data : DataFrame
        待处理数据.

    """

    @classmethod
    def validate(self, data):
        tscode = data.iloc[0]['tscode']
        date = data.iloc[0]['date']
        result = DtoStockValidationResult(RESULT_SUCCESS, [], tscode.split('.')[0], date.replace('-', ''))
        data = data[data['volume'] > 0]
        if len(data) > 0:
            # 最后一个有成交量的时间
            time = data.iloc[-1]['time']
            # 最后有成交量的时间大于15：00：00
            if time > add_milliseconds_suffix(STOCK_TRANSACTION_END_TIME):
                result.result = RESULT_FAIL
                result.error_details.append('The closing call auction time {0} larger than closing time'.format(time))
        return result

    @classmethod
    def compare_validate(self, target_data, compare_data):
        return True

    @classmethod
    def convert(self, target_data, compare_data):
        pass


class CollectionBiddingDuplicateDataValidator(Validator):
    """检查集合竞价时段是不是有同一个时间的重复数据

    Parameters
    ----------
    data : DataFrame
        待处理数据.

    """

    @classmethod
    def validate(self, data):
        tscode = data.iloc[0]['tscode']
        date = data.iloc[0]['date']
        result = DtoStockValidationResult(RESULT_SUCCESS, [], tscode.split('.')[0], date.replace('-', ''))
        temp_data = data[(data['time'] >= '09:15:00.000') & (data['time'] < '09:25:00.000')]
        temp_data_group_by = temp_data.groupby('time').count()['count']
        temp_data = temp_data.merge(temp_data_group_by, on=['time'], how='left')
        temp_data = temp_data[temp_data['count_y'] > 1]
        if len(temp_data) > 0:
            get_logger().error(temp_data["time"])
            result.result = RESULT_FAIL
            result.error_details.append('There are collection bidding duplicate data')
        return result

    @classmethod
    def compare_validate(self, target_data, compare_data):
        return True

    @classmethod
    def convert(self, target_data, compare_data):
        pass

class ClosingDuplicateDataValidator(StockTickDataValidator):
    """检查15:00时间点上有多条数据，且有成交量不为0的数据

    Parameters
    ----------
    data : DataFrame
        待处理数据.

    """

    @classmethod
    def validate(self, data):
        tscode = data.iloc[0]['tscode']
        date = data.iloc[0]['date']
        result = DtoStockValidationResult(RESULT_SUCCESS, [], tscode.split('.')[0], date.replace('-', ''))
        temp_data = data[(data['time'] == '15:00:00.000')]
        if len(temp_data) > 1:
            # 这里要保证有成交量的记录
            temp_data = temp_data[temp_data['volume'] > 0]
            if len(temp_data) > 0:
                result.result = RESULT_FAIL
                result.error_details.append('There are multiple records at 15:00')
        return result

    @classmethod
    def compare_validate(self, target_data, compare_data):
        return True

    @classmethod
    def convert(self, target_data, compare_data):
        pass


class TransactionTimeDataMissingValidator(StockTickDataValidator):
    """检查原始股票数据，9:30-11:30 13:00-15:00交易时间有数据缺失

    Parameters
    ----------
    data : DataFrame
        待处理数据.

    """

    @classmethod
    def validate(self, data):
        tscode = data.iloc[0]['tscode']
        date = data.iloc[0]['date']
        result = DtoStockValidationResult(RESULT_SUCCESS, [], tscode.split('.')[0], date.replace('-', ''))
        if len(data) == 1:
            return result
        morning_data = data[(data['time'] >= add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)) & (data['time'] <= add_milliseconds_suffix(STOCK_TRANSACTION_NOON_END_TIME))]
        if len(morning_data) == 0:
            result.result = RESULT_FAIL
            result.error_details.append('There are data missing for transaction time')
            return result
        morning_start_time = morning_data.iloc[0]['time']
        morning_data['time_sec'] = morning_data.apply(lambda item: self.get_time_sec(self, morning_start_time, item['time']), axis = 1)
        morning_data['delta_time'] = morning_data['time_sec'] - morning_data['time_sec'].shift(1)
        noon_data = data[(data['time'] >= add_milliseconds_suffix(STOCK_TRANSACTION_NOON_START_TIME)) & (data['time'] <= add_milliseconds_suffix(STOCK_TRANSACTION_END_TIME))]
        if len(noon_data) == 0:
            result.result = RESULT_FAIL
            result.error_details.append('There are data missing for transaction time')
            return result
        noon_start_time = noon_data.iloc[0]['time']
        noon_data['time_sec'] = noon_data.apply(lambda item: self.get_time_sec(self, noon_start_time, item['time']), axis = 1)
        noon_data['delta_time'] = noon_data['time_sec'] - noon_data['time_sec'].shift(1)
        if len(morning_data[morning_data['delta_time'] > 15]) > 0 or len(noon_data[noon_data['delta_time'] > 15]) > 0:
            print(morning_data[morning_data['delta_time'] > 15])
            print(noon_data[noon_data['delta_time'] > 15])
            result.result = RESULT_FAIL
            result.error_details.append('There are data missing for transaction time and missing number: {}'.format(str(len(morning_data[morning_data['delta_time'] > 15]) + len(noon_data[noon_data['delta_time'] > 15]))))
        return result

    def get_time_sec(self, start_time, end_time):
        return time_difference(start_time, end_time)

    @classmethod
    def compare_validate(self, target_data, compare_data):
        return True

    @classmethod
    def convert(self, target_data, compare_data):
        pass

class ZeroTransactionNumberValidator(Validator):
    """检查transaction_number为0的数据
    直接检查生成数据
    先选择transaction_number不为0的第一条数据的时间点
    如果大于这个时间点还有transaction_number为0的数据视为需要修正的数据

    Parameters
    ----------
    data : DataFrame
        待处理数据.

    """

    @classmethod
    def validate(self, data):
        tscode = data.iloc[0]['tscode']
        date = data.iloc[0]['date']
        result = DtoStockValidationResult(RESULT_SUCCESS, [], tscode.split('.')[0], date.replace('-', ''))
        temp_data = data[(data['transaction_number'] != 0)]
        check_time = temp_data.iloc[0]['time']
        temp_data = data[(data['time'] > check_time) & (data['transaction_number'] == 0)]
        if len(temp_data) > 1:
            result.result = RESULT_FAIL
            result.error_details.append('There are transaction_number records with 0 value')
        return result

    @classmethod
    def compare_validate(self, target_data, compare_data):
        return True

    @classmethod
    def convert(self, target_data, compare_data):
        pass

class NotClearedAfterCompletionValidator(Validator):
    """补齐插值时需要把成交量和成交额清0，因为是从上一个时刻照搬下来的，检查是不是有没有清零的数据

    Parameters
    ----------
    data : DataFrame
        待处理数据.

    """

    @classmethod
    def validate(self, data):
        tscode = data.iloc[0]['tscode']
        date = data.iloc[0]['date']
        result = DtoStockValidationResult(RESULT_SUCCESS, [], tscode.split('.')[0], date.replace('-', ''))
        data['delta_daily_accumulated_volume'] = data['daily_accumulated_volume'] - data['daily_accumulated_volume'].shift(1)
        if len(data[(data['delta_daily_accumulated_volume'] == 0) & (data['volume'] > 0)]) > 0:
            get_logger().error(data[(data['delta_daily_accumulated_volume'] == 0) & (data['volume'] > 0)][['time','delta_daily_accumulated_volume','volume']])
            result.result = RESULT_FAIL
            result.error_details.append('Invalid volume should to be fixed')
        return result

    @classmethod
    def compare_validate(self, target_data, compare_data):
        return True

    @classmethod
    def convert(self, target_data, compare_data):
        pass

class RepeatRecordValidator(Validator):
    """检查是否有重复数据, 只检查交易时间

    Parameters
    ----------
    data : DataFrame
        待处理数据.

    """

    @classmethod
    def validate(self, data):
        tscode = data.iloc[0]['tscode']
        date = data.iloc[0]['date']
        result = DtoStockValidationResult(RESULT_SUCCESS, [], tscode.split('.')[0], date.replace('-', ''))
        temp_data = data[(data['time'] <= add_milliseconds_suffix(STOCK_TRANSACTION_END_TIME)) & (data['time'] >= add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)) &
                         ((data['time'] >= add_milliseconds_suffix(STOCK_TRANSACTION_NOON_START_TIME)) | (data['time'] <= add_milliseconds_suffix(STOCK_TRANSACTION_NOON_END_TIME)))]
        temp_data = temp_data.groupby('time')[['time']].count()
        temp_data = temp_data[temp_data['time'] > 1]
        if len(temp_data) > 0:
            get_logger().error(temp_data)
            result.result = RESULT_FAIL
            result.error_details.append('Repeat records')
        return result

    @classmethod
    def compare_validate(self, target_data, compare_data):
        return True

    @classmethod
    def convert(self, target_data, compare_data):
        pass

class CloseRecordMissingValidator(Validator):
    """检查生成数据是否有15：00和11：30时间点的记录

    Parameters
    ----------
    data : DataFrame
        待处理数据.

    """

    @classmethod
    def validate(self, data):
        tscode = data.iloc[0]['tscode']
        date = data.iloc[0]['date']
        result = DtoStockValidationResult(RESULT_SUCCESS, [], tscode.split('.')[0], date.replace('-', ''))
        temp_data1 = data[(data['time'] == add_milliseconds_suffix(STOCK_TRANSACTION_END_TIME))]
        temp_data2 = data[(data['time'] == add_milliseconds_suffix(STOCK_TRANSACTION_NOON_END_TIME))]
        if len(temp_data1) == 0 or len(temp_data2) == 0:
            result.result = RESULT_FAIL
            result.error_details.append('The close record is missing')
        return result

    @classmethod
    def compare_validate(self, target_data, compare_data):
        return True

    @classmethod
    def convert(self, target_data, compare_data):
        pass

class TenGradeFiveGradeDataValidator(Validator):
    """检查是否有10档5档的委比数据

    Parameters
    ----------
    data : DataFrame
        待处理数据.

    """

    @classmethod
    def validate(self, data):
        tscode = data.iloc[0]['tscode']
        date = data.iloc[0]['date']
        result = DtoStockValidationResult(RESULT_SUCCESS, [], tscode.split('.')[0], date.replace('-', ''))
        columns = data.columns
        for column in ['10_grade_bid_amount','10_grade_ask_amount','5_grade_bid_amount','5_grade_ask_amount']:
            if column not in columns:
                result.result = RESULT_FAIL
                result.error_details.append('10 and 5 grade data not enriched')
        return result

    @classmethod
    def compare_validate(self, target_data, compare_data):
        return True

    @classmethod
    def convert(self, target_data, compare_data):
        pass


class TestValidator(Validator):
    """测试

    Parameters
    ----------
    data : DataFrame
        待处理数据.

    """

    @classmethod
    def validate(self, data):
        tscode = data.iloc[0]['tscode']
        date = data.iloc[0]['date']
        result = DtoStockValidationResult(RESULT_SUCCESS, [], tscode.split('.')[0], date.replace('-', ''))
        return result

    @classmethod
    def compare_validate(self, target_data, compare_data):
        return True

    @classmethod
    def convert(self, target_data, compare_data):
        pass

class NoVolumeDataValidator(Validator):
    """检查全天没成交量的数据

    Parameters
    ----------
    data : DataFrame
        待处理数据.

    """

    @classmethod
    def validate(self, data):
        tscode = data.iloc[0]['tscode']
        date = data.iloc[0]['date']
        result = DtoStockValidationResult(RESULT_SUCCESS, [], tscode.split('.')[0], date.replace('-', ''))
        empty_records = len(data[data['volume'] == 0])
        all_records = len(data)
        if all_records > 0 and empty_records/all_records > 0.999:
            result.result = RESULT_FAIL
            result.error_details.append('There are plenty of 0 volume data')
        return result

    @classmethod
    def compare_validate(self, target_data, compare_data):
        return True

    @classmethod
    def convert(self, target_data, compare_data):
        pass

class BidOrAskMissingDataValidator(Validator):
    """时间段数据缺失，如：2021/2/24 10:47:18 - 11:05:00 000001

    Parameters
    ----------
    data : DataFrame
        待处理数据.

    """

    _columns = ['time', 'bid_price1', 'bid_volume1', 'bid_price2', 'bid_volume2','bid_price3', 'bid_volume3','bid_price4', 'bid_volume4','bid_price5', 'bid_volume5','bid_price6', 'bid_volume6','bid_price7', 'bid_volume7','bid_price8', 'bid_volume8','bid_price9', 'bid_volume9','bid_price10', 'bid_volume10',
                             'ask_price1', 'ask_volume1', 'ask_price2', 'ask_volume2', 'ask_price3', 'ask_volume3','ask_price4', 'ask_volume4','ask_price5', 'ask_volume5','ask_price6', 'ask_volume6','ask_price7', 'ask_volume7','ask_price8', 'ask_volume8','ask_price9', 'ask_volume9','ask_price10', 'ask_volume10']

    @classmethod
    def validate(self, data):
        tscode = data.iloc[0]['tscode']
        date = data.iloc[0]['date']
        if tscode == '600340.SH' and date == '2017-04-05':
            print('aa')
        result = DtoStockValidationResult(RESULT_SUCCESS, [], tscode.split('.')[0], date.replace('-', ''))
        temp_data = data[(data['time'] < add_milliseconds_suffix(STOCK_CLOSE_CALL_AUACTION_START_TIME)) & (
                    data['time'] > add_milliseconds_suffix(STOCK_OPEN_CALL_AUACTION_2ND_STAGE_END_TIME)) &
                         ((data['time'] >= add_milliseconds_suffix(STOCK_TRANSACTION_NOON_START_TIME)) | (
                                     data['time'] <= add_milliseconds_suffix(STOCK_TRANSACTION_NOON_END_TIME)))]
        temp_data = temp_data[self._columns]
        if len(temp_data[temp_data.values == 0]) > 0:
            print(temp_data[temp_data.values == 0])
            result.result = RESULT_FAIL
            result.error_details.append('The bid or ask data is missing')
            result.issue_count = len(set(temp_data[temp_data.values == 0].index.tolist()))
        return result

    @classmethod
    def compare_validate(self, target_data, compare_data):
        return True

    @classmethod
    def convert(self, target_data, compare_data):
        pass

class IncompleteDataValidator(Validator):
    """数据缺失检查 2021/8/3 000158
    最大时间小于15：00：00

    Parameters
    ----------
    data : DataFrame
        待处理数据.

    """

    @classmethod
    def validate(self, data):
        tscode = data.iloc[0]['tscode']
        date = data.iloc[0]['date']
        result = DtoStockValidationResult(RESULT_SUCCESS, [], tscode.split('.')[0], date.replace('-', ''))
        max_time = data['time'].max()
        if max_time < add_milliseconds_suffix(STOCK_TRANSACTION_END_TIME):
            result.result = RESULT_FAIL
            result.error_details.append('The data is incomplete and last time is {0}'.format(max_time))
        return result

    @classmethod
    def compare_validate(self, target_data, compare_data):
        return True

    @classmethod
    def convert(self, target_data, compare_data):
        pass

def validate_factor_data(validate_code, factor, product='', date_list=[]):
    """
    检查因子数据

    Parameters
    ----------
    validate_code
    factor
    product
    date_list

    Returns
    -------

    """
    session = create_session()
    checked_list = session.execute('select concat(instrument, date) from factor_validation_result where validation_code = :vcode', {'vcode': validate_code})
    checked_set = set(map(lambda item : item[0], checked_list))
    runner = ProcessRunner(10)
    if product == '':
        products = [product]
    else:
        products = STOCK_INDEX_PRODUCTS
    for product in products:
        data = factor.load(product)
        instruments = list(set(data['instrument'].tolist()))
        instruments.sort()
        for instrument in instruments:
            runner.execute(validate_factor_by_instrument, args=(validate_code, checked_set, factor, product, instrument))
            # validate_factor_by_instrument(validate_code, checked_set, instrument)
    time.sleep(100000)

def validate_factor_by_instrument(validate_code, checked_set, factor, product, instrument):
    """
    按合约检查因子数据，用于多进程并行运算

    Parameters
    ----------
    validate_code
    checked_set
    factor
    product
    instument

    Returns
    -------

    """
    session = create_session()
    data = factor.load(product)
    instrument_data = data[data['instrument'] == instrument]
    dates = list(set(instrument_data['date'].tolist()))
    dates.sort()
    for date in dates:
        if instrument + date not in checked_set:
            date_data = data[(data['instrument'] == instrument) & (data['date'] == date)]
            validation_result = CloseRecordMissingValidator().validate(date_data)
            factor_validation_result = FactorValidationResult(validate_code, validation_result, len(date_data))
            session.add(factor_validation_result)
            session.commit()

def load_and_analyze_stock_patch_data():
    """
    检查段兄给的股票数据，目录：D:\liuli\data\patch\stock\missed_data
    Returns
    -------

    """
    path = 'D:\\liuli\\data\\patch\\stock\\missed_data\\missed_data.pickle'
    with open(path, 'rb') as file:
        data = pickle.load(file)
    date_list = list(data.keys())
    date_list.sort()
    df = data['2017-02-20']
    print(date_list)
    print(df.index)

def load_and_analyze_stock_patch_data_for_10_grade_commission():
    """
    服用修复框架，只是修改查询条件
    检查段兄给的股票数据修复10档委买委卖，目录：D:\liuli\data\patch\stock\patch_for_ten_grade_commission
    Returns
    -------

    """
    session = create_session()
    # 针对第一次给段兄提供的查询条件
    checked_list = session.execute("select concat(date, tscode) from stock_validation_result where validation_code = '20230309-finley' and result = 1 and issue_count > 4000")
    checked_set = set(map(lambda item: item[0], checked_list))
    runner = ProcessRunner(20)
    year_folder_list = list_files_in_path(STOCK_TICK_ORGANIZED_DATA_PATH + os.path.sep)
    for year_folder in year_folder_list:
        years = re.search('[0-9]{4}', year_folder)
        if not years:
            continue
        month_folder_list = list_files_in_path(STOCK_TICK_ORGANIZED_DATA_PATH + os.path.sep + year_folder + os.path.sep)
        for month_folder in month_folder_list:
            if not re.search('[0-9]{6}', month_folder):
                continue
            # runner.execute(fix_stock_organized_data_by_month, args=('20230309-finley', checked_set, year_folder, month_folder))
            fix_stock_organized_data_by_month('20230309-finley', checked_set, year_folder, month_folder)
    time.sleep(100000)



if __name__ == '__main__':
    # fix_stock_tick_data('2021', '01', ['20210108','20210111','20210112','20210113','20210114','20210115','20210118','20210119','20210120','20210121','20210122','20210125','20210126','20210127','20210128','20210129'])
    # fix_stock_tick_data('2021', '03', ['20210326','20210329','20210330','20210331'])
    # fix_stock_tick_data('2021', '11', ['20211130'])
    # fix_stock_tick_data('2021', '07', ['20210723'])
    # fix_stock_tick_data('2021', '04', ['20210430'])
    # fix_stock_tick_data('2020', '12', ['20201207'])
    # fix_stock_tick_data('2020', '11', ['20201105'])
    # fix_stock_tick_data('2020', '10', ['20201028','20201015'])
    # fix_stock_tick_data('2020', '09', ['20200918'])
    # fix_stock_tick_data('2020', '08', ['20200831','20200803'])
    # fix_stock_tick_data('2020', '07', ['20200731','20200730','20200729','20200728','20200727','20200723'])
    # fix_stock_tick_data('2020', '05', ['20200526','20200522','20200521','20200520','20200513','20200512','20200511','20200508','20200507','20200506'])
    # fix_stock_tick_data('2020', '04', ['20200430','20200429','20200428','20200417','20200416','20200415','20200414','20200413'])
    # fix_stock_tick_data('2020', '03', ['20200306','20200305','20200304','20200303','20200302'])
    # fix_stock_tick_data('2019', '08', ['20190826','20190823','20190822','20190821','20190820','20190819','20190816'])
    # fix_stock_tick_data('2019', '07', ['20190731','20190724','20190722','20190719','20190718','20190717','20190716','20190711'])
    # fix_stock_tick_data('2019', '06', ['20190621','20190620','20190619','20190618','20190614','20190613','20190612','20190611','20190606','20190605','20190604'])
    # fix_stock_tick_data('2019', '05', ['20190531','20190529','20190528','20190527','20190524','20190523','20190522','20190521','20190520','20190517','20190516','20190515','20190514','20190513','20190510','20190509','20190507','20190506'])
    # fix_stock_tick_data('2019', '04', ['20190430','20190429','20190426','20190425','20190424','20190423','20190409','20190404','20190403','20190402','20190401'])
    # fix_stock_tick_data('2019', '03', ['20190329','20190328','20190327','20190326','20190325','20190322','20190321','20190320','20190319','20190318','20190315','20190314','20190313','20190307','20190306','20190304','20190301'])
    # fix_stock_tick_data('2019', '02', ['20190228','20190227','20190225','20190220'])
    # fix_stock_tick_data('2019', '01', ['20190125','20190124','20190123','20190122','20190121','20190109','20190108','20190107','20190104','20190103','20190102'])
    # fix_stock_tick_data('2018', '12', ['20181228','20181227','20181226','20181225','20181220','20181219','20181218','20181217','20181214','20181213','20181212','20181211','20181210','20181207','20181206','20181205','20181204','20181203'])
    # fix_stock_tick_data('2018', '08', ['20180817','20180816','20180814','20180813','20180810','20180807','20180806','20180802','20180801'])
    # fix_stock_tick_data('2018', '07', ['20180731','20180727','20180726','20180725','20180724','20180720','20180718','20180716','20180713','20180711','20180710'])
    # fix_stock_tick_data('2018', '05', ['20180531','20180530','20180529','20180522'])
    # fix_stock_tick_data('2017', '12', ['20171214','20171212'])
    # fix_stock_tick_data('2017', '11', ['20171130','20171127'])
    # fix_stock_tick_data('2017', '06', ['20170626','20170607'])
    # fix_stock_tick_data('2017', '05', ['20170531','20170508','20170504','20170502'])
    # fix_stock_tick_data('2017', '03', ['20170307'])
    # fix_stock_tick_data('2017', '02', ['20170203'])
    # fix_stock_tick_data('2017', '01', ['20170113'])

    # check_issue_stock_validation_data('0c7cfff7-bbb6-419b-bc5a-02dbdabfa645')
    # check_issue_stock_validation_data('603775c1-8121-4515-a578-935185249f94')
    # check_issue_stock_validation_data('89feaeaa-6b07-43e8-977d-7aaea0fdae50')
    # check_issue_stock_validation_data('081727da-a3ad-43be-b319-21c4f8cb382d')

    #检查已生成股票数据
    # validate_stock_organized_data('temp')

    #检查原始股票数据
    # validate_stock_original_data('20230320-finley')

    #修复有问题股票数据
    # fix_stock_organized_data('20230309-finley')

    #检查原始期货数据
    # validate_future_original_data('20230329-future-finley')

    #修复有问题的期货数据
    # fix_future_organized_data('20230329-future-finley')


    #检查收盘集合竞价数据是不是来迟了
    # data = read_decompress("E:\\data\\fix\\4th\\000333-20220617\\000333.original.pkl")
    # data = StockTickDataColumnTransform().process(data)
    # data = StockTickDataCleaner().process(data)
    # validation_result = ClosingCallAuctionValidator().validate(data)
    # print(validation_result)

    # 检查集合竞价阶段是否有重复数据
    # data = read_decompress('D:\\liuli\\data\\original\\stock\\tick\\stk_tick10_w_2018\\stk_tick10_w_201812\\20181225\\600126.pkl')
    # data = StockTickDataColumnTransform().process(data)
    # data = StockTickDataCleaner().process(data)
    # validation_result = CollectionBiddingDuplicateDataValidator().validate(data)
    # print(validation_result)

    # validator = TransactionTimeDataMissingValidator()
    # data = read_decompress('D:\\liuli\\data\\original\\stock\\tick\\stk_tick10_w_2017\\stk_tick10_w_201711\\20171110\\002690.pkl')
    # data = StockTickDataColumnTransform().process(data)
    # data = StockTickDataCleaner().process(data)
    # print(validator.validate(data))

    # data = read_decompress("E:\\data\\organized\\stock\\tick\\stk_tick10_w_2019\\stk_tick10_w_201909\\20190906\\002081.pkl")
    # data = read_decompress("E:\\data\\organized\\stock\\tick\\stk_tick10_w_2022\\stk_tick10_w_202203\\20220324\\000858.pkl")
    # data = read_decompress("E:\\data\\organized\\stock\\tick\\stk_tick10_w_2022\\stk_tick10_w_202206\\20220617\\000333.pkl")
    # data = read_decompress("E:\\data\\organized\\stock\\tick\\stk_tick10_w_2020\\stk_tick10_w_202009\\20200922\\300059.pkl")
    # data = fix_stock_organized_data_daily(data)
    # save_compress(data, 'E:\\data\\fix\\4th\\300059-20200922\\300059.fix.pkl')

    # 单独测试validator
    # validator = ZeroTransactionNumberValidator()
    # data = read_decompress('E:\\data\\organized\\stock\\tick\\stk_tick10_w_2022\\stk_tick10_w_202205\\20220517\\000156.pkl')
    # print(validator.validate(data))

    # validator = CloseRecordMissingValidator()
    # data = read_decompress('E:\\data\\organized\\stock\\tick\\stk_tick10_w_2022\\stk_tick10_w_202206\\20220602\\002459.pkl')
    # print(validator.validate(data))

    # validator = TenGradeFiveGradeDataValidator()
    # data = read_decompress('E:\\data\\organized\\stock\\tick\\stk_tick10_w_2022\\stk_tick10_w_202206\\20220602\\002459.pkl')
    # print(validator.validate(data))

    # validator = NotClearedAfterCompletionValidator()
    # data = read_decompress('x')
    # print(validator.validate(data))

    # validator = RepeatRecordValidator()
    # data = read_decompress('E:\\data\\organized\\stock\\tick\\stk_tick10_w_2020\\stk_tick10_w_202004\\20200424\\600859.pkl')
    # print(validator.validate(data))

    # validator = NoVolumeDataValidator()
    # data = read_decompress('E:\\data\\organized\\stock\\tick\\stk_tick10_w_2022\\stk_tick10_w_202206\\20220602\\000563.pkl')
    # print(validator.validate(data))

    # validator = BidOrAskMissingDataValidator()
    # data = read_decompress('E:\\data\\organized\\stock\\tick\\stk_tick10_w_2017\\stk_tick10_w_201702\\20170220\\000839.pkl')
    # print(validator.validate(data))

    # validator = FutureDataMissingValidator()
    # data = pd.read_csv('D:\\liuli\\data\\original\\future\\tick\\IC\\CFFEX.IC1705.csv')
    # print(validator.validate(data, 'IC1705'))

    # 单独测试fixer
    # fixer = CollectionBiddingDuplicateDataFixer()
    # data = read_decompress('E:\\data\\organized\\stock\\tick\\stk_tick10_w_2018\\stk_tick10_w_201812\\20181226\\000627.pkl')
    # data = fixer.fix(data)
    # save_compress(data, 'E:\\data\\temp\\000627_fix.pkl')

    # fixer = ClosingDuplicateDataFixer()
    # data = read_decompress('E:\\data\\organized\\stock\\tick\\stk_tick10_w_2018\\stk_tick10_w_201811\\20181108\\600369.pkl')
    # data = fixer.fix(data)
    # save_compress(data, 'E:\\data\\temp\\aa.pkl')

    # fixer = ZeroTransactionNumberFixer()
    # data = read_decompress('E:\\data\\organized\\stock\\tick\\stk_tick10_w_2022\\stk_tick10_w_202205\\20220517\\000156.pkl')
    # data = fixer.fix(data)
    # data.to_csv('E:\\data\\temp\\000156_fix.csv')

    # fixer = CloseRecordMissingFixer()
    # data = read_decompress('E:\\data\\organized\\stock\\tick\\stk_tick10_w_2019\\stk_tick10_w_201908\\20190830\\600642.pkl')
    # data = fixer.fix(data)
    # save_compress(data, 'E:\\data\\temp\\aa.pkl')
    # data = read_decompress('E:\\data\\organized\\stock\\tick\\stk_tick10_w_2017\\stk_tick10_w_201704\\20170405\\600649.pkl')
    # data = fixer.fix(data)
    # save_compress(data, 'E:\\data\\temp\\bb.pkl')
    # data = read_decompress('E:\\data\\organized\\stock\\tick\\stk_tick10_w_2022\\stk_tick10_w_202202\\20220216\\600704.pkl')
    # data = fixer.fix(data)
    # save_compress(data, 'E:\\data\\temp\\cc.pkl')
    # data = read_decompress('E:\\data\\organized\\stock\\tick\\stk_tick10_w_2022\\stk_tick10_w_202206\\20220602\\002948.pkl')
    # data = fixer.fix(data)
    # save_compress(data, 'E:\\data\\temp\\dd.pkl')
    # data = read_decompress('E:\\data\\organized\\stock\\tick\\stk_tick10_w_2022\\stk_tick10_w_202205\\20220531\\300253.pkl')
    # data = fixer.fix(data)
    # save_compress(data, 'E:\\data\\temp\\ee.pkl')
    # data = read_decompress('E:\\data\\organized\\stock\\tick\\stk_tick10_w_2022\\stk_tick10_w_202205\\20220520\\601818.pkl')
    # data = fixer.fix(data)
    # data.to_csv('E:\\data\\temp\\601818_fix.csv')

    # fixer = NoVolumeDataFixer()
    # data = read_decompress('E:\\data\\organized\\stock\\tick\\stk_tick10_w_2022\\stk_tick10_w_202206\\20220601\\688561.pkl')
    # data = fixer.fix(data)
    # data.to_csv('E:\\data\\temp\\688561_fix.csv')

    # fixer = NotClearedAfterCompletionFixer()
    # data = read_decompress('E:\\data\\organized\\stock\\tick\\stk_tick10_w_2020\\stk_tick10_w_202012\\20201210\\000415.pkl')
    # data = fixer.fix(data)
    # data.to_csv('E:\\data\\temp\\000415_fix.csv')

    # fixer = RepeatRecordFixer()
    # data = read_decompress('E:\\data\\organized\\stock\\tick\\stk_tick10_w_2020\\stk_tick10_w_202004\\20200424\\600859.pkl')
    # data = fixer.fix(data)
    # data.to_csv('E:\\data\\temp\\600859_fix.csv')

    # fixer = TenGradeFiveGradeDataFixer()
    # data = read_decompress('E:\\data\\organized\\stock\\tick\\stk_tick10_w_2020\\stk_tick10_w_202001\\20200102\\000039.pkl')
    # data = fixer.fix(data)
    # data.to_csv('E:\\data\\temp\\000039_fix.csv')

    # fixer = DefaultDataFixer()
    # data = read_decompress('E:\\data\\organized\\stock\\tick\\stk_tick10_w_2021\\stk_tick10_w_202105\\20210507\\600338.pkl')
    # data = fixer.fix(data)

    # fixer = BidOrAskMissingDataFixer()
    # # data = read_decompress('E:\\data\\organized\\stock\\tick\\stk_tick10_w_2020\\stk_tick10_w_202002\\20200203\\000006.pkl')
    # data = read_decompress('E:\\data\\organized\\stock\\tick\\stk_tick10_w_2019\\stk_tick10_w_201908\\20190819\\000006.pkl')
    # # data = read_decompress('E:\\data\\organized\\stock\\tick\\stk_tick10_w_2021\\stk_tick10_w_202107\\20210712\\000063.pkl')
    # data = fixer.fix(data)
    # data.to_csv('E:\\data\\temp\\20190819_000006_fix.csv')

    fixer = FutrueDataMissingFixer()
    data = read_decompress('E:\\data\\organized\\future\\tick\\IC\\IC1705.pkl')
    data['instrument'] = 'IC1705'
    data = fixer.fix(data, '2017-05-04')

    # 利用段兄给的数据修补股票数据
    # load_and_analyze_stock_patch_data()

    # 利用段兄给的数据修补10档委比数据
    # load_and_analyze_stock_patch_data_for_10_grade_commission()

    # 利用琦哥给的数据修复
