#! /usr/bin/env python
# -*- coding:utf8 -*-
import os
import re
import time
from datetime import datetime, timedelta

import pandas as pd

from common import constants
from common.aop import timing
from common.constants import FUTURE_TICK_DATA_PATH, FUTURE_TICK_FILE_PREFIX, FUTURE_TICK_COMPARE_DATA_PATH, \
    STOCK_TICK_DATA_PATH, CONFIG_PATH, FUTURE_TICK_TEMP_DATA_PATH, FUTURE_TICK_ORGANIZED_DATA_PATH, RESULT_SUCCESS, \
    STOCK_TICK_ORGANIZED_DATA_PATH, STOCK_TRANSACTION_NOON_END_TIME, STOCK_TRANSACTION_NOON_START_TIME, STOCK_TRANSACTION_END_TIME, STOCK_TRANSACTION_START_TIME, RESULT_FAIL
from common.localio import list_files_in_path, save_compress, read_decompress
from common.persistence.dbutils import create_session
from common.persistence.po import StockValidationResult, FutrueProcessRecord, StockProcessRecord
from data.process import FutureTickDataColumnTransform, StockTickDataColumnTransform, StockTickDataCleaner, DataCleaner, \
    FutureTickDataProcessorPhase1, FutureTickDataProcessorPhase2, StockTickDataEnricher
from data.validation import StockFilterCompressValidator, FutureTickDataValidator, StockTickDataValidator, StockOrganizedDataValidator, Validator, DtoStockValidationResult
from framework.localconcurrent import ProcessRunner
from common.timeutils import date_alignment, add_milliseconds_suffix, datetime_advance, time_advance


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
    return 'stk_tick10_w_' + folder

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
    file_prefix = 'stk_tick10_w_'
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
            runner.execute(validate_stock_organized_by_month, args=(validate_code, checked_set, year_folder, month_folder))
            # validate_stock_organized_by_month(validate_code, checked_set, year_folder, month_folder)
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
    for date in date_folder_list:
        if not re.search('[0-9]{8}', date):
            continue
        stock_file_list = list_files_in_path(
            STOCK_TICK_ORGANIZED_DATA_PATH + os.path.sep + year_folder + os.path.sep + month_folder + os.path.sep + date + os.path.sep)
        for stock in stock_file_list:
            if date + stock.split('.')[0] not in checked_set:
                try:
                    organized_stock_file_path = STOCK_TICK_ORGANIZED_DATA_PATH + os.path.sep + year_folder + os.path.sep + month_folder + os.path.sep + date + os.path.sep + stock
                    data = read_decompress(organized_stock_file_path)
                except Exception as e:
                    print('Load file: {0} error'.format(organized_stock_file_path))
                    continue
                print(date + ':' + stock)
                validation_result = StockOrganizedDataValidator().validate(data)
                stock_validation_result = StockValidationResult(validate_code, validation_result, len(data))
                session.add(stock_validation_result)
                session.commit()
            else:
                print("{0} {1} has been handled".format(date, stock))


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
    checked_list = session.execute('select concat(date, tscode) from stock_validation_result where validation_code = :vcode and result = 1', {'vcode': validation_code})
    checked_set = set(map(lambda item : item[0], checked_list))
    runner = ProcessRunner(4)
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
            # runner.execute(fix_stock_organized_data_by_month, args=(validation_code, checked_set, year_folder, month_folder))
            fix_stock_organized_data_by_month(validation_code, checked_set, year_folder, month_folder)
    # time.sleep(100000)

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
    for date in date_folder_list:
        if not re.search('[0-9]{8}', date):
            continue
        stock_file_list = list_files_in_path(
            STOCK_TICK_ORGANIZED_DATA_PATH + os.path.sep + year_folder + os.path.sep + month_folder + os.path.sep + date + os.path.sep)
        for stock in stock_file_list:
            if date + stock.split('.')[0] in checked_set:
                try:
                    organized_stock_file_path = STOCK_TICK_ORGANIZED_DATA_PATH + os.path.sep + year_folder + os.path.sep + month_folder + os.path.sep + date + os.path.sep + stock
                    data = read_decompress(organized_stock_file_path)
                except Exception as e:
                    print('Load file: {0} error'.format(organized_stock_file_path))
                    continue
                print(date + ':' + stock)
                data = fix_stock_organized_data_daily(data)
                save_compress(data, organized_stock_file_path)
                validation_result = session.query(StockValidationResult).filter(StockValidationResult.validation_code == validation_code, StockValidationResult.tscode == stock.split('.')[0], StockValidationResult.date == date).one()
                validation_result.result = 2 #2表示修复过
                validation_result.modified_time = datetime.now()
                session.commit()

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
    for year_folder in year_folder_list:
        years = re.search('[0-9]{4}', year_folder)
        if not years:
            continue
        elif len(include_year_list) > 0 and years.group() not in include_year_list:
            continue
        month_folder_list = list_files_in_path(STOCK_TICK_DATA_PATH + os.path.sep + year_folder + os.path.sep)
        for month_folder in month_folder_list:
            if not re.search('[0-9]{6}', month_folder):
                continue
            # runner.execute(validate_stock_original_by_month, args=(validate_code, checked_set, year_folder, month_folder))
            validate_stock_original_by_month(validate_code, checked_set, year_folder, month_folder)
    # time.sleep(100000)


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
    for date in date_folder_list:
        if not re.search('[0-9]{8}', date):
            continue
        stock_file_list = list_files_in_path(
            STOCK_TICK_DATA_PATH + os.path.sep + year_folder + os.path.sep + month_folder + os.path.sep + date + os.path.sep)
        for stock in stock_file_list:
            if date + stock.split('.')[0] not in checked_set:
                try:
                    original_stock_file_path = STOCK_TICK_DATA_PATH + os.path.sep + year_folder + os.path.sep + month_folder + os.path.sep + date + os.path.sep + stock
                    data = read_decompress(original_stock_file_path)
                except Exception as e:
                    print('Load file: {0} error'.format(original_stock_file_path))
                    continue
                print(date + ':' + stock)
                try:
                    data = StockTickDataColumnTransform().process(data)
                except Exception as e:
                    print('Invalid data for {0} : {1}'.format(date, stock))
                    continue
                data = StockTickDataCleaner().process(data)
                # 停盘数据
                if len(data) == 0:
                    print('Empty data for {0} : {1}'.format(date, stock))
                    continue
                validation_result = ClosingCallAuctionValidator().validate(data)
                stock_validation_result = StockValidationResult(validate_code, validation_result, len(data))
                session.add(stock_validation_result)
                session.commit()
            else:
                print("{0} {1} has been handled".format(date, stock))

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
            if time > STOCK_TRANSACTION_END_TIME + '.000':
                result.result = RESULT_FAIL
                result.error_details.append('The closing call auction time {0} larger than closing time'.format(time))
        return result

    @classmethod
    def compare_validate(self, target_data, compare_data):
        return True

    @classmethod
    def convert(self, target_data, compare_data):
        pass

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
    # validate_stock_organized_data('20221231-finley')

    #检查原始股票数据
    # validate_stock_original_data('20230105-finley')

    #修复有问题股票数据
    fix_stock_organized_data('20230105-finley')

    #检查收盘集合竞价数据是不是来迟了
    # data = read_decompress("E:\\data\\fix\\4th\\000333-20220617\\000333.original.pkl")
    # data = StockTickDataColumnTransform().process(data)
    # data = StockTickDataCleaner().process(data)
    # validation_result = ClosingCallAuctionValidator().validate(data)
    # print(validation_result)

    # data = read_decompress("E:\\data\\organized\\stock\\tick\\stk_tick10_w_2019\\stk_tick10_w_201909\\20190906\\002081.pkl")
    # data = read_decompress("E:\\data\\organized\\stock\\tick\\stk_tick10_w_2022\\stk_tick10_w_202203\\20220324\\000858.pkl")
    # data = read_decompress("E:\\data\\organized\\stock\\tick\\stk_tick10_w_2022\\stk_tick10_w_202206\\20220617\\000333.pkl")
    # data = read_decompress("E:\\data\\organized\\stock\\tick\\stk_tick10_w_2020\\stk_tick10_w_202009\\20200922\\300059.pkl")
    # data = fix_stock_organized_data_daily(data)
    # save_compress(data, 'E:\\data\\fix\\4th\\300059-20200922\\300059.fix.pkl')
