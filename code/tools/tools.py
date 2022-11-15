#! /usr/bin/env python
# -*- coding:utf8 -*-
import os
import re
import time

import pandas as pd

from common import constants
from common.aop import timing
from common.constants import FUTURE_TICK_DATA_PATH, FUTURE_TICK_FILE_PREFIX, FUTURE_TICK_COMPARE_DATA_PATH, \
    STOCK_TICK_DATA_PATH, CONFIG_PATH, FUTURE_TICK_TEMP_DATA_PATH, FUTURE_TICK_ORGANIZED_DATA_PATH, RESULT_SUCCESS, STOCK_TICK_ORGANIZED_DATA_PATH
from common.io import list_files_in_path, save_compress, read_decompress
from common.persistence.dbutils import create_session
from common.persistence.po import StockValidationResult, FutrueProcessRecord, StockProcessRecord
from data.process import FutureTickDataColumnTransform, StockTickDataColumnTransform, StockTickDataCleaner, DataCleaner, \
    FutureTickDataProcessorPhase1, FutureTickDataProcessorPhase2, StockTickDataEnricher
from data.validation import StockFilterCompressValidator, FutureTickDataValidator, StockTickDataValidator
from framework.concurrent import ProcessRunner


@timing
def filter_stock_data(year, month):
    """根据期指股票集合过滤股票数据
    --2022
        --202201
            --20220101
        --202202

    Parameters
    ----------
    year : 年
    month : 月

    """
    root_path = STOCK_TICK_DATA_PATH
    file_prefix = 'stk_tick10_w_'
    stocks_abstract_50 = pd.read_pickle(CONFIG_PATH + os.path.sep + '50_stocks_abstract.pkl')
    stocks_abstract_300 = pd.read_pickle(CONFIG_PATH + os.path.sep + '300_stocks_abstract.pkl')
    stocks_abstract_500 = pd.read_pickle(CONFIG_PATH + os.path.sep + '500_stocks_abstract.pkl')
    month_folder_path = root_path + file_prefix + year + os.path.sep + file_prefix + year + month
    date_list = list_files_in_path(month_folder_path)
    date_list = sorted(date_list)
    for date in date_list:
        if not re.match('[0-9]{8}', date):
            continue
        print('Handle date %s' % date)
        stocks_50 = get_index_stock_list(date, stocks_abstract_50)
        stocks_300 = get_index_stock_list(date, stocks_abstract_300)
        stocks_500 = get_index_stock_list(date, stocks_abstract_500)
        stock_file_list = list_files_in_path(month_folder_path + os.path.sep + date)
        for stock_file in stock_file_list:
            if os.path.getsize(month_folder_path + os.path.sep + date + os.path.sep + stock_file) > 0:
                if extract_tsccode(stock_file) in stocks_50 or extract_tsccode(
                        stock_file) in stocks_300 or extract_tsccode(
                        stock_file) in stocks_500:
                    print('Stock %s is in index' % extract_tsccode(stock_file))
                    data = pd.read_csv(month_folder_path + os.path.sep + date + os.path.sep + stock_file, encoding='gbk')
                    save_compress(data, month_folder_path + os.path.sep + date + os.path.sep + extract_tsccode(stock_file) + '.pkl')
                else:
                    print('Stock %s is not in index' % extract_tsccode(stock_file))
                os.remove(month_folder_path + os.path.sep + date + os.path.sep + stock_file)


@timing
def compare_compress_file_by_date(month_folder_path, date):
    stock_file_list = list_files_in_path(month_folder_path + '/' + date)
    for stock_file in stock_file_list:
        stock = extract_tsccode(stock_file)
        if not re.match('[0-9]{6}', stock):
            continue
        print('Validate for stock %s' % stock)
        data_csv = pd.read_csv(month_folder_path + '/' + date + '/' + stock_file, encoding='gbk')
        try:
            data_pkl = read_decompress(month_folder_path + '/' + date + '/pkl/' + stock + '.pkl')
        except Exception as e:
            continue
        if StockFilterCompressValidator().compare_validate(data_pkl, data_csv):
            print('Validation pass for stock %s' % stock)


@timing
def compare_future_tick_data(exclude_product=[], exclude_instument=[], include_instrument=[]):
    """遍历比较股指tick数据质量：
    股指tick数据目录结构
    product
        - instrument
    Parameters
    ----------
    exclude_product : list 排除的产品.
    exclude_instument : list 排除的合约.
    """
    product_list = ['IC', 'IF', 'IH']
    runner = ProcessRunner(10)
    if len(include_instrument) > 0:
        for instrument in include_instrument:
            product = instrument[0:2]
            future_file = constants.FUTURE_TICK_FILE_PREFIX + '.' + instrument + '.csv'
            runner.execute(do_compare, args=(future_file, instrument, product))
    else:
        for product in product_list:
            if product in exclude_product:
                continue
            future_file_list = list_files_in_path(FUTURE_TICK_DATA_PATH + product + os.path)
            future_file_list.sort()
            for future_file in future_file_list:
                if FUTURE_TICK_FILE_PREFIX in future_file:
                    instrument = future_file.split('.')[1]
                    if instrument in exclude_instument:
                        continue
                    runner.execute(do_compare, args=(future_file, instrument, product))
    time.sleep(100000)

# @time
def validate_stock_tick_data(validate_code, include_year_list=[]):
    session = create_session()
    checked_list = session.execute('select concat(date, tscode) from stock_validation_result where validation_code = :vcode', {'vcode': validate_code})
    checked_set = set(map(lambda item : item[0], checked_list))
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
            runner.execute(validate_stock_tick_by_month, args=(validate_code, checked_set, year_folder, month_folder))
    time.sleep(100000)

def validate_stock_tick_by_month(validate_code, checked_set, year_folder, month_folder):
    session = create_session()
    data_length_threshold = 100
    date_folder_list = list_files_in_path(
        STOCK_TICK_DATA_PATH + os.path.sep + year_folder + os.path.sep + month_folder + os.path.sep)
    for date in date_folder_list:
        if not re.search('[0-9]{8}', date):
            continue
        stock_file_list = list_files_in_path(
            STOCK_TICK_DATA_PATH + os.path.sep + year_folder + os.path.sep + month_folder + os.path.sep + date + os.path.sep)
        for stock in stock_file_list:
            if date + stock.split('.')[0] not in checked_set:
                data = read_decompress(
                    STOCK_TICK_DATA_PATH + os.path.sep + year_folder + os.path.sep + month_folder + os.path.sep + date + os.path.sep + stock)
                print(date + ':' + stock)
                data = StockTickDataColumnTransform().process(data)
                data = StockTickDataCleaner().process(data)
                if len(data) > data_length_threshold:
                    validation_result = StockTickDataValidator().validate(data)
                    stock_validation_result = StockValidationResult(validate_code, validation_result)
                    session.add(stock_validation_result)
                    session.commit()
            else:
                print("{0} {1} has been handled".format(date, stock))

def enrich_stock_tick_data(process_code, include_year_list=[], include_month_list=[], include_date_list=[], include_stock_list=[]):
    runner = ProcessRunner(20)
    session = create_session()
    checked_list = session.execute('select concat(date, tscode) from stock_process_record where process_code = :pcode', {'pcode': process_code})
    checked_set = set(map(lambda item : item[0], checked_list))
    year_folder_list = list_files_in_path(STOCK_TICK_DATA_PATH + os.path.sep)
    for year_folder in year_folder_list:
        years = re.search('[0-9]{4}', year_folder)
        if not years:
            continue
        elif len(include_year_list) > 0 and years.group() not in include_year_list:
            continue
        month_folder_list = list_files_in_path(STOCK_TICK_DATA_PATH + os.path.sep + year_folder + os.path.sep)
        for month_folder in month_folder_list:
            months = re.search('[0-9]{6}', month_folder)
            if not months:
                continue
            elif len(include_month_list) > 0 and months.group()[4:] not in include_month_list:
                continue
            runner.execute(enrich_stock_tick_data_by_month, args=(checked_set, process_code, include_date_list, include_stock_list,
                                            year_folder, month_folder))
    time.sleep(100000)


def enrich_stock_tick_data_by_month(checked_set, process_code, include_date_list, include_stock_list,
                                    year_folder, month_folder):
    data_length_threshold = 100
    session = create_session()
    date_folder_list = list_files_in_path(
        STOCK_TICK_DATA_PATH + os.path.sep + year_folder + os.path.sep + month_folder + os.path.sep)
    for date in date_folder_list:
        days = re.search('[0-9]{8}', date)
        if not days:
            continue
        elif len(include_date_list) > 0 and days.group()[6:] not in include_date_list:
            continue
        stock_file_list = list_files_in_path(
            STOCK_TICK_DATA_PATH + os.path.sep + year_folder + os.path.sep + month_folder + os.path.sep + date + os.path.sep)
        for stock in stock_file_list:
            if date + stock.split('.')[0] not in checked_set:
                stocks = re.search('[0-9]{6}', stock)
                if not stocks:
                    continue
                elif len(include_stock_list) > 0 and stocks.group()[0:6] not in include_stock_list:
                    continue
                data = read_decompress(
                    STOCK_TICK_DATA_PATH + os.path.sep + year_folder + os.path.sep + month_folder + os.path.sep + date + os.path.sep + stock)
                data = StockTickDataColumnTransform().process(data)
                data = StockTickDataCleaner().process(data)
                if len(data) > data_length_threshold:
                    validation_result = StockTickDataValidator(True).validate(data)
                    if validation_result.result == RESULT_SUCCESS:
                        data = StockTickDataEnricher().process(data)
                        if not os.path.exists(
                                STOCK_TICK_ORGANIZED_DATA_PATH + os.path.sep + year_folder + os.path.sep + month_folder + os.path.sep + date):
                            os.makedirs(
                                STOCK_TICK_ORGANIZED_DATA_PATH + os.path.sep + year_folder + os.path.sep + month_folder + os.path.sep + date)
                        save_compress(data,
                                      STOCK_TICK_ORGANIZED_DATA_PATH + os.path.sep + year_folder + os.path.sep + month_folder + os.path.sep + date + os.path.sep + stock)
                        stock_process_record = StockProcessRecord(process_code, stock.split('.')[0], date, 0)
                        session.add(stock_process_record)
                        session.commit()
                    else:
                        stock_process_record = StockProcessRecord(process_code, stock.split('.')[0], date, 1,
                                                                  validation_result)
                        session.add(stock_process_record)
                        session.commit()
            else:
                print("{0} {1} has been handled".format(date, stock))


@timing
def create_k_line_for_future_tick(process_code, include_product=[], include_instrument=[]):
    """遍历股指文件生成k线文件到临时目录
    临时文件目录
    instrument
        date
    Parameters
    ----------
    include_product : list 包含的产品.
    include_instrument : list 包含的合约.
    """
    products = ['IC', 'IF', 'IH']
    session = create_session()
    checked_list = session.execute('select concat(instrument, date) from future_process_record where process_code = :pcode',
                                   {'pcode': process_code})
    checked_set = set(map(lambda item: item[0], checked_list))
    runner = ProcessRunner(10)
    for product in products:
        if len(include_product) > 0 and product not in include_product:
            continue
        product_path = FUTURE_TICK_DATA_PATH + product
        instrument_list = list_files_in_path(product_path)
        instrument_list.sort()
        for instrument_file in instrument_list:
            if not re.search('[0-9]{4}', instrument_file):
                continue
            instrument = instrument_file.split('.')[1]
            if len(include_instrument) > 0 and instrument not in include_instrument:
                continue
            runner.execute(create_future_k_line_by_instrument, args=(process_code,  checked_set, product, instrument_file))
    time.sleep(100000)

@timing
def conbine_k_line_for_future_tick(process_code, include_product=[], include_instrument=[]):
    """遍历k线临时文件，将文件按合约合并生成到organized目录
    organized文件目录
    product
        instrument
    Parameters
    ----------
    include_product : list 包含的产品
    include_instrument : list 包含的合约
    """

    products = ['IC', 'IF', 'IH']
    session = create_session()
    checked_list = session.execute('select concat(instrument, date) from future_process_record where process_code = :pcode and status = 0',
                                   {'pcode': process_code})
    checked_set = set(map(lambda item: item[0], checked_list))
    runner = ProcessRunner(10)
    for product in products:
        if len(include_product) > 0 and product not in include_product:
            continue
        instrument_list = list_files_in_path(FUTURE_TICK_TEMP_DATA_PATH + product)
        instrument_list.sort()
        for instrument in instrument_list:
            if not re.search('[0-9]{4}', instrument):
                continue
            if len(include_instrument) > 0 and instrument not in include_instrument:
                continue
            if not os.path.exists(FUTURE_TICK_ORGANIZED_DATA_PATH + product):
                os.makedirs(FUTURE_TICK_ORGANIZED_DATA_PATH + product)
            runner.execute(combine_future_k_line_by_instrument,
                           args=(process_code, checked_set, product, instrument))
    time.sleep(100000)


def combine_future_k_line_by_instrument(process_code, checked_set, product, instrument):
    session = create_session()
    columns = ['datetime', 'open', 'close', 'high', 'low', 'volume', 'interest', 'ret.1', 'ret.2', 'ret.5', 'ret.10',
               'ret.20', 'ret.30']
    data = pd.DataFrame(columns=columns)
    if os.path.exists(FUTURE_TICK_ORGANIZED_DATA_PATH + product + os.path.sep + instrument + '.pkl'):
        data = read_decompress(FUTURE_TICK_ORGANIZED_DATA_PATH + product + os.path.sep + instrument + '.pkl')
    file_list = list_files_in_path(FUTURE_TICK_TEMP_DATA_PATH + product + os.path.sep + instrument)
    file_list.sort()
    for file in file_list:
        date = file[0: 8]
        if instrument + date in checked_set:
            date_file = read_decompress(
                FUTURE_TICK_TEMP_DATA_PATH + product + os.path.sep + instrument + os.path.sep + date + '.pkl')
            date_file = FutureTickDataProcessorPhase2().process(date_file)  # 计算收益
            data = pd.concat([data, date_file])
            session.execute(
                'update future_process_record set status = 1 where process_code = :pcode and instrument = :instrument and date = :date and status = 0',
                {'pcode': process_code, 'instrument': instrument, 'date': date})
            session.commit()
    data = data.reset_index()
    save_compress(data, FUTURE_TICK_ORGANIZED_DATA_PATH + product + os.path.sep + instrument + '.pkl')


def create_future_k_line_by_instrument(process_code,  checked_set, product, instrument_file):
    session = create_session()
    print('AA')
    instrument = instrument_file.split('.')[1]
    print('Create k-line for {0} and {1}'.format(product, instrument))
    data = pd.read_csv(FUTURE_TICK_DATA_PATH + product + os.path.sep + instrument_file)
    data = FutureTickDataColumnTransform(product, instrument).process(data)
    data = DataCleaner().process(data)
    data['date'] = data['datetime'].str[0:10]
    date_list = sorted(list(set(data['date'].tolist())))
    for date in date_list:
        if instrument + date in checked_set:
            continue
        date_data = data[data['date'] == date]
        k_line_data = FutureTickDataProcessorPhase1().process(date_data)
        print(k_line_data)
        if not os.path.exists(FUTURE_TICK_TEMP_DATA_PATH + product + os.path.sep + instrument):
            os.makedirs(FUTURE_TICK_TEMP_DATA_PATH + product + os.path.sep + instrument)
        date_replace = date.replace('-', '')
        save_compress(k_line_data,
                      FUTURE_TICK_TEMP_DATA_PATH + product + os.path.sep + instrument + os.path.sep + date_replace + '.pkl')
        future_process_record = FutrueProcessRecord(process_code, instrument, date_replace, 0)
        session.add(future_process_record)
        session.commit()


def do_compare(future_file, instrument, product):
    target_data = pd.read_csv(FUTURE_TICK_DATA_PATH + product + '/' + future_file)
    target_data = FutureTickDataColumnTransform(product, instrument).process(target_data)
    compare_data = pd.DataFrame(
        pd.read_pickle(FUTURE_TICK_COMPARE_DATA_PATH + product + '/' + instrument + '.CCFX-ticks.pkl'))
    compare_data = FutureTickDataValidator().convert(target_data, compare_data)
    FutureTickDataValidator().compare_validate(target_data, compare_data, instrument)


def in_date_range(date, str_date_range):
    """查询当前日期是不是在给定的日期区间，闭区间

    Parameters
    ----------
    date : 日期
    date_range : 日期区间

    """
    date_range = str_date_range.split('_')
    return date <= date_range[1] and date >= date_range[0]


def extract_tsccode(filename):
    return filename.split('_')[0][2:]


def get_index_stock_list(date, abstract):
    for date_range in abstract.keys():
        if in_date_range(date, date_range):
            return abstract[date_range]


if __name__ == '__main__':
    # 测试in_date_range函数
    # print(in_date_range('20110101','20110101_20220607'))
    # print(in_date_range('20110103','20110101_20220607'))
    # print(in_date_range('20110607','20110101_20220607'))
    # print(in_date_range('20101231','20110101_20220607'))

    # 测试extract_tsccode函数
    # print(extract_tsccode('sz300603_20170126.csv'))

    # 测试正则
    # print(re.match('[0-9]{8}','20220102'))

    # 测试filter_stock_data函数
    # for month in ['01']:
    #     filter_stock_data('2017', month)

    # 比较压缩数据
    # month_folder_path = '/Users/finley/Projects/stock-index-future/data/original/stock_daily/stk_tick10_w_2017/stk_tick10_w_201701/'
    # date = '20170103'
    # compare_compress_file_by_date(month_folder_path, date)

    # 期指tick数据比较
    # compare_future_tick_data(['IC'],
    #                          ['IF1701', 'IF1702', 'IF1703', 'IF1704', 'IF1705', 'IF1706', 'IF1707', 'IF1708', 'IF1709',
    #                           'IF1710', 'IF1711', 'IF1712', 'IF1801', 'IF1802', 'IF1803', 'IF1804', 'IF1805', 'IF1806',
    #                           'IF1807', 'IF1808', 'IF1809', 'IF1810', 'IF1811', 'IF1812', 'IF1901', 'IF1902', 'IF1903',
    #                           'IF1905', 'IF1910', 'IF1907', 'IF1908', 'IF1911', 'IF2001', 'IF2002'])

    # 检查stock数据
    # validate_stock_tick_data('20221109-finley',['2022'])

    # 生成stock数据
    enrich_stock_tick_data('20221111-finley-1')

    # 生成股指k线
    # create_k_line_for_future_tick('20221110-finley')

    # 拼接股指k线
    # conbine_k_line_for_future_tick('20221109-finley-1')


