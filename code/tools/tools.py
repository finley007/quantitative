#! /usr/bin/env python
# -*- coding:utf8 -*-
import os
import re
import time
from datetime import datetime
import random
import heapq

import pandas as pd

from common import constants
from common.aop import timing, deamon
from common.constants import FUTURE_TICK_DATA_PATH, FUTURE_TICK_FILE_PREFIX, FUTURE_TICK_COMPARE_DATA_PATH, \
    STOCK_TICK_DATA_PATH, CONFIG_PATH, FUTURE_TICK_TEMP_DATA_PATH, FUTURE_TICK_ORGANIZED_DATA_PATH, RESULT_SUCCESS, \
    STOCK_TICK_ORGANIZED_DATA_PATH, REPORT_PATH, RESULT_FAIL, STOCK_TICK_COMBINED_DATA_PATH, YEAR_LIST, STOCK_FILE_PREFIX, \
    STOCK_TICK_COMPARE_DATA_PATH, TDX_PATH, STOCK_INDEX_PRODUCTS, STOCK_DATA_PATH
from common.crawler import StockInfoCrawler
from common.localio import list_files_in_path, save_compress, read_decompress, FileWriter, read_txt, get_ip
from common.persistence.dbutils import create_session
from common.persistence.po import StockValidationResult, FutrueProcessRecord, StockProcessRecord, IndexConstituentConfig
from common.persistence.dao import StockReversionConfigDao
from common.stockutils import get_full_stockcode, get_full_stockcode_for_tdx, get_full_stockcode_for_duan
from data.process import FutureTickDataColumnTransform, StockTickDataColumnTransform, StockTickDataCleaner, DataCleaner, \
    FutureTickDataProcessorPhase1, FutureTickDataProcessorPhase2, StockTickDataEnricher, IndexConstituentStocksInfo
from common.timeutils import date_format_transform
from data.validation import StockFilterCompressValidator, FutureTickDataValidator, StockTickDataValidator, DtoStockValidationResult
from framework.localconcurrent import ProcessRunner
from data.access import create_stock_file_path
from common.log import get_logger


@timing
def filter_stock_data(year, month, date_list=[], stock_file_list=[]):
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
        print('Handle date %s' % date)
        stocks_50 = get_index_stock_list(date, stocks_abstract_50)
        stocks_300 = get_index_stock_list(date, stocks_abstract_300)
        stocks_500 = get_index_stock_list(date, stocks_abstract_500)
        if len(stock_file_list) == 0:
            stock_file_list = list_files_in_path(month_folder_path + os.path.sep + date)
        for stock_file in stock_file_list:
            if os.path.getsize(month_folder_path + os.path.sep + date + os.path.sep + stock_file) > 0:
                if extract_tsccode(stock_file) in stocks_50 or extract_tsccode(
                        stock_file) in stocks_300 or extract_tsccode(
                        stock_file) in stocks_500:
                    print('Stock %s is in index' % extract_tsccode(stock_file))
                    source_path = month_folder_path + os.path.sep + date + os.path.sep + stock_file
                    data = pd.read_csv(source_path, encoding='gbk')
                    target_path = month_folder_path + os.path.sep + date + os.path.sep + extract_tsccode(stock_file) + '.pkl'
                    save_compress(data, target_path)
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
    """
    检查股票数据
    Parameters
    ----------
    validate_code： string 一次批量检查的唯一标识
    include_year_list：list 年过滤条件

    Returns
    -------

    """
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
            # runner.execute(validate_stock_tick_by_month, args=(validate_code, checked_set, year_folder, month_folder))
            validate_stock_tick_by_month(validate_code, checked_set, year_folder, month_folder)
    # time.sleep(100000)

def validate_stock_tick_by_month(validate_code, checked_set, year_folder, month_folder):
    """
    按月检查股票数据，主要为了多进程并行执行

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
                    stock_validation_result = StockValidationResult(validate_code, DtoStockValidationResult(RESULT_FAIL, [str(e)], stock.split('.')[0], date), len(data))
                    session.add(stock_validation_result)
                    session.commit()
                    continue
                data = StockTickDataCleaner().process(data)
                if len(data) > data_length_threshold:
                    validation_result = StockTickDataValidator().validate(data)
                    stock_validation_result = StockValidationResult(validate_code, validation_result, len(data))
                    session.add(stock_validation_result)
                    session.commit()
            else:
                print("{0} {1} has been handled".format(date, stock))

def enrich_stock_tick_data(process_code, include_year_list=[], include_month_list=[], include_date_list=[], include_stock_list=[]):
    """
    处理股票数据，从original数据集生成organized数据集
    Parameters
    ----------
    process_code：string 一次批量处理的唯一标识
    include_year_list：list 年过滤
    include_month_list：list 月过滤
    include_date_list：list 日过滤
    include_stock_list：list 股票过滤

    Returns
    -------

    """
    process_code = process_code + get_ip()
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
        month_folder_list.sort()
        for month_folder in month_folder_list:
            months = re.search('[0-9]{6}', month_folder)
            if not months:
                continue
            elif len(include_month_list) > 0 and months.group()[4:] not in include_month_list:
                continue
            enrich_stock_tick_data_by_month(checked_set, process_code, include_date_list, include_stock_list, year_folder, month_folder)
            # runner.execute(enrich_stock_tick_data_by_month, args=(checked_set, process_code, include_date_list, include_stock_list, year_folder, month_folder))
    time.sleep(100000)


def enrich_stock_tick_data_by_month(checked_set, process_code, include_date_list, include_stock_list,
                                    year_folder, month_folder):
    """
    按月处理股票数据，主要是为了多进程并行执行
    Parameters
    ----------
    checked_set
    process_code
    include_date_list
    include_stock_list
    year_folder
    month_folder

    Returns
    -------

    """
    data_length_threshold = 100
    session = create_session()
    date_folder_list = list_files_in_path(STOCK_TICK_DATA_PATH + os.path.sep + year_folder + os.path.sep + month_folder + os.path.sep)
    date_folder_list.sort()
    get_logger().info('Handle date for year {0} and month {1}: {2}'.format(year_folder, month_folder, '|'.join(date_folder_list)))
    for date in date_folder_list:
        days = re.search('[0-9]{8}', date)
        if not days:
            continue
        elif len(include_date_list) > 0 and days.group()[6:] not in include_date_list:
            continue
        stock_file_list = list_files_in_path(STOCK_TICK_DATA_PATH + os.path.sep + year_folder + os.path.sep + month_folder + os.path.sep + date + os.path.sep)
        stock_file_list.sort()
        get_logger().debug('Handle date {0} for stock: {1}'.format(date, '|'.join(stock_file_list)))
        for stock in stock_file_list:
            if date + stock.split('.')[0] not in checked_set:
                stocks = re.search('[0-9]{6}', stock)
                if not stocks:
                    continue
                elif len(include_stock_list) > 0 and stocks.group()[0:6] not in include_stock_list:
                    continue
                try :
                    original_stock_file_path = STOCK_TICK_DATA_PATH + os.path.sep + year_folder + os.path.sep + month_folder + os.path.sep + date + os.path.sep + stock
                    data = read_decompress( original_stock_file_path)
                except Exception as e:
                    get_logger().error('Load file: {0} error'.format(original_stock_file_path))
                    continue
                try :
                    data = StockTickDataColumnTransform().process(data)
                except Exception as e:
                    get_logger().error('Do column transform error for file: {0}'.format(original_stock_file_path))
                    continue
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
                                                                  str(validation_result))
                        session.add(stock_process_record)
                        session.commit()
            else:
                get_logger().debug("{0} {1} has been handled".format(date, stock))

def combine_stock_tick_data(process_code, include_year_list=[], include_month_list=[], include_date_list=[]):
    """
    按天合并股票数据，这个的目的是为了合并大文件提升读取性能，但是经过测试这样做是没有效果的

    Parameters
    ----------
    process_code
    include_year_list
    include_month_list
    include_date_list

    Returns
    -------

    """
    runner = ProcessRunner(10)
    session = create_session()
    checked_list = session.execute('select concat(date, tscode) from stock_process_record where process_code = :pcode', {'pcode': process_code})
    checked_set = set(map(lambda item: item[0], checked_list))
    year_folder_list = list_files_in_path(STOCK_TICK_ORGANIZED_DATA_PATH + os.path.sep)
    for year_folder in year_folder_list:
        years = re.search('[0-9]{4}', year_folder)
        if not years:
            continue
        elif len(include_year_list) > 0 and years.group() not in include_year_list:
            continue
        month_folder_list = list_files_in_path(STOCK_TICK_ORGANIZED_DATA_PATH + os.path.sep + year_folder + os.path.sep)
        for month_folder in month_folder_list:
            months = re.search('[0-9]{6}', month_folder)
            if not months:
                continue
            elif len(include_month_list) > 0 and months.group()[4:] not in include_month_list:
                continue
            combine_stock_tick_data_by_month(checked_set, process_code, include_date_list, year_folder, month_folder)
            # runner.execute(combine_stock_tick_data_by_month, args=(checked_set, process_code, include_date_list, year_folder, month_folder))
    # time.sleep(100000)

def combine_stock_tick_data_by_month(checked_set, process_code, include_date_list,  year_folder, month_folder):
    default_tscode = '000000'
    session = create_session()
    date_folder_list = list_files_in_path(
        STOCK_TICK_ORGANIZED_DATA_PATH + os.path.sep + year_folder + os.path.sep + month_folder + os.path.sep)
    for date in date_folder_list:
        if date + default_tscode not in checked_set:
            days = re.search('[0-9]{8}', date)
            if not days:
                continue
            elif len(include_date_list) > 0 and days.group()[6:] not in include_date_list:
                continue
            stock_file_list = list_files_in_path(
                STOCK_TICK_ORGANIZED_DATA_PATH + os.path.sep + year_folder + os.path.sep + month_folder + os.path.sep + date + os.path.sep)
            stock_file_list.sort()
            combined_data = pd.DataFrame()
            for stock in stock_file_list:
                stocks = re.search('[0-9]{6}', stock)
                if not stocks:
                    continue
                try :
                    organized_stock_file_path = STOCK_TICK_ORGANIZED_DATA_PATH + os.path.sep + year_folder + os.path.sep + month_folder + os.path.sep + date + os.path.sep + stock
                    data = read_decompress(organized_stock_file_path)
                except Exception as e:
                    print('Load file: {0} error'.format(organized_stock_file_path))
                    continue
                combined_data = pd.concat([combined_data, data])
            date_path = STOCK_TICK_COMBINED_DATA_PATH + os.path.sep + year_folder + os.path.sep + month_folder + os.path.sep + date
            if not os.path.exists(date_path):
                os.makedirs(date_path)
            save_compress(combined_data, date_path + os.path.sep + date + '.pkl')
            stock_process_record = StockProcessRecord(process_code, default_tscode, date, 0)
            session.add(stock_process_record)
            session.commit()
        else:
            print("{0} has been combined".format(date))

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
    process_code = process_code + '_' + get_ip()
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
            # create_future_k_line_by_instrument(process_code,  checked_set, product, instrument_file)
            runner.execute(create_future_k_line_by_instrument, args=(process_code,  checked_set, product, instrument_file))
    time.sleep(100000)

def create_future_k_line_by_instrument(process_code,  checked_set, product, instrument_file):
    session = create_session()
    instrument = instrument_file.split('.')[1]
    get_logger().info('Create k-line for {0} and {1}'.format(product, instrument))
    data = pd.read_csv(FUTURE_TICK_DATA_PATH + product + os.path.sep + instrument_file)
    data = FutureTickDataColumnTransform(product, instrument).process(data)
    data = DataCleaner().process(data)
    data['date'] = data['datetime'].str[0:10]
    date_list = sorted(list(set(data['date'].tolist())))
    for date in date_list:
        date_replace = date.replace('-', '')
        if instrument + date_replace in checked_set:
            continue
        date_data = data[data['date'] == date]
        k_line_data = FutureTickDataProcessorPhase1().process(date_data)
        if not os.path.exists(FUTURE_TICK_TEMP_DATA_PATH + product + os.path.sep + instrument):
            os.makedirs(FUTURE_TICK_TEMP_DATA_PATH + product + os.path.sep + instrument)
        save_compress(k_line_data, FUTURE_TICK_TEMP_DATA_PATH + product + os.path.sep + instrument + os.path.sep + date_replace + '.pkl')
        future_process_record = FutrueProcessRecord(process_code, instrument, date_replace, 0)
        session.add(future_process_record)
        session.commit()

@timing
def combine_k_line_for_future_tick(process_code, include_product=[], include_instrument=[]):
    """遍历k线临时文件，将文件按合约合并生成到organized目录
    organized文件目录
    product
        instrument
    Parameters
    ----------
    include_product : list 包含的产品
    include_instrument : list 包含的合约
    """

    process_code = process_code + get_ip()
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
            # combine_future_k_line_by_instrument(process_code, checked_set, product, instrument)
            runner.execute(combine_future_k_line_by_instrument, args=(process_code, checked_set, product, instrument))
    time.sleep(100000)

def combine_future_k_line_by_instrument(process_code, checked_set, product, instrument):
    session = create_session()
    columns = ['datetime', 'open', 'close', 'high', 'low', 'volume', 'interest', 'ret.1', 'ret.2', 'ret.5', 'ret.10',
               'ret.20', 'ret.30']
    data = pd.DataFrame(columns=columns)
    get_logger().info('Combine k-line for {0} and {1}'.format(product, instrument))
    file_list = list_files_in_path(FUTURE_TICK_TEMP_DATA_PATH + product + os.path.sep + instrument)
    file_list.sort()
    for file in file_list:
        date = file[0: 8]
        if instrument + date in checked_set:
            date_file = read_decompress(FUTURE_TICK_TEMP_DATA_PATH + product + os.path.sep + instrument + os.path.sep + date + '.pkl')
            date_file = FutureTickDataProcessorPhase2().process(date_file)  # 计算收益
            data = pd.concat([data, date_file])
            session.execute(
                'update future_process_record set status = 1 where process_code = :pcode and instrument = :instrument and date = :date and status = 0',
                {'pcode': process_code, 'instrument': instrument, 'date': date})
            session.commit()
    data = data.reset_index()
    save_compress(data, FUTURE_TICK_ORGANIZED_DATA_PATH + product + os.path.sep + instrument + '.pkl')

@timing
def init_index_constituent_config():
    """根据股指合约成分股配置文件生成数据库数据，用于检查股票数据的完整性
    """
    session = create_session()
    stocks_abstract_50 = pd.read_pickle(CONFIG_PATH + os.path.sep + '50_stocks.pkl')
    stocks_abstract_300 = pd.read_pickle(CONFIG_PATH + os.path.sep + '300_stocks.pkl')
    stocks_abstract_500 = pd.read_pickle(CONFIG_PATH + os.path.sep + '500_stocks.pkl')
    start_date = '2017-01-01'
    end_date = '2022-08-12'
    for time in stocks_abstract_50:
        date = str(time)[0 : 10]
        if date < start_date or date > end_date:
            continue
        for stock in stocks_abstract_50[time]:
            config = IndexConstituentConfig('IH', date, stock[0: 6])
            try:
                session.add(config)
                session.commit()
            except Exception as e:
                print('The combination: {0}, {1}, {2}'.format('IH', date, stock[0: 6]))
    for time in stocks_abstract_300:
        date = str(time)[0: 10]
        if date < start_date or date > end_date:
            continue
        for stock in stocks_abstract_300[time]:
            config = IndexConstituentConfig('IF', date, stock[0: 6])
            try:
                session.add(config)
                session.commit()
            except Exception as e:
                print('The combination: {0}, {1}, {2}'.format('IF', date, stock[0: 6]))
    for time in stocks_abstract_500:
        date = str(time)[0: 10]
        if date < start_date or date > end_date:
            continue
        for stock in stocks_abstract_500[time]:
            config = IndexConstituentConfig('IC', date, stock[0: 6])
            try:
                session.add(config)
                session.commit()
            except Exception as e:
                print('The combination: {0}, {1}, {2}'.format('IC', date, stock[0: 6]))

def create_stock_files_statistics(check_original=True, year_list=[], month_list=[], date_filter=[]):
    """生成文件数量统计
    Parameters
    ----------
    check_original : boolean 检查原始数据 为true则检查：original/stock/tick目录，如果为false则检查：organized/stock/tick
    """
    if check_original:
        root_path = STOCK_TICK_DATA_PATH
    else:
        root_path = STOCK_TICK_ORGANIZED_DATA_PATH
    year_folder_list = list_files_in_path(root_path)
    year_folder_list.sort()
    for year_folder in year_folder_list:
        year_count = 0
        year = re.search('[0-9]{4}', year_folder)
        if not year:
            continue
        if len(year_list) > 0 and year.group() not in year_list:
            continue
        year_folder_path = root_path + year_folder
        month_folder_list = list_files_in_path(year_folder_path)
        month_folder_list.sort()
        for month_folder in month_folder_list:
            month_count = 0
            month = re.search('[0-9]{6}', month_folder)
            if not month:
                continue
            if len(month_list) > 0 and month.group()[4:] not in month_list:
                continue
            month_folder_path = root_path + year_folder + os.path.sep + month_folder
            date_list = list_files_in_path(month_folder_path)
            date_list.sort()
            for date in date_list:
                date_regex = re.match('[0-9]{8}', date)
                if not date_regex:
                    continue
                if len(date_filter) > 0 and date_regex.group()[6:] not in date_filter:
                    continue
                checked_stock_list = list(map(lambda stock: stock.split('.')[0], list_files_in_path(month_folder_path + os.path.sep + date)))
                year_count = year_count + len(checked_stock_list)
                month_count = month_count + len(checked_stock_list)
                if len(month_list) > 0:
                    print(date + ':' + str(len(checked_stock_list)))
                if len(date_filter) > 0:
                    print(checked_stock_list)
            if len(year_list) > 0:
                print(str(month.group()) + ':' + str(month_count))
        print(str(year.group()) + ':' + str(year_count))

def validate_stock_data_integrity_check(check_original=True):
    """根据股指合约成分股配置文件生成数据库数据，用于检查股票数据的完整性
    Parameters
    ----------
    check_original : boolean 检查原始数据 为true则检查：original/stock/tick目录，如果为false则检查：organized/stock/tick
    """
    session = create_session()
    total_file_count = 0
    if check_original:
        file_writer = FileWriter(REPORT_PATH + os.path.sep + "stock\\tick\\report\\origin_amount_check_20221121")
    else:
        file_writer = FileWriter(REPORT_PATH + os.path.sep + "stock\\tick\\report\\organized_amount_check_20221121")
    stock_cache = {}
    stock_info_crawler = StockInfoCrawler()
    if check_original:
        root_path = STOCK_TICK_DATA_PATH
    else:
        root_path = STOCK_TICK_ORGANIZED_DATA_PATH
    year_folder_list = list_files_in_path(root_path)
    year_folder_list.sort()
    for year_folder in year_folder_list:
        year = re.search('[0-9]{4}', year_folder)
        if not year:
            continue
        year_folder_path = root_path + year_folder
        month_folder_list = list_files_in_path(year_folder_path)
        month_folder_list.sort()
        for month_folder in month_folder_list:
            if not re.search('[0-9]{6}', month_folder):
                continue
            month_folder_path = root_path + year_folder + os.path.sep + month_folder
            date_list = list_files_in_path(month_folder_path)
            date_list.sort()
            for date in date_list:
                if not re.match('[0-9]{8}', date):
                    continue
                print('Handle date %s' % date)
                query_result = session.execute(
                    'select distinct tscode from index_constituent_config where date = :date order by tscode;', {'date': date_format_transform(date)}).fetchall()
                stock_list = list(map(lambda stock: stock[0], query_result))
                stock_list.sort()
                checked_stock_list = list(map(lambda stock: stock.split('.')[0], list_files_in_path(month_folder_path + os.path.sep + date)))
                total_file_count = total_file_count + len(checked_stock_list)
                checked_stock_list.sort()
                miss_stocks = set(stock_list).difference(set(checked_stock_list))
                if len(miss_stocks) > 0:
                    filter_miss_stocks = []
                    for stock_code in miss_stocks:
                        full_stock_code = get_full_stockcode(stock_code)
                        cache_key = (stock_code + '-' + year.group()[2:])
                        if cache_key not in stock_cache:
                            date_set = set(stock_info_crawler.get_content(year.group()[2:], full_stock_code))
                            stock_cache[cache_key] = date_set
                        if date not in stock_cache[cache_key]:
                            #停牌
                            filter_miss_stocks.append(stock_code + 'x')
                        else:
                            filter_miss_stocks.append(stock_code + 'o')
                    file_writer.write_file_line('Date: {0} and missing stocks: {1}'.format(date, filter_miss_stocks))
    file_writer.write_file_line('Total files: {0}'.format(total_file_count))

def update_stock_suspension_status():
    """
    根据https://data.gtimg.cn/flashdata/hushen/daily/更新股票停盘信息
    Returns
    -------

    """
    year_list = YEAR_LIST
    session = create_session()
    stock_list = session.execute('select distinct tscode from index_constituent_config order by tscode').fetchall()
    stock_info_crawler = StockInfoCrawler()
    for stock in stock_list:
        date_list = session.execute('select distinct date from index_constituent_config where tscode = :tscode order by date', {'tscode': stock[0]}).fetchall()
        date_list = list(map(lambda date: date[0], date_list))
        date_list.sort()
        for year in year_list:
            full_stock_code = get_full_stockcode(stock[0])
            normal_date_list = list(set(stock_info_crawler.get_content(year[2:4], full_stock_code)))
            normal_date_list = list(map(lambda date: date[0:4] + '-' + date[4:6] + '-' + date[6:8], normal_date_list))
            all_date_list = list(filter(lambda date: year in date, date_list))
            suspend_date_list = set(all_date_list).difference(set(normal_date_list))
            if len(suspend_date_list) > 0:
                session.execute('update index_constituent_config set status = 1 where tscode = :tscode and date in :dates', {'tscode': stock[0], 'dates' : set(suspend_date_list)})
                session.commit()

def update_stock_st_status():
    """
    根据D:\liuli\data\original\stock\stock_data_1d.h5更新st状态
    Returns
    -------

    """
    session = create_session()
    stock_list = session.execute('select distinct tscode from index_constituent_config order by tscode').fetchall()
    stock_list.sort()
    config_file_path = STOCK_DATA_PATH + os.path.sep + 'stock_data_1d.h5'
    config = pd.read_hdf(config_file_path)
    for stock in stock_list:
        date_list = session.execute('select distinct date from index_constituent_config where tscode = :tscode order by date', {'tscode': stock[0]}).fetchall()
        date_list = list(map(lambda date: date[0], date_list))
        date_list.sort()
        for date in date_list:
            get_logger().debug('Handle date: {} for stock: {}'.format(date, stock[0]))
            try:
                if config.loc[date, get_full_stockcode_for_duan(stock[0])]['is_ST'] == 1:
                    session.execute(
                        'update index_constituent_config set is_st = 1 where tscode = :tscode and date = :date',
                        {'tscode': stock[0], 'date': date})
                    session.commit()
            except Exception as e:
                get_logger().error('Exception occur when handle date: {} for stock: {}'.format(date, stock[0]))

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


def create_stock_manually_check_files(compare_count = 10):
    """
    生成股票数据人工对比文件

    Parameters
    ----------
    compare_count

    Returns
    -------

    """
    while compare_count > 0:
        year = YEAR_LIST[random.randint(0, 5)]
        month = list(range(1, 13, 1))[random.randint(0, 11)]
        if month == 2:
            day = list(range(1, 29, 1))[random.randint(0, 27)]
        elif month in [1, 3, 5, 7, 8, 10, 12]:
            day = list(range(1, 32, 1))[random.randint(0, 30)]
        else:
            day = list(range(1, 31, 1))[random.randint(0, 29)]
        if month < 10:
            month = '0' + str(month)
        else:
            month = str(month)
        if day < 10:
            day = '0' + str(day)
        else:
            day = str(day)
        original_file_path = create_stock_file_path(year, month, day)
        try:
            stock_file_list = list_files_in_path(original_file_path)
        except Exception as e:
            continue
        stock_file_list.sort()
        stock = stock_file_list[random.randint(0, len(stock_file_list) - 1)]
        original_data = read_decompress(create_stock_file_path(year, month, day, stock))
        organized_data = read_decompress(create_stock_file_path(year, month, day, stock, is_original=False))
        target_path = STOCK_TICK_COMPARE_DATA_PATH + datetime.now().strftime("%Y%m%d") + os.path.sep
        if not os.path.exists(target_path):
            os.makedirs(target_path)
        original_data.to_csv(target_path + year + month + day + '_' + stock + '_original.csv')
        organized_data.to_csv(target_path + year + month + day + '_' + stock + '_organized.csv')
        compare_count = compare_count - 1

@deamon
def create_stock_reversion_info(is_incremental = False, products=STOCK_INDEX_PRODUCTS):
    """
    生成股票复权信息
    Returns
    -------

    """
    init_index_constituent_config = IndexConstituentStocksInfo()
    stock_reversion_config_dao = StockReversionConfigDao()
    un_reversion_data = TDX_PATH + os.path.sep + 'wfq'
    back_reversion_data = TDX_PATH + os.path.sep + 'hfq'
    for product in products:
        stocks = init_index_constituent_config.get_all_constituent_stocks(product)
        get_logger().info('Start to handle product {0} \n stock list: {1}'.format(product, stocks))
        # 增量更新 vs 全量更新
        if not is_incremental:
            stock_reversion_config_dao.delete_records_by_stocks(stocks)
            for stock in stocks:
                #未复权
                un_reversion = parse_tdx_data(un_reversion_data + os.path.sep + get_full_stockcode_for_tdx(stock))
                #后复权
                back_reversion = parse_tdx_data(back_reversion_data + os.path.sep + get_full_stockcode_for_tdx(stock))
                data = pd.merge(un_reversion, back_reversion, on='date')
                data.dropna(inplace=True)
                data['factor'] = data.apply(lambda item: float(item['open_y'])/float(item['open_x']), axis = 1)
                print(data)
                data['delta_factor'] = data['factor'] - data['factor'].shift(1)
                data.dropna(inplace=True)
                min_delta_factor = min(data['delta_factor'].tolist())
                print(data[data['delta_factor'] == min_delta_factor][['date', 'open_x', 'open_y', 'factor', 'delta_factor', 'close_x', 'close_y']])
                print(data[data['date'] == '2022-11-28'][['date', 'open_x', 'open_y', 'factor', 'delta_factor', 'close_x', 'close_y']])

def parse_tdx_data(file):
    """
    日期	    开盘	    最高	    最低	    收盘	    成交量	    成交额
    Returns
    -------

    """
    return pd.DataFrame(read_txt(file, '\t'), columns=['date', 'open', 'high', 'low', 'close', 'volume', 'amount'])



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
    # filter_stock_data('2020', '12', ['20201218'],['sz000860_20201218.csv','sz000869_20201218.csv','sz000876_20201218.csv','sz000877_20201218.csv','sz000878_20201218.csv','sz000883_20201218.csv','sz000887_20201218.csv'])

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
    # validate_stock_tick_data('20221213-finley')

    # 生成stock数据
    # enrich_stock_tick_data('20221111-finley-1')
    # enrich_stock_tick_data('20221111-finley-2',['2019'],['11'],['28'],['603877'])

    #合并股票数据
    # combine_stock_tick_data('20230109-finley-1',['2018'],['08', '09', '10'])

    # 检查stock数据
    #初始化表
    # init_index_constituent_config()
    #核对文件数量
    # create_stock_files_statistics(False, year_list=['2020'], month_list=['06'])
    #检查原始股票数据
    # validate_stock_data_integrity_check()
    #检查已处理股票数据
    # validate_stock_data_integrity_check(False)

    # 生成股指k线
    # create_k_line_for_future_tick('20230411-finley')
    # 拼接股指k线
    # combine_k_line_for_future_tick('20230313-finley')

    # 分析股指成分股
    # stocks_50 = pd.read_pickle(CONFIG_PATH + os.path.sep + '50_stocks.pkl')
    # print('stocks_50-----------------------------')
    # for key in stocks_50.keys():
    #     if '2022-06-13' in str(key):
    #         print(stocks_50[key])
    # stocks_300 = pd.read_pickle(CONFIG_PATH + os.path.sep + '300_stocks.pkl')
    # print('stocks_300-----------------------------')
    # for key in stocks_300.keys():
    #     if '2022-06-13' in str(key):
    #         print(stocks_300[key])
    # stocks_500 = pd.read_pickle(CONFIG_PATH + os.path.sep + '500_stocks.pkl')
    # print('stocks_500-----------------------------')
    # for key in stocks_500.keys():
    #     if '2022-06-13' in str(key):
    #         print(stocks_500[key])

    # stocks_50 = pd.read_pickle(CONFIG_PATH + os.path.sep + '50_stocks_abstract.pkl')
    # print('stocks_50-----------------------------')
    # for key in stocks_50.keys():
    #     if '2022' in key:
    #         print(key)
    #         print(stocks_50[key])
    # stocks_300 = pd.read_pickle(CONFIG_PATH + os.path.sep + '300_stocks_abstract.pkl')
    # print('stocks_300-----------------------------')
    # for key in stocks_300.keys():
    #     if '2022' in key:
    #         print(key)
    #         print(stocks_300[key])
    # stocks_500 = pd.read_pickle(CONFIG_PATH + os.path.sep + '500_stocks_abstract.pkl')
    # print('stocks_500-----------------------------')
    # for key in stocks_500.keys():
    #     if '2022' in key:
    #         print(key)
    #         print(stocks_500[key])

    # stocks_50 = pd.read_pickle(CONFIG_PATH + os.path.sep + '50_stocks.pkl')
    # print('stocks_50-----------------------------')
    # print(stocks_50[pd.Timestamp('2021-12-10 00:00:00')])
    # print(stocks_50[pd.Timestamp('2021-12-13 00:00:00')])
    # stocks_300 = pd.read_pickle(CONFIG_PATH + os.path.sep + '300_stocks.pkl')
    # print('stocks_300-----------------------------')
    # print(stocks_300[pd.Timestamp('2021-12-10 00:00:00')])
    # print(stocks_300[pd.Timestamp('2021-12-13 00:00:00')])
    # stocks_500 = pd.read_pickle(CONFIG_PATH + os.path.sep + '500_stocks.pkl')
    # print(stocks_500[pd.Timestamp('2021-09-27 00:00:00')])
    # print(stocks_500[pd.Timestamp('2021-09-28 00:00:00')])

    # 生成股票停盘信息
    # update_stock_suspension_status()

    # 生成股票st信息
    update_stock_st_status()

    # 生成股票复权信息
    # create_stock_reversion_info()

    # 生成股票数据人工对比文件
    # create_stock_manually_check_files(10)

    # print(len(read_decompress("E:/data/factor/IC_FCT_01_002_CLOSE_MINUS_MOVING_AVERAGE-1.0[200,500,1000,1500]")))