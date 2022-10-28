#! /usr/bin/env python
# -*- coding:utf8 -*-
import os
import re
import time

from common.aop import timing
from common.constants import FUTURE_TICK_DATA_PATH, TICK_FILE_PREFIX, FUTURE_TICK_COMPARE_DATA_PATH, \
    STOCK_TICK_DATA_PATH, RESULT_FAIL
from common.io import list_files_in_path, save_compress, read_decompress, build_path
import pandas as pd

from data.process import FutureTickDataColumnTransform, StockTickDataColumnTransform
from data.validation import StockFilterCompressValidator, FutureTickDataValidator, StockTickDataValidator, \
    ValidationResult
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
    root_path = 'E:\\data\\original\\stock\\'
    # root_path = 'D:\\'
    file_prefix = 'stk_tick10_w_'
    stocks_abstract_50 = pd.read_pickle('D:\\liuli\\workspace\\quantitative\\data\\config\\50_stocks_abstract.pkl')
    stocks_abstract_300 = pd.read_pickle('D:\\liuli\\workspace\\quantitative\\data\\config\\300_stocks_abstract.pkl')
    stocks_abstract_500 = pd.read_pickle('D:\\liuli\\workspace\\quantitative\\data\\config\\500_stocks_abstract.pkl')
    month_folder_path = root_path + file_prefix + year + '/' + file_prefix + year + month
    date_list = list_files_in_path(month_folder_path)
    date_list = sorted(date_list)
    for date in date_list:
        if not re.match('[0-9]{8}', date):
            continue
        print('Handle date %s' % date)
        stocks_50 = get_index_stock_list(date, stocks_abstract_50)
        stocks_300 = get_index_stock_list(date, stocks_abstract_300)
        stocks_500 = get_index_stock_list(date, stocks_abstract_500)
        stock_file_list = list_files_in_path(month_folder_path + '/' + date)
        for stock_file in stock_file_list:
            if os.path.getsize(month_folder_path + '/' + date + '/' + stock_file) > 0:
                if extract_tsccode(stock_file) in stocks_50 or extract_tsccode(stock_file) in stocks_300 or extract_tsccode(
                        stock_file) in stocks_500:
                    print('Stock %s is in index' % extract_tsccode(stock_file))
                    data = pd.read_csv(month_folder_path + '/' + date + '/' + stock_file, encoding='gbk')
                    save_compress(data, month_folder_path + '/' + date + '/' + extract_tsccode(stock_file) + '.pkl')
                else:
                    print('Stock %s is not in index' % extract_tsccode(stock_file))
                os.remove(month_folder_path + '/' + date + '/' + stock_file)

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
def compare_future_tick_data(exclude_product=[], exclude_instument=[]):
    """遍历比较股指tick数据质量：
    股指tick数据目录结构
    product
        - instrument
    Parameters
    ----------
    exclude_product : list 排除的产品.
    exclude_instument : list 排除的合约.
    """
    product_list = ['IC','IF','IH']
    runner = ProcessRunner(10)
    for product in product_list:
        if product in exclude_product:
            continue
        future_file_list = list_files_in_path(FUTURE_TICK_DATA_PATH + product + '/')
        future_file_list.sort()
        for future_file in future_file_list:
            if TICK_FILE_PREFIX in future_file:
                instrument = future_file.split('.')[1]
                if instrument in exclude_instument:
                    continue
                runner.execute(do_compare, args=(future_file, instrument, product))
    time.sleep(100000)

def do_compare(future_file, instrument, product):
    target_data = pd.read_csv(FUTURE_TICK_DATA_PATH + product + '/' + future_file)
    target_data = FutureTickDataColumnTransform(product, instrument).process(target_data)
    compare_data = pd.DataFrame(
        pd.read_pickle(FUTURE_TICK_COMPARE_DATA_PATH + product + '/' + instrument + '.CCFX-ticks.pkl'))
    compare_data = FutureTickDataValidator().convert(target_data, compare_data)
    FutureTickDataValidator().compare_validate(target_data, compare_data, instrument)


@timing
def validate_stock_tick_data(filter=[]):
    """遍历验证股票tick数据质量：
    股票tick数据目录结构
    year
        - month
            - day
                - stock
    Parameters
    ----------
    exclude_instument : list 排除的合约.
    """
    year_list = ['2017','2018','2019','2020','2021','2022']
    folder_prefix = 'stk_tick10_w_'
    runner = ProcessRunner(5)
    error_list = []
    for year in year_list:
        root_path = STOCK_TICK_DATA_PATH + folder_prefix + year
        month_folder_list = list_files_in_path(build_path(root_path))
        month_folder_list.sort()
        for month_folder in month_folder_list:
            date_folder_list = list_files_in_path(build_path(root_path, month_folder))
            for date_folder in date_folder_list:
                stock_file_list = list_files_in_path(build_path(root_path, month_folder, date_folder))
                for stock_file in stock_file_list:
                    data = read_decompress(build_path(root_path, month_folder, date_folder, stock_file))
                    data = StockTickDataColumnTransform().process(data)
                    if not StockTickDataValidator().validate(data):
                        error = ValidationResult(RESULT_FAIL, [year + month_folder + date_folder + '-' + stock_file + ' validation failed'])
                        error_list.append(error)



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
    # for month in ['06']:
    #     filter_stock_data('2017', month)

    # 比较压缩数据
    # month_folder_path = '/Users/finley/Projects/stock-index-future/data/original/stock_daily/stk_tick10_w_2017/stk_tick10_w_201701/'
    # date = '20170103'
    # compare_compress_file_by_date(month_folder_path, date)

    # 期指tick数据比较
    compare_future_tick_data(['IC'], ['IF1701','IF1702','IF1703','IF1704','IF1705','IF1706','IF1707','IF1708','IF1709','IF1710','IF1711','IF1712','IF1801','IF1802','IF1803','IF1804','IF1805','IF1806','IF1807','IF1808','IF1809','IF1810','IF1811','IF1812','IF1901','IF1902','IF1903','IF1905','IF1910'])

