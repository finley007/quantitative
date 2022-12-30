#! /usr/bin/env python
# -*- coding:utf8 -*-

import os
import sys
import re
import time

parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0,parentdir)
from common import constants
from common.constants import STOCK_INDEX_PRODUCTS, FUTURE_TICK_DATA_PATH, FUTURE_TICK_ORGANIZED_DATA_PATH, STOCK_TICK_DATA_PATH, STOCK_TICK_ORGANIZED_DATA_PATH
from common import localio
from common.localio import read_decompress, list_files_in_path, save_compress
from abc import ABCMeta, abstractmethod
import pandas as pd
from data.access import StockDataAccess
from framework.concurrent import ProcessRunner
from common.aop import timing

class FileNameHandler(metaclass = ABCMeta):
    
    @classmethod
    @abstractmethod
    def parse(self, filename):
        pass

    @classmethod
    @abstractmethod
    def build(self, instrument):
        pass
    
class FutureTickerHandler(FileNameHandler):
    
    PREFIX = 'CFFEX.'
    
    def parse(self, filename):
        return filename.split('.')[1]
    
    def build(self, instrument):
        return  self.PREFIX + instrument + constants.FILE_TYPE_CSV


class StockTickerHandler(FileNameHandler):

    def __init__(self, date):
        self._date = date

    def parse(self, filename):
        return filename.split('_')[0]

    def build(self, instrument):
        return instrument + '_' + self._date + constants.FILE_TYPE_CSV


def get_instrument_by_product(product):
    """根据期货产品返回合约列表

    Parameters
    ----------
    product : 期货产品:IF IC IH

    Returns
    -------
    list : 合约列表

    """
    return io.list_files_in_path(constants.FUTURE_TICK_DATA_PATH + product)


def get_instrument_detail(product, instrument): 
    """根据合约名返回合约文件详细信息

    Parameters
    ----------
    product : 产品
    instrument : 合约

    Returns
    -------
    details : 合约文件详情
        record_count : 记录数
        start_time : 开始时间
        end_time : 结束时间

    """
    content = pd.read_csv(constants.FUTURE_TICK_DATA_PATH + product + '/' + FutureTickerHandler().build(instrument))
    return {
        'record_count' : content.size,
        'start_time' : content['datetime'].min(),
        'end_time' : content['datetime'].max(),
    }
    
def get_instrument_transaction_date_list(product, instrument): 
    """根据合约名返回交易日列表

    Parameters
    ----------
    product : 产品
    instrument : 合约

    Returns
    -------
    date_list : 交易日列表

    """
    content = pd.read_csv(constants.FUTURE_TICK_DATA_PATH + product + '/' + FutureTickerHandler().build(instrument))
    time_list = content['datetime'].tolist()
    date_list = sorted(list(set([time[0:10] for time in time_list])))
    return date_list


def get_instrument_tick_daily_statistic(product, instrument): 
    """根据合约名获取tick记录统计信息

    Parameters
    ----------
    product : 产品
    instrument : 合约

    Returns
    -------
    tick_statistic : 合约文件详情
        record_count : 记录数
        date_count : 交易日数
        daily_count : 每天记录数

    """
    content = pd.read_csv(constants.FUTURE_TICK_DATA_PATH + product + '/' + FutureTickerHandler().build(instrument))
    content['date'] = content.apply(lambda item: item['datetime'][0:10], axis = 1)
    content['count'] = 1
    content = content[['date','count']]
    return {
        'record_count' : len(content),
        'date_count' : len(content.groupby('date').count()),
        'daily_count' : content.groupby('date').count()
    }


class FutrueDataStatisticProducer():

    def produce(self):
        """
        分析期货原始数据，结果包含：
        文件数量
        记录数
        起始合约
        起始日期
        最早和最晚时间戳分布
        Returns
        -------

        """
        result = {
        }
        for product in STOCK_INDEX_PRODUCTS:
            files = list_files_in_path(self.get_file_path(product))
            instruments = list(map(lambda item: self.extract_instrument(item), files))
            instruments.sort()
            date_list = []
            start_time_list = []
            end_time_list = []
            record_count = 0
            for file in files:
                data = self.get_data(file, product)
                end_time, start_time = self.parse_time(data)
                date_list.append(start_time[0:10].replace('-',''))
                start_time_list.append(start_time[0:20])
                end_time_list.append(end_time[0:20])
                record_count += len(data)
            date_list = list(set(date_list))
            date_list.sort()
            result[product] = {
                'file_count': len(files),
                'record_count': record_count,
                'instrument_list': instruments,
                'date_list': date_list,
                'start_time_list': start_time_list,
                'end_time_list': end_time_list
            }
        print(result)

    def extract_instrument(self, item):
        return item.split('.')[1]

    def get_file_path(self, product):
        return FUTURE_TICK_DATA_PATH + product

    def parse_time(self, data):
        start_time = data.iloc[0]['datetime']
        end_time = data.iloc[-1]['datetime']
        return end_time, start_time

    def get_data(self, file, product):
        return pd.read_csv(FUTURE_TICK_DATA_PATH + product + os.path.sep + file)

class FutrueOrganizedDataStatisticProducer(FutrueDataStatisticProducer):

    def extract_instrument(self, item):
        return item.split('.')[0]

    def get_file_path(self, product):
        return FUTURE_TICK_ORGANIZED_DATA_PATH + product

    def parse_time(self, data):
        start_time = data.iloc[0]['datetime']
        end_time = data.iloc[-1]['datetime']
        return end_time, start_time

    def get_data(self, file, product):
        return read_decompress(FUTURE_TICK_ORGANIZED_DATA_PATH + product + os.path.sep + file)

@timing
def traverse_stock_data(interface, is_async=True, check_original=True):
    traverse_results = []
    if is_async:
        process_runner = ProcessRunner(6)
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
        if is_async:
            process_runner.execute(handle_by_year, args=(root_path, year_folder, interface))
        else:
            traverse_results = traverse_results + list(set(handle_by_year(root_path, year_folder, interface)))
    if is_async:
        results = process_runner.get_results()
        for result in results:
            traverse_results = traverse_results + list(set(result.get()))
        process_runner.close()
    return traverse_results

def handle_by_year(root_path, year_folder, interface):
    data_access = StockDataAccess()
    results = []
    year_folder_path = root_path + year_folder
    month_folder_list = list_files_in_path(year_folder_path)
    month_folder_list.sort()
    for month_folder in month_folder_list:
        month = re.search('[0-9]{6}', month_folder)
        if not month:
            continue
        month_folder_path = root_path + year_folder + os.path.sep + month_folder
        date_list = list_files_in_path(month_folder_path)
        date_list.sort()
        for date in date_list:
            date_regex = re.match('[0-9]{8}', date)
            if not date_regex:
                continue
            stock_list = list_files_in_path(month_folder_path + os.path.sep + date)
            for stock in stock_list:
                tscode = stock.split('.')[0]
                print('Handle {0} {1}'.format(date, tscode))
                try:
                    data = data_access.access(date, tscode)
                except Exception as e:
                    print(e)
                    continue
                result = interface.operate(data)
                if result:
                    results.append(result)
                    if interface.is_search_mode(): #查询模式下立刻返回结果
                        return results
    return results

class StockDataTraversalInterface(metaclass=ABCMeta):
    """
    提供一个接口，用于遍历操作股票数据
    """
    @abstractmethod
    def operate(self, data):
        pass

    def is_search_mode(self):
        return False

class StockClosingCallAuctionAnalysis(StockDataTraversalInterface):

    def operate(self, data):
        """
        获取最后一个成交量不为0的记录的时间戳
        Parameters
        ----------
        data

        Returns
        -------

        """
        if len(data) > 0:
            try:
                data = data[data['成交量'] > 0]
                if len(data) > 0:
                    time = data.iloc[-1]['时间']
                    return time
            except Exception as e:
                print(e)
        else:
            print('Invalid data')


class StockClosingCallAuctionTimeSearch(StockDataTraversalInterface):

    def operate(self, data):
        """
        获取最后一个成交量不为0的记录的时间戳
        Parameters
        ----------
        data

        Returns
        -------

        """
        if len(data) > 0:
            try:
                data = data[data['成交量'] > 0]
                if len(data) > 0:
                    time = data.iloc[-1]['时间']
                    if time == '15:00:11.141':
                        return str(data.iloc[-1]['交易所代码']) + '|' + str(data.iloc[-1]['自然日']) + '|' + time
            except Exception as e:
                print(e)
        else:
            print('Invalid data')

    def is_search_mode(self):
        return True



if __name__ == '__main__':
    # print(get_instrument_by_product('IF'))
    # print(FutureTickerHandler().build('IF2212'))
    # print(FutureTickerHandler().parse('CFFEX.IF2212.csv'))
    # print(StockTickerHandler('20220812').build('sh688800'))
    # print(StockTickerHandler('20220812').parse('sh688800_20220812.csv'))
    # print(get_instrument_detail('IF','IF2212'))
    # print(len(get_instrument_transaction_date_list('IF','IF2212')))
    # print(get_instrument_tick_daily_statistic('IF','IF2212'))
    # FutrueDataStatisticProducer().produce()
    # FutrueOrganizedDataStatisticProducer().produce()
    # print(traverse_stock_data(StockClosingCallAuctionAnalysis()))
    print(traverse_stock_data(StockClosingCallAuctionTimeSearch()))
