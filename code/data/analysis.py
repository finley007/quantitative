#! /usr/bin/env python
# -*- coding:utf8 -*-

import os
import sys
import re
import time
from abc import ABCMeta, abstractmethod
import pandas as pd
import numpy as np


parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0,parentdir)
from common import constants
from common.constants import STOCK_INDEX_PRODUCTS, FUTURE_TICK_DATA_PATH, FUTURE_TICK_ORGANIZED_DATA_PATH, STOCK_TICK_DATA_PATH, STOCK_TICK_ORGANIZED_DATA_PATH, STOCK_TRANSACTION_END_TIME, STOCK_OPEN_CALL_AUACTION_1ST_STAGE_START_TIME\

from common import localio
from common.localio import read_decompress, list_files_in_path, save_compress
from data.access import StockDataAccess
from framework.localconcurrent import ProcessRunner
from common.aop import timing
from common.timeutils import add_milliseconds_suffix

class FileNameHandler(metaclass = ABCMeta):
    """
    文件名称处理接口
    """
    
    @classmethod
    @abstractmethod
    def parse(self, filename):
        pass

    @classmethod
    @abstractmethod
    def build(self, instrument):
        pass
    
class FutureTickerHandler(FileNameHandler):
    """
    期货tick数据文件名称处理，有前缀CFFEX
    """
    
    PREFIX = 'CFFEX.'
    
    def parse(self, filename):
        return filename.split('.')[1]
    
    def build(self, instrument):
        return  self.PREFIX + instrument + constants.FILE_TYPE_CSV


class StockTickerHandler(FileNameHandler):
    """
    股票tick数据文件名称处理：sz000333_20220616.csv
    """

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
def traverse_stock_data(interface, is_async=True, check_original=True,  include_year_list=[], include_month_list=[], include_date_list=[], include_stock_list=[]):
    """
    遍历股票数据

    Parameters
    ----------
    interface：遍历执行检查逻辑
    is_async：同步还是异步执行
    check_original：检查原始数据还是生成数据

    Returns
    -------

    """
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
        elif len(include_year_list) > 0 and year.group() not in include_year_list:
            continue
        if is_async:
            process_runner.execute(handle_by_year, args=(root_path, year_folder, interface, include_month_list, include_date_list, include_stock_list))
        else:
            result_list = handle_by_year(root_path, year_folder, interface, include_month_list, include_date_list, include_stock_list)
            try:
                traverse_results = traverse_results + list(set(result_list))
            except Exception as e:
                traverse_results = traverse_results + result_list
    if is_async:
        results = process_runner.get_results()
        for result in results:
            try:
                traverse_results = traverse_results + list(set(result.get()))
            except Exception as e:
                traverse_results = traverse_results + result.get()
        process_runner.close()
    return traverse_results

def handle_by_year(root_path, year_folder, interface, include_month_list=[], include_date_list=[], include_stock_list=[]):
    """
    按年执行查询逻辑，主要是并发多进程执行

    Parameters
    ----------
    root_path
    year_folder
    interface

    Returns
    -------

    """
    data_access = StockDataAccess()
    results = []
    year_folder_path = root_path + year_folder
    month_folder_list = list_files_in_path(year_folder_path)
    month_folder_list.sort()
    for month_folder in month_folder_list:
        month = re.search('[0-9]{6}', month_folder)
        if not month:
            continue
        elif len(include_month_list) > 0 and month.group()[4:] not in include_month_list:
            continue
        month_folder_path = root_path + year_folder + os.path.sep + month_folder
        date_list = list_files_in_path(month_folder_path)
        date_list.sort()
        for date in date_list:
            date_regex = re.match('[0-9]{8}', date)
            if not date_regex:
                continue
            elif len(include_date_list) > 0 and date_regex.group()[6:] not in include_date_list:
                continue
            stock_list = list_files_in_path(month_folder_path + os.path.sep + date)
            for stock in stock_list:
                tscode = stock.split('.')[0]
                if len(include_stock_list) > 0 and tscode not in include_stock_list:
                    continue
                print('Handle {0} {1}'.format(date, tscode))
                try:
                    data = data_access.access(date, tscode)
                except Exception as e:
                    print(e)
                    continue
                result = interface.operate(data)
                if result:
                    if isinstance(result, list):
                        results = results + result
                    else:
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
                    if time == '14:42:25.000':
                        return str(data.iloc[-1]['交易所代码']) + '|' + str(data.iloc[-1]['自然日']) + '|' + time
            except Exception as e:
                print(e)
        else:
            print('Invalid data')

    def is_search_mode(self):
        return True


class StockVolumeAfterCloseTimeSearch(StockDataTraversalInterface):

    def operate(self, data):
        """
        查找收盘事件之后所有成交量不为0的记录
        Parameters
        ----------
        data

        Returns
        -------

        """
        if len(data) > 0:
            try:
                data = data[(data['成交量'] > 0) & (data['时间'] >= add_milliseconds_suffix(STOCK_TRANSACTION_END_TIME))][['交易所代码','自然日','时间','成交量','成交额','成交笔数','当日累计成交量']]
                if len(data) > 0:
                    return np.array(data).tolist()
            except Exception as e:
                print(e)
        else:
            print('Invalid data')

class RepeatDataSearch(StockDataTraversalInterface):

    def operate(self, data):
        """
        查找重复且成交量不为0的数据
        Parameters
        ----------
        data

        Returns
        -------

        """
        if len(data) > 0:
            try:
                data = data[data['成交量'] > 0]
                data['count'] = data.groupby(['时间'])['时间'].transform('count')
                index_to_be_handled = data.index[(data['count'] > 1) & (data['时间'] >= constants.STOCK_OPEN_CALL_AUACTION_2ND_STAGE_END_TIME)]
                index_set_to_be_handled = set(index_to_be_handled.tolist())
                new_index_set = set()
                for index in index_set_to_be_handled:
                    if index + 1 not in index_set_to_be_handled:
                        new_index_set.add(index + 1)
                index_set_to_be_handled = index_set_to_be_handled | new_index_set
                data = data.loc[index_set_to_be_handled, ['代码','自然日','时间','成交价','成交量']]
                if len(data) > 0:
                    return np.array(data).tolist()
            except Exception as e:
                print(e)
        else:
            print('Invalid data')


class RecordWithVolumeBetweenNoon(StockDataTraversalInterface):

    def operate(self, data):
        """
        查找11：30到13：00之间成交量不为0的数据
        Parameters
        ----------
        data

        Returns
        -------

        """
        if len(data) > 0:
            try:
                last_record_before_noon_list = data[(data['时间'] <= '11:30:00.000') & (data['时间'] > '11:29:57.000')]['成交量'].tolist()
                can_be_fixed = True #这里过滤出能修复的，也就是11：30这个时间上没有成交量
                if len(last_record_before_noon_list) > 0:
                    for volume in last_record_before_noon_list:
                        if volume > 0:
                            can_be_fixed = False
                data = data[(data['成交量'] > 0) & (data['时间'] > '11:30:00.000') & (data['时间'] < '13:00:00.000')]
                data = data[['代码','自然日','时间','成交价','成交量']]
                if len(data) > 0 and can_be_fixed:
                    return np.array(data).tolist()
            except Exception as e:
                print(e)
        else:
            print('Invalid data')

class NoonClosingDataMissing(StockDataTraversalInterface):

    def operate(self, data):
        """
        缺少11：30的数据

        Parameters
        ----------
        data

        Returns
        -------

        """

        if len(data) > 0:
            try:
                first_item = data.iloc[0][['代码','自然日']]
                print(first_item.tolist())
                if len(data[(data['时间'] == '11:30:00.000')] == 0):
                    return first_item.tolist()
            except Exception as e:
                print(e)
        else:
            print('Invalid data')

class CollectionBiddingDuplicateData(StockDataTraversalInterface):

    def operate(self, data):
        """
        查询集合竞价阶段的重复数据9：15-9：25

        Parameters
        ----------
        data

        Returns
        -------

        """

        if len(data) > 0:
            try:
                temp_data = data[(data['时间'] >= '09:15:00.000') & (data['时间'] < '09:25:00.000')]
                temp_data_group_by = temp_data.groupby('时间')['代码'].count()
                temp_data = temp_data.merge(temp_data_group_by, on=['时间'], how='left')
                temp_data = temp_data[temp_data['代码_y'] > 1]
                temp_data = temp_data[['代码_x', '自然日', '时间','成交价','成交量']]
                if len(temp_data) > 1:
                    return np.array(temp_data).tolist()
            except Exception as e:
                print(e)
        else:
            print('Invalid data')

class ClosingDuplicateData(StockDataTraversalInterface):

    def operate(self, data):
        """
        15:00时间点上有两条数据

        Parameters
        ----------
        data

        Returns
        -------

        """

        if len(data) > 0:
            try:
                temp_data = data[(data['时间'] == '15:00:00.000')]
                temp_data = temp_data[['代码', '自然日', '时间', '成交价', '成交量']]
                if len(temp_data) > 1:
                    return np.array(temp_data).tolist()
            except Exception as e:
                print(e)
        else:
            print('Invalid data')


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
    # print(traverse_stock_data(StockClosingCallAuctionTimeSearch()))
    # pd.DataFrame(traverse_stock_data(StockVolumeAfterCloseTimeSearch())).to_csv('E:\\data\\test\\volume_after_close_time.csv')

    # data = read_decompress('D:\\liuli\\data\\original\\stock\\tick\\stk_tick10_w_2021\\stk_tick10_w_202108\\20210803\\600787.pkl')
    # print(RepeatDataSearch().operate(data))

    # data = pd.read_csv('E:\\data\\compare\\stock\\tick\\20230201\\20181108_600369.pkl_original.csv')
    # print(CollectionBiddingDuplicateData().operate(data))

    # data = pd.read_csv('E:\\data\\compare\\stock\\tick\\20230201\\20181108_600369.pkl_original.csv')
    print(ClosingDuplicateData().operate(data))

    # pd.DataFrame(traverse_stock_data(RepeatDataSearch())).to_csv(
    #     'E:\\data\\test\\repeat_data.csv')

    # pd.DataFrame(traverse_stock_data(RecordWithVolumeBetweenNoon())).to_csv(
    #     'E:\\data\\test\\volume_between_noon_can_be_fixed.csv')

    # pd.DataFrame(traverse_stock_data(NoonClosingDataMissing())).to_csv(
    #     'E:\\data\\test\\noon_closing_data_missing.csv')

    pd.DataFrame(traverse_stock_data(CollectionBiddingDuplicateData())).to_csv(
        'E:\\data\\test\\collection_bidding_duplicate_data.csv')

    pd.DataFrame(traverse_stock_data(ClosingDuplicateData())).to_csv(
        'E:\\data\\test\\closing_duplicate_data.csv')
