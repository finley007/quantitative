#! /usr/bin/env python
# -*- coding:utf8 -*-
import re
from abc import ABCMeta, abstractmethod
import hashlib

import numpy

from common.aop import timing
from common.constants import RESULT_SUCCESS, RESULT_FAIL, FUTURE_TICK_REPORT_DATA_PATH, STOCK_OPEN_CALL_AUACTION_2ND_STAGE_END_TIME, \
    STOCK_CLOSE_CALL_AUACTION_START_TIME, STOCK_TRANSACTION_NOON_END_TIME, STOCK_TRANSACTION_NOON_START_TIME, STOCK_TRANSACTION_END_TIME, STOCK_TRANSACTION_NOON_END_TIME, STOCK_TRANSACTION_NOON_START_TIME, STOCK_TRANSACTION_START_TIME
from common.exception.exception import ValidationFailed, InvalidStatus
from common.localio import FileWriter, read_decompress
from common.timeutils import date_alignment, add_milliseconds_suffix
from data.process import StockTickDataColumnTransform, StockTickDataCleaner
from common.persistence.dao import IndexConstituentConfigDao


class ValidationResult:
    """验证结果类

    Parameters
    ----------
    result : string
    error_details : list
    """

    def __init__(self):
        self.result = ''
        self.error_details = []

    def __str__(self):
        if self.result == RESULT_SUCCESS:
            return 'The validation result is {0}'.format(RESULT_SUCCESS)
        elif self.result == RESULT_FAIL:
            return 'The validation result is {0} and error details: \n {1}'.format(RESULT_SUCCESS,
                                                                                   '\n'.join(self.error_details))
        else:
            raise InvalidStatus('Invalid validation status {0}'.format(self.result))

class DtoStockValidationResult(ValidationResult):

    def __init__(self, result, error_details, tscode, date):
        self.result = result
        self.error_details = error_details
        self.tscode = tscode
        self.date = date

    def __str__(self):
        if self.result == RESULT_SUCCESS:
            return 'The validation result for stock: {0} date: {1} is {2}'.format(self.tscode, self.date, RESULT_SUCCESS)
        elif self.result == RESULT_FAIL:
            return 'The validation result for stock: {0} date: {1} is {2} and error details: \n {3}'.format(self.tscode, self.date, RESULT_FAIL,
                                                                                   '\n'.join(self.error_details))
        else:
            raise InvalidStatus('Invalid validation status {0} for stock: {1} date: {2}'.format(self.result, self.tscode, self.date))


class CompareResult(ValidationResult):
    """比较结果类

    Parameters
    ----------
    path : string
    same_count : int
    diff_count : int
    total_count : int
    diff_details : list
    """

    def __init__(self, file_name):
        self.file_name = file_name
        self.path = ''
        self.target_count = 0
        self.compare_count = 0
        self.same_count = 0
        self.diff_count = 0
        self.only_in_target_count = 0
        self.only_in_compare_count = 0
        self.only_in_target_list = []
        self.only_in_compare_list = []
        self.diff_details = []

    def print(self, path=''):
        if path != '':
            self.path = path
        file_writer = FileWriter(self.path)
        file_writer.write_file_line('--------{0}分析结果'.format(self.file_name))
        file_writer.write_file_line('--------目标数据集记录数{0}'.format(self.target_count))
        file_writer.write_file_line('--------对比数据集记录数{0}'.format(self.compare_count))
        file_writer.write_file_line('--------只包含在目标文件中的记录数{0},列表：'.format(self.only_in_target_count))
        self.only_in_target_list.sort()
        file_writer.write_file_line(str(self.only_in_target_list))
        file_writer.write_file_line('--------只包含在比较文件中的记录数{0},列表：'.format(self.only_in_compare_count))
        self.only_in_compare_list.sort()
        file_writer.write_file_line(str(self.only_in_compare_list))
        file_writer.write_file_line('--------比较结果相同的记录数{0}'.format(self.same_count))
        file_writer.write_file_line('--------比较结果不同的记录数{0},明细列表'.format(self.diff_count))
        for diff in self.diff_details:
            file_writer.write_file_line(diff)


# 和金字塔数据源比对
class Validator(metaclass=ABCMeta):
    """数据验证接口

    Parameters
    ----------
    data : DataFrame
        待处理数据.

    """

    @classmethod
    @abstractmethod
    def validate(self, data):
        """数据验证接口

        Parameters
        ----------
        data : DataFrame
            待处理数据.

        """
        pass

    @classmethod
    @abstractmethod
    def compare_validate(self, target_data, compare_data):
        """数据对比验证接口

        Parameters
        ----------
        target_data : DataFrame
            待验证数据.
        compare_data : DataFrame
            对比数据.
        """
        pass

    @classmethod
    @abstractmethod
    def convert(self, target_data, compare_data):
        """数据转换，将compare_data转换成target_data结构

        Parameters
        ----------
        target_data : DataFrame
            待验证数据.
        compare_data : DataFrame
            对比数据.
        """
        pass


class StockFilterCompressValidator(Validator):
    """股票过滤压缩数据比较验证

    Parameters
    ----------
    data : DataFrame
        待处理数据.

    """

    @classmethod
    def validate(self, data):
        print(data)

    @classmethod
    def compare_validate(self, target_data, compare_data):
        """数据比较验证接口，主要比较压缩后的文件与原始csv文件
        1. 数量
        2. 逐行摘要对比
        Parameters
        ----------
        target_data : DataFrame
            待验证数据.
        compare_data : DataFrame
            对比数据.
        """
        if (len(target_data) != len(compare_data)):
            raise ValidationFailed('Different data length')
        target_sign_list = [hashlib.md5('|'.join(item).encode('gbk')).hexdigest() for item in
                            [str(item) for item in target_data.values.tolist()]]
        compare_sign_list = [hashlib.md5('|'.join(item).encode('gbk')).hexdigest() for item in
                             [str(item) for item in target_data.values.tolist()]]
        diff = list(set(target_sign_list).difference(set(compare_sign_list)))
        diff.extend(list(set(compare_sign_list).difference(set(target_sign_list))))
        if diff:
            raise ValidationFailed('Different content')
        return True


class StockTickDataValidator(Validator):
    """股票Tick数据验证

    Parameters
    ----------
    data : DataFrame
        待处理数据.

    """

    _ignore_length_check = False

    def __init__(self, ignore_length_check = False):
        self._ignore_length_check = ignore_length_check

    @classmethod
    def validate(self, data):
        """数据验证接口
        1. 检查tick是否完整
        2. 检查是否包含开盘时间点的数据，插值时需要
        3. 检查成交量是否递增
        4. 检查是否停盘，price价全为0
        5. 检查除开盘集合竞价时间段之外是否有成交价为0的数据，属于非法数据
        6. 检查除集合竞价时间段之外是否有报价为0的数据，属于非法数据
        7. 检查除开盘集合竞价时间段之外是否有OCHL为0的数据，属于非法数据
        Parameters
        ----------
        data : DataFrame
            待处理数据.

        """
        tscode = data.iloc[0]['tscode']
        date = data.iloc[0]['date']
        time = data.iloc[0]['time']
        result = DtoStockValidationResult(RESULT_SUCCESS, [], tscode.split('.')[0], date.replace('-',''))
        # 检查是否停盘，如果停盘直接终止检查
        # if 'price' in data.loc[:, (data == 0).all()].columns.tolist():
        index_constituent_config_dao = IndexConstituentConfigDao()
        suspend_set = index_constituent_config_dao.get_suspend_list()
        if (date + tscode[0:6]) in suspend_set:
            result.result = RESULT_FAIL
            result.error_details.append('The stock is suspended')
            return result
        # 检查tick是否完整
        if self._ignore_length_check and len(data) < 4800:
            result.result = RESULT_FAIL
            result.error_details.append('The data is incomplete and length: {0}'.format(str(len(data))))
        # 检查是否包含开盘时间点的数据
        # if time > '09:16:00.000':
        #     print(time)
        #     result.result = RESULT_FAIL
        #     result.error_details.append('The data is incomplete and miss the opening time data')
        # 检查成交量是否递增
        time_sorted_list = data.sort_values(by='time')['daily_accumulated_volume'].to_list()
        time_sorted_abstract = hashlib.md5(
            '|'.join(list(map(lambda item: str(item), time_sorted_list))).encode('gbk')).hexdigest()
        volume_sorted_list = data.sort_values(by='daily_accumulated_volume')['daily_accumulated_volume'].to_list()
        volume_sorted_abstract = hashlib.md5(
            '|'.join(list(map(lambda item: str(item), volume_sorted_list))).encode('gbk')).hexdigest()
        if time_sorted_abstract != volume_sorted_abstract:
            result.result = RESULT_FAIL
            result.error_details.append('The volume has reverse order')
        # 检查除开盘集合竞价时间段之外是否有成交价为0的数据，属于非法数据
        if len(data[(data['price'] == 0) & (data['time'] > add_milliseconds_suffix('09:31:00'))]) > 0:
            print(data[(data['price'] == 0) & (data['time'] > add_milliseconds_suffix('09:31:00'))][['time','price']])
            result.result = RESULT_FAIL
            result.error_details.append('Invalid price value')
        # 检查除集合竞价时间段之外是否有报价为0的数据，属于非法数据 - 不需要这个检查
        # check_columns = ['time', 'bid_price1', 'bid_price2', 'bid_price3', 'bid_price4', 'bid_price5', 'bid_price6', 'bid_price7', 'bid_price8', 'bid_price9', 'bid_price10',
        #                   'bid_volume1', 'bid_volume2', 'bid_volume3', 'bid_volume4', 'bid_volume5', 'bid_volume6', 'bid_volume7', 'bid_volume8', 'bid_volume9', 'bid_volume10',
        #                   'ask_price1', 'ask_price2', 'ask_price3', 'ask_price4', 'ask_price5', 'ask_price6', 'ask_price7', 'ask_price8', 'ask_price9', 'ask_price10',
        #                   'ask_volume1', 'ask_volume2', 'ask_volume3', 'ask_volume4', 'ask_volume5', 'ask_volume6', 'ask_volume7', 'ask_volume8', 'ask_volume9', 'ask_volume10']
        # temp_data = data[check_columns]
        # temp_data = temp_data[(temp_data['time'] > add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)) & (temp_data['time'] < add_milliseconds_suffix(STOCK_CLOSE_CALL_AUACTION_START_TIME))]
        # for i in range(len(check_columns)):
        #     if i > 0:
        #         if 0 in temp_data[check_columns[i]].tolist():
        #             index = temp_data[check_columns[i]].tolist().index(0)
        #             print(temp_data.iloc[index][check_columns])
        #             result.result = RESULT_FAIL
        #             result.error_details.append('Invalid {0} value'.format(check_columns[i]))
        # 检查除开盘集合竞价时间段之外是否有OCHL为0的数据，属于非法数据
        if len(data[((data['open'] == 0) | (data['close'] == 0) | (data['high'] == 0) | (data['low'] == 0))
               & (data['time'] > add_milliseconds_suffix('09:31:00'))]) > 0:
            print(data[((data['open'] == 0) | (data['close'] == 0) | (data['high'] == 0) | (data['low'] == 0))
               & (data['time'] > add_milliseconds_suffix('09:31:00'))][['time','open','close','high','low']])
            result.result = RESULT_FAIL
            result.error_details.append('Invalid OCHL value')
        return result

    @classmethod
    def compare_validate(self, target_data, compare_data):
        """数据比较验证接口
        1. 和通达信数据随机采样比对
        Parameters
        ----------
        target_data : DataFrame
            待验证数据.
        compare_data : DataFrame
            对比数据.
        """
        # Todo
        return True

    @classmethod
    def convert(self, target_data, compare_data):
        """数据转换，将compare_data转换成target_data结构

        Parameters
        ----------
        target_data : DataFrame
            待验证数据.
        compare_data : DataFrame
            对比数据.
        """
        pass

class StockOrganizedDataValidator(Validator):
    """股票Organize数据验证

    Parameters
    ----------
    data : DataFrame
        待处理数据.

    """


    @classmethod
    def validate(self, data):
        """数据验证接口
        1. 是否有中午休盘时的数据
        2. 是否有补齐时未清零的数据
        3. 是否有重复数据
        4. 11：30的数据
        5. 集合竞价数据
        Parameters
        ----------
        data : DataFrame
            待处理数据.

        """
        tscode = data.iloc[0]['tscode']
        date = data.iloc[0]['date']
        result = DtoStockValidationResult(RESULT_SUCCESS, [], tscode.split('.')[0], date.replace('-',''))
        # 是否有中午休盘时的数据
        # if len(data[(data['time'] > add_milliseconds_suffix(STOCK_TRANSACTION_NOON_END_TIME)) & (data['time'] < add_milliseconds_suffix(STOCK_TRANSACTION_NOON_START_TIME))]) > 0:
        #     result.result = RESULT_FAIL
        #     result.error_details.append('The redundant data for noon break exists')
        # 是否有补齐时未清零的数据
        # data['delta_daily_accumulated_volume'] = data['daily_accumulated_volume'] - data['daily_accumulated_volume'].shift(1)
        # if len(data[(data['delta_daily_accumulated_volume'] == 0) & (data['volume'] > 0)]) > 0:
        #     print(data[(data['delta_daily_accumulated_volume'] == 0) & (data['volume'] > 0)][['time','delta_daily_accumulated_volume','volume']])
        #     result.result = RESULT_FAIL
        #     result.error_details.append('Invalid volume should to be fixed')
        # 是否有重复数据
        temp_data = data[(data['time'] <= STOCK_TRANSACTION_END_TIME + '.000') & (data['time'] >= STOCK_TRANSACTION_START_TIME + '.000') & ((data['time'] >= STOCK_TRANSACTION_NOON_START_TIME + '.000') | (data['time'] <= STOCK_TRANSACTION_NOON_END_TIME + '.000'))]
        temp_data = temp_data.groupby('time')[['time']].count()
        temp_data = temp_data[temp_data['time'] > 1]
        if len(temp_data) > 0:
            result.result = RESULT_FAIL
            result.error_details.append('Repeat records')
        # 是否有11：30的数据
        if len(data[data['time'] == '11:30:00.000']) == 0:
            result.result = RESULT_FAIL
            result.error_details.append('Miss 11:30:00 data')
        return result

    @classmethod
    def compare_validate(self, target_data, compare_data):
        """数据比较验证接口
        1. 和通达信数据随机采样比对
        Parameters
        ----------
        target_data : DataFrame
            待验证数据.
        compare_data : DataFrame
            对比数据.
        """
        # Todo
        return True

    @classmethod
    def convert(self, target_data, compare_data):
        """数据转换，将compare_data转换成target_data结构

        Parameters
        ----------
        target_data : DataFrame
            待验证数据.
        compare_data : DataFrame
            对比数据.
        """
        pass


class FutureTickDataValidator(Validator):
    """期货tick数据验证

    Parameters
    ----------
    data : DataFrame
        待处理数据.

    """

    @classmethod
    def validate(self, data):
        """数据验证接口
        Parameters
        ----------
        data : DataFrame
            待处理数据.

        """
        return True

    @classmethod
    @timing
    def compare_validate(self, target_data, compare_data, file_name):
        """数据比较验证接口
        按时间戳进行比对
        1. 检查哪些时间戳数据有差异，数据对齐
        Parameters
        ----------
        target_data : DataFrame
            待验证数据.
        compare_data : DataFrame
            对比数据.
        """
        compare_result = CompareResult(file_name)
        # 按compare_data裁剪
        start_time = compare_data.loc[1]['datetime']
        end_time = compare_data.tail(1)['datetime'].tolist()[0]
        print('Before filter: %s' % len(target_data))
        target_data = target_data[(target_data['datetime'] >= start_time) & (target_data['datetime'] <= end_time)]
        print('After filter: %s' % len(target_data))
        # 获取目标数据集事件驱动列表
        target_data['delta_volume'] = target_data['volume'] - target_data['volume'].shift(1)
        first_index = target_data.head(1).index.tolist()[0]
        target_data.loc[first_index, 'delta_volume'] = target_data.loc[first_index, 'volume']  # 这里要处理边界情况，主要是第一个值的delta_volume为空
        target_data = target_data[target_data['delta_volume'] > 0]
        target_data_list = target_data['datetime'].tolist()
        compare_data_list = compare_data['datetime'].tolist()
        only_in_target = list(set(target_data_list).difference(set(compare_data_list)))
        only_in_compare = list(set(compare_data_list).difference(set(target_data_list)))
        # 处理比较数据的对齐问题
        to_be_corrected_compare = list(
            filter(lambda dt: self.get_pair_tick_time(dt) in only_in_target, only_in_compare))
        to_be_corrected_target = list(map(lambda dt: self.get_pair_tick_time(dt), to_be_corrected_compare))
        only_in_target = list(set(only_in_target) - set(to_be_corrected_target))
        only_in_compare = list(set(only_in_compare) - set(to_be_corrected_compare))
        compare_result.only_in_target_count = len(only_in_target)
        compare_result.only_in_target_list = only_in_target
        compare_result.only_in_compare_count = len(only_in_compare)
        compare_result.only_in_compare_list = only_in_compare
        # compare_data比target_data少很多数据，遍历
        union_set = list(set(compare_data_list) & set(target_data_list))
        same_count = 0
        diff_count = 0
        diff_details = []
        for dt in union_set:
            try:
                # compare_abstract = self.create_abstract(compare_data, dt, ['current', 'a1_p', 'b1_p', 'a1_v', 'b1_v', 'volume']).compute()
                compare_abstract = self.create_abstract(compare_data, dt,
                                                        ['current', 'a1_p', 'b1_p', 'a1_v', 'b1_v', 'volume'])
                # target_abstract = self.create_abstract(target_data, dt, ['last_price', 'ask_price1', 'bid_price1', 'ask_volume1', 'bid_volume1', 'volume']).compute()
                target_abstract = self.create_abstract(target_data, dt,
                                                       ['last_price', 'ask_price1', 'bid_price1', 'ask_volume1',
                                                        'bid_volume1', 'volume'])
            except Exception as e:
                diff_count = diff_count + 1
                diff_details.append('Invalid data for {0} and error: {1}'.format(dt, e))
                continue
            if compare_abstract == target_abstract:
                same_count = same_count + 1
            else:
                diff_count = diff_count + 1
                diff_details.append(dt + ':' + compare_abstract + ' <> ' + target_abstract)
        # 处理比较数据的对齐问题
        for dt in to_be_corrected_target:
            try:
                compare_abstract = self.create_abstract(compare_data, self.get_pair_tick_time(dt),
                                                        # ['current', 'a1_p', 'b1_p', 'a1_v', 'b1_v', 'volume']).compute()
                                                        ['current', 'a1_p', 'b1_p', 'a1_v', 'b1_v', 'volume'])
                # target_abstract = self.create_abstract(target_data, dt, ['last_price', 'ask_price1', 'bid_price1', 'ask_volume1', 'bid_volume1', 'volume']).compute()
                target_abstract = self.create_abstract(target_data, dt,
                                                       ['last_price', 'ask_price1', 'bid_price1', 'ask_volume1',
                                                        'bid_volume1', 'volume'])
            except Exception as e:
                diff_count = diff_count + 1
                diff_details.append('Invalid data for {0} and error: {1}'.format(dt, e))
                continue
            if compare_abstract == target_abstract:
                same_count = same_count + 1
            else:
                diff_count = diff_count + 1
                diff_details.append(dt + ':' + compare_abstract + ' <> ' + target_abstract)
        compare_result.same_count = same_count
        compare_result.diff_count = diff_count
        compare_result.target_count = len(target_data)
        compare_result.compare_count = len(compare_data)
        compare_result.path = FUTURE_TICK_REPORT_DATA_PATH + '/' + file_name + '_compare_result'
        compare_result.diff_details = diff_details
        compare_result.print()
        return True

    @classmethod
    # @delayed
    def create_abstract(cls, data, dt, column_list):
        value_list = data[data['datetime'] == dt][
            column_list].iloc[0].tolist()
        abstract = '|'.join(list(map(lambda item: cls.format(item), value_list)))
        return abstract

    def convert(self, target_data, compare_data):
        """字段映射：
        time: datetime
        current: CFFEX.IC1701.last_price
        a1_p: CFFEX.IC1701.ask_price1
        b1_p: CFFEX.IC1701.bid_price1
        a1_v: CFFEX.IC1701.ask_volume1
        b1_v: CFFEX.IC1701.bid_volume1
        volume: CFFEX.IC1701.volume
        position: ?


        Parameters
        ----------
        target_data : DataFrame
            待验证数据.
        compare_data : DataFrame
            对比数据.
        """
        compare_data['datetime'] = compare_data.apply(lambda item: self.date_format(str(item['time'])), axis=1)
        target_data['datetime'] = target_data.apply(lambda item: date_alignment(str(item['datetime'])), axis=1)
        return compare_data

    def date_format(self, date):
        # 20170103092900.0 -> 2017-01-03 09:29:00.000000000
        return date_alignment(date[0:4] + '-' + date[4:6] + '-' + date[6:8] + ' ' \
                                   + date[8:10] + ':' + date[10:12] + ':' + date[12:16] + '00000000')

    def format(num=0):
        if isinstance(num, float):
            return str(num)
        elif isinstance(num, numpy.int64):
            return str(num) + '.0'
        else:
            return num

    def get_pair_tick_time(datetime):
        """根据原始tick时间找到成对的tick时间：
        因为0.5秒一个tick，对其之后成对的应该是2017-03-17 09:30:16.500000000  <-> 2017-03-17 09:30:16.000000000

        Parameters
        ----------
        datetime : string 原始时间.
        """
        if datetime[20:] == '000000000':
            return datetime[0:20] + '500000000'
        elif datetime[20:] == '500000000':
            return datetime[0:20] + '000000000'
        else:
            return datetime


if __name__ == '__main__':
    # 测试股指tick数据比较验证
    # target_data = pd.read_csv('/Users/finley/Projects/stock-index-future/data/original/future/tick/IF/CFFEX.IF1705.csv')
    # # target_data = pd.read_csv('E:\\data\\original\\future\\tick\\IH\\CFFEX.IH2208.csv')
    # target_data = FutureTickDataColumnTransform('IF', 'IF1705').process(target_data)
    # # compare_data = pd.DataFrame(pd.read_pickle('E:\\data\\compare\\future\\tick\\IH\\IH2208.CCFX-ticks.pkl'))
    # compare_data = pd.DataFrame(
    #     pd.read_pickle('/Users/finley/Projects/stock-index-future/data/original/future/tick/IF1705.CCFX-ticks.pkl'))
    # compare_data = FutureTickDataValidator().convert(target_data, compare_data)
    # FutureTickDataValidator().compare_validate(target_data, compare_data, 'IF1705')
    # 测试股票tick数据验证
    # path = '/Users/finley/Projects/stock-index-future/data/original/stock/tick/stk_tick10_w_2017/stk_tick10_w_201701/20170126/600917.pkl'
    path = 'D:\\liuli\\data\\original\\stock\\tick\\stk_tick10_w_2018\\stk_tick10_w_201809\\20180913\\600155.pkl'
    data = read_decompress(path)
    data = StockTickDataColumnTransform().process(data)
    data = StockTickDataCleaner().process(data)
    print(StockTickDataValidator().validate(data))
    # 测试organised股票数据验证
    # path = 'E:\\data\\organized\\stock\\tick\\stk_tick10_w_2022\\stk_tick10_w_202203\\20220324\\000858.pkl'
    # data = read_decompress(path)
    # print(StockOrganizedDataValidator().validate(data))

