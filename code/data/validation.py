#! /usr/bin/env python
# -*- coding:utf8 -*-
import re
from abc import ABCMeta, abstractmethod
import hashlib

import numpy
import pandas as pd

from common.aop import timing
from common.constants import RESULT_SUCCESS, RESULT_FAIL, TEMP_PATH, FUTURE_TICK_REPORT_DATA_PATH
from common.exception.exception import ValidationFailed, InvalidStatus
from common.io import FileWriter
from data.process import FutureTickDataColumnTransform


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
            return 'The validation result is {0} and error details: \n {1}'.format(RESULT_SUCCESS, '\n'.join(self.error_details))
        else:
            raise InvalidStatus('Invalid validation status {0}'.format(self.result))

    
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

    def print(self, path = ''):
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



    
#和金字塔数据源比对
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
        target_sign_list = [hashlib.md5('|'.join(item).encode('gbk')).hexdigest() for item in [str(item) for item in target_data.values.tolist()]]
        compare_sign_list = [hashlib.md5('|'.join(item).encode('gbk')).hexdigest() for item in [str(item) for item in target_data.values.tolist()]]
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

    @classmethod
    def validate(self, data):
        """数据验证接口
        1. 检查成交量是否递增
        Parameters
        ----------
        data : DataFrame
            待处理数据.

        """
        print(data)

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
        #Todo
        return True

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
        print(data)

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
        #按compare_data裁剪
        start_time = compare_data.loc[1]['datetime']
        end_time = compare_data.tail(1)['datetime'].tolist()[0]
        print('Before filter: %s' % len(target_data))
        target_data = target_data[(target_data['datetime'] >= start_time) & (target_data['datetime'] <= end_time)]
        print('After filter: %s' % len(target_data))
        #获取目标数据集事件驱动列表
        target_data['delta_volume'] = target_data['volume'] - target_data['volume'].shift(1)
        target_data = target_data[target_data['delta_volume'] > 0]
        target_data_list = target_data['datetime'].tolist()
        compare_data_list = compare_data['datetime'].tolist()
        only_in_target = list(set(target_data_list).difference(set(compare_data_list)))
        only_in_compare = list(set(compare_data_list).difference(set(target_data_list)))
        to_be_corrected_compare = list(filter(lambda dt: self.get_pair_tick_time(dt) in only_in_target, only_in_compare))
        to_be_corrected_target = list(map(lambda dt: self.get_pair_tick_time(dt), to_be_corrected_compare))
        only_in_target = list(set(only_in_target) - set(to_be_corrected_target))
        only_in_compare = list(set(only_in_compare) - set(to_be_corrected_compare))
        compare_result.only_in_target_count = len(only_in_target)
        compare_result.only_in_target_list = only_in_target
        compare_result.only_in_compare_count = len(only_in_compare)
        compare_result.only_in_compare_list = only_in_compare
        #compare_data比target_data少很多数据，遍历
        union_set = list(set(compare_data_list) & set(target_data_list))
        same_count = 0
        diff_count = 0
        diff_details = []
        for dt in union_set:
            try:
                current_compare_value_list = compare_data[compare_data['datetime'] == dt][['current','a1_p','b1_p','a1_v','b1_v','volume']].iloc[0].tolist()
                compare_abstract = '|'.join(list(map(lambda item: self.format(item), current_compare_value_list)))
                current_target_value_list = target_data[target_data['datetime'] == dt][['last_price','ask_price1','bid_price1','ask_volume1','bid_volume1','volume']].iloc[0].tolist()
                target_abstract = '|'.join(list(map(lambda item: self.format(item), current_target_value_list)))
            except Exception as e:
                diff_count = diff_count + 1
                diff_details.append('Invalid data for {0} and error: {1}'.format(dt, e))
                continue
            if compare_abstract == target_abstract:
                same_count = same_count + 1
            else:
                diff_count = diff_count + 1
                diff_details.append(dt + ':' + compare_abstract + ' <> ' + target_abstract)
        for dt in to_be_corrected_target:
            try:
                current_compare_value_list = compare_data[compare_data['datetime'] == self.get_pair_tick_time(dt)][
                    ['current', 'a1_p', 'b1_p', 'a1_v', 'b1_v', 'volume']].iloc[0].tolist()
                compare_abstract = '|'.join(list(map(lambda item: self.format(item), current_compare_value_list)))
                current_target_value_list = target_data[target_data['datetime'] == dt][
                    ['last_price', 'ask_price1', 'bid_price1', 'ask_volume1', 'bid_volume1',
                     'volume']].iloc[0].tolist()
                target_abstract = '|'.join(list(map(lambda item: self.format(item), current_target_value_list)))
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
        target_data['datetime'] = target_data.apply(lambda item: self.date_alignment(str(item['datetime'])), axis=1)
        return compare_data

    def date_format(self, date):
        #20170103092900.0 -> 2017-01-03 09:29:00.000000000
        return self.date_alignment(date[0:4] + '-' + date[4:6] + '-' + date[6:8] + ' ' \
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

    def date_alignment(self, date):
        #分秒对齐
        subsec = 0
        if int(date.split('.')[1][0]) > 4:
            subsec = 5
        return date.split('.')[0] + '.' + str(subsec) + '00000000'



if __name__ == '__main__':
    target_data = pd.read_csv('D:\\liuli\\data\\original\\future\\IC\\CFFEX.IC1909.csv')
    target_data = FutureTickDataColumnTransform('IC','IC1909').process(target_data)
    compare_data = pd.DataFrame(pd.read_pickle('D:\\ic2017-2021\\IC1909.CCFX-ticks.pkl'))
    compare_data = FutureTickDataValidator().convert(target_data, compare_data)
    FutureTickDataValidator().compare_validate(target_data, compare_data, 'IC1909')


