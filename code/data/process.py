#! /usr/bin/env python
# -*- coding:utf8 -*-
import math
import numpy as np
import os
import pandas as pd
import sys
from abc import abstractmethod, ABCMeta
from datetime import datetime, timedelta, time
from pandas import DataFrame

from common import constants
from common.aop import timing
from common.constants import OFF_TIME_IN_SECOND, OFF_TIME_IN_MORNING, STOCK_TRANSACTION_START_TIME, \
    STOCK_VALID_DATA_STARTTIME, CONFIG_PATH, RET_PERIOD
from common.localio import read_decompress, save_compress
from common.timeutils import datetime_advance, date_alignment, time_carry, time_advance, add_milliseconds_suffix
from data.analysis import FutureTickerHandler, StockTickerHandler

parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parentdir)
import pandas as pd


class DataProcessor(metaclass=ABCMeta):
    """数据处理接口

    """

    @classmethod
    @abstractmethod
    def process(self, data):
        """数据处理接口

        Parameters
        ----------
        data : DataFrame
            待处理数据.

        """
        pass


class DataCleaner(DataProcessor):
    """数据清洗：
    删除NaN

    Parameters
    ----------
    data : DataFrame
        待处理数据.

    """

    def process(self, data):
        data = data.dropna(inplace=False)
        return data


class StockTickDataCleaner(DataCleaner):
    """股票数据清洗：
    删除不需要的列
    删除9：15之前的数据
    删除非法数据：
    000028.SZ               28  2017-01-05         0::.0    0.0       0

    Parameters
    ----------
    data : DataFrame
        待处理数据.

    """

    def __init__(self):
        self._ignore_columns = ['iopv', 'total_varieties', 'total_increase_varieties', 'total_falling_varieties',
                                'total_equal_varieties']

    def process(self, data):
        super().process(data)
        # 清除多余的列
        data = data.drop(columns=self._ignore_columns)
        # 按时间清除 小于09:15:00，11：30到13：00，大于15：00的数据
        data = data.drop(data.index[data['time'] < constants.STOCK_START_TIME + '.000'])
        # 清除盘后交易数据
        data = data.drop(data.index[(data['time'] > constants.STOCK_TRANSACTION_END_TIME + '.000') & (data['price'] == 0)])
        data = data.drop(data.index[(data['time'] > constants.STOCK_TRANSACTION_END_TIME + '.000') & (data['transaction_number'] == 0)])
        data = data.drop(data.index[(data['time'] > constants.STOCK_TRANSACTION_NOON_END_TIME + '.000') & (data['time'] < constants.STOCK_TRANSACTION_NOON_START_TIME + '.000')])
        # 清除000028.SZ               28  2017-01-05         0::.0    0.0       0
        data = data.drop(data.index[data['time'] == '0::.0'])
        # 清除股票的重复成交量为0的数据，如果多行的成交量都为0，保留一行
        data['count'] = data.groupby(['time'])['time'].transform('count')
        # 处理有成交量记录不为0的数据
        index_to_be_serverd = data.index[(data['count'] > 1) & (data['volume'] > 0) & (data['time'] >= constants.STOCK_OPEN_CALL_AUACTION_2ND_STAGE_END_TIME)]
        for index in index_to_be_serverd:
            time_to_handled = data.loc[index]['time']
            data = data.drop(data.index[(data['time'] == time_to_handled) & (data['volume'] == 0)])
        # 处理没有成交量记录不为0的数据
        data['count'] = data.groupby(['time'])['time'].transform('count')
        index_to_be_removed = data.index[(data['count'] > 1) & (data['volume'] == 0) & ( data['time'] >= constants.STOCK_OPEN_CALL_AUACTION_2ND_STAGE_END_TIME)]
        handled_time_set = set()
        for index in index_to_be_removed:
            cur_time = data.loc[index]['time']
            if cur_time not in handled_time_set:
                data = data.drop(index)
                handled_time_set.add(cur_time)
        return data


class StockTickDataColumnTransform(DataProcessor):
    """翻译股票Tick数据的列名：
    代码          tscode
    交易所代码     exchange_tscode
    自然日        date
    时间          time
    成交价        price
    成交量        volume
    成交额        amount
    成交笔数      transaction_number
    IOPV(先忽略)  iopv
    成交标志(先忽略)  transaction_flag
    BS标志(先忽略)  bs_flag
    当日累计成交量  daily_accumulated_volume
    当日成交额     daily_amount
    最高价        high
    最低价        low
    开盘价        open
    前收盘        close
    申卖价1       bid_price1
    申卖价2       bid_price2
    申卖价3       bid_price3
    申卖价4       bid_price4
    申卖价5       bid_price5
    申卖价6       bid_price6
    申卖价7       bid_price7
    申卖价8       bid_price8
    申卖价9       bid_price9
    申卖价10      bid_price10
    申卖量1       bid_volume1
    申卖量2       bid_volume2
    申卖量3       bid_volume3
    申卖量4       bid_volume4
    申卖量5       bid_volume5
    申卖量6       bid_volume6
    申卖量7       bid_volume7
    申卖量8       bid_volume8
    申卖量9       bid_volume9
    申卖量10      bid_volume10
    申买价1       ask_price1
    申买价2       ask_price2
    申买价3       ask_price3
    申买价4       ask_price4
    申买价5       ask_price5
    申买价6       ask_price6
    申买价7       ask_price7
    申买价8       ask_price8
    申买价9       ask_price9
    申买价10      ask_price10
    申买量1       ask_volume1
    申买量2       ask_volume2
    申买量3       ask_volume3
    申买量4       ask_volume4
    申买量5       ask_volume5
    申买量6       ask_volume6
    申买量7       ask_volume7
    申买量8       ask_volume8
    申买量9       ask_volume9
    申买量10      ask_volume10
    加权平均叫卖价 weighted_average_bid_price
    加权平均叫买价 weighted_average_ask_price
    叫卖总量      total_bid_volume
    叫买总量      total_ask_volume
    不加权指数    unwighted_index
    品种总数(先忽略)      total_varieties
    上涨品种数(先忽略)    total_increase_varieties
    下跌品种数(先忽略)    total_falling_varieties
    持平品种数(先忽略)    total_equal_varieties

    Parameters
    ----------
    data : DataFrame
        待处理数据.

    """

    def __init__(self):
        self._columns = ['tscode', 'exchange_tscode', 'date', 'time', 'price', 'volume', 'amount', 'transaction_number',
                         'iopv', 'transaction_flag', 'bs_flag', 'daily_accumulated_volume', 'daily_amount', 'high',
                         'low', 'open', 'close', 'ask_price1', 'ask_price2', 'ask_price3', 'ask_price4', 'ask_price5',
                         'ask_price6', 'ask_price7', 'ask_price8', 'ask_price9', 'ask_price10', 'ask_volume1',
                         'ask_volume2', 'ask_volume3', 'ask_volume4', 'ask_volume5', 'ask_volume6', 'ask_volume7',
                         'ask_volume8', 'ask_volume9', 'ask_volume10', 'bid_price1', 'bid_price2', 'bid_price3',
                         'bid_price4', 'bid_price5', 'bid_price6', 'bid_price7', 'bid_price8', 'bid_price9',
                         'bid_price10', 'bid_volume1', 'bid_volume2', 'bid_volume3', 'bid_volume4', 'bid_volume5',
                         'bid_volume6', 'bid_volume7', 'bid_volume8', 'bid_volume9', 'bid_volume10',
                         'weighted_average_ask_price', 'weighted_average_bid_price', 'total_ask_volume',
                         'total_bid_volume', 'unwighted_index', 'total_varieties', 'total_increase_varieties',
                         'total_falling_varieties', 'total_equal_varieties']

    def get_columns(self):
        return self._columns

    def process(self, data):
        if len(data.columns) > len(self._columns):
            columns_to_be_deleted = []
            for i in range(len(data.columns) - len(self._columns)):
                columns_to_be_deleted.append(data.columns.tolist()[len(data.columns) - (i + 1)])
            data = data.drop(columns_to_be_deleted, axis=1)
            print(data)
        data.columns = self._columns
        return data

class StockTickDataEnricher(DataProcessor):
    """股票Tick填充：先对齐后插值
    对齐：从9点30开始
    插值：基于对齐结果

    Parameters
    ----------
    data : DataFrame
        待处理数据.

    """
    @timing
    def process(self, data):
        pre_data = data[data['time'] < STOCK_TRANSACTION_START_TIME]

        print(data[['time', 'price', 'open', 'high', 'low', 'close']])
        #处理开盘数据缺失，用昨日收盘价补齐
        last_close = data[data['close'] > 0].iloc[0]['close']
        if len(data[(data['time'] >= add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)) & (data['time'] <= add_milliseconds_suffix(
                STOCK_VALID_DATA_STARTTIME)) & (data['price'] == 0)]) > 0:
            data.loc[(data['time'] >= add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)) & (data['time'] <= add_milliseconds_suffix(
                STOCK_VALID_DATA_STARTTIME)) & (data['price'] == 0), 'price'] = last_close
        if len(data[(data['time'] >= add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)) & (data['time'] <= add_milliseconds_suffix(
                STOCK_VALID_DATA_STARTTIME)) & (data['open'] == 0)]) > 0:
            data.loc[(data['time'] >= add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)) & (data['time'] <= add_milliseconds_suffix(
                STOCK_VALID_DATA_STARTTIME)) & (data['open'] == 0), 'open'] = last_close
        if len(data[(data['time'] >= add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)) & (data['time'] <= add_milliseconds_suffix(
                STOCK_VALID_DATA_STARTTIME)) & (data['high'] == 0)]) > 0:
            data.loc[(data['time'] >= add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)) & (data['time'] <= add_milliseconds_suffix(
                STOCK_VALID_DATA_STARTTIME)) & (data['high'] == 0), 'high'] = last_close
        if len(data[(data['time'] >= add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)) & (data['time'] <= add_milliseconds_suffix(
                STOCK_VALID_DATA_STARTTIME)) & (data['low'] == 0)]) > 0:
            data.loc[(data['time'] >= add_milliseconds_suffix(STOCK_TRANSACTION_START_TIME)) & (data['time'] <= add_milliseconds_suffix(
                STOCK_VALID_DATA_STARTTIME)) & (data['low'] == 0), 'low'] = last_close

        #对齐
        data = data[data['time'] >= STOCK_TRANSACTION_START_TIME]
        to_be_removed_index = []
        for index, row_data in data.iterrows():
            cur_time = datetime.strptime(row_data['time'], '%H:%M:%S.%f')
            if cur_time.second % 3 == 0:
                continue
            else:
                new_hour, new_minute, new_second = time_carry(cur_time.hour, cur_time.minute, (int((cur_time.second + 3) / 3)) * 3)
                new_time = datetime(year=cur_time.year, month=cur_time.month, day=cur_time.day, hour=new_hour, minute=new_minute, second=new_second)
                if len(data[data['time'] == datetime.strftime(new_time, '%H:%M:%S') + '.000']) == 0:
                    data['time'][index] = datetime.strftime(new_time, '%H:%M:%S') + '.000'
                else:
                    to_be_removed_index.append(index)
            # if len(to_be_removed_index) > 0: # 这种数据有可能包含有用信息，所以应该保留
            #     data.drop(to_be_removed_index)

        #处理收盘集中竞价
        temp_data = data[data['volume'] > 0]
        if len(temp_data) > 0:
            time = temp_data.iloc[-1]['time']
            # 收盘集合竞价的时间戳大于15：00：00
            if time > constants.STOCK_TRANSACTION_END_TIME + '.000':
                # 删除15：00：00的数据，将最后集合竞价成交的tick时间改为15：00：00，清楚之后所有的数据（盘后交易数据）
                data = data.drop(data.index[data['time'] == constants.STOCK_TRANSACTION_END_TIME + '.000'])
                data.loc[(data['time']) == time, 'time'] = constants.STOCK_TRANSACTION_END_TIME + '.000'
                data = data.drop(data.index[data['time'] > constants.STOCK_TRANSACTION_END_TIME + '.000'])

        #插值
        miss_data = pd.DataFrame(columns=data.columns.tolist())
        data['realtime'] = data.apply(
            lambda item: datetime.strptime(item['time'], "%H:%M:%S.%f"), axis=1)
        data['delta_time'] = data.shift(-1)['realtime'] - data['realtime']
        data['delta_time_sec'] = data.apply(
            lambda item: self.handle_off_time(item), axis=1)
        issued_data = data[data['delta_time_sec'] > constants.STOCK_TICK_SAMPLE_INTERVAL]
        for index, row_data in issued_data.iterrows():
            delta_time_sec = row_data[64]
            step = constants.STOCK_TICK_SAMPLE_INTERVAL
            item = data.loc[index]
            str_cur_time = item['time']
            while step < delta_time_sec:
                item['time'] = time_advance(str_cur_time, step)
                item['volume'] = 0
                item['amount'] = 0
                miss_data = miss_data.append(item)
                step = step + constants.STOCK_TICK_SAMPLE_INTERVAL
        data = data.append(miss_data)
        data = pd.concat([pre_data, data])

        data = data.sort_values(by=['time'])
        data = data.reset_index(drop=True)
        return data

    def handle_off_time(self, item):
        seconds = item['delta_time'].total_seconds()
        if seconds >= OFF_TIME_IN_SECOND:
            return seconds - OFF_TIME_IN_SECOND
        else:
            return seconds



class FutureTickDataColumnTransform(DataProcessor):
    """更改Tick数据的列名，去掉合约信息：
    CFFEX.IF2212.last_price,CFFEX.IF2212.highest,CFFEX.IF2212.lowest,CFFEX.IF2212.bid_price1,CFFEX.IF2212.bid_volume1,CFFEX.IF2212.ask_price1,CFFEX.IF2212.ask_volume1,CFFEX.IF2212.volume,CFFEX.IF2212.amount,CFFEX.IF2212.open_interest

    Parameters
    ----------
    data : DataFrame
        待处理数据.

    """

    def __init__(self, product, instrument):
        self._product = product
        self._instrument = instrument

    def process(self, data):
        column_list = data.columns.tolist()[1:]
        new_column_list = [column.replace(constants.FUTURE_TICK_FILE_PREFIX + '.' + self._instrument + '.', '') for column in
                           column_list]
        column_map = dict(map(lambda x, y: [x, y], column_list, new_column_list))
        data = data.rename(columns=column_map)
        return data


class FutureTickDataEnricher(DataProcessor):
    """Tick填充：先数据对齐，再数据填充

    Parameters
    ----------
    data : DataFrame
        待处理数据.
                                  str_datetime  ... delta_time_sec
    3        2022-04-18 09:30:00.400000000  ...           60.0
    6        2022-04-18 09:30:02.400000000  ...            1.0
    8        2022-04-18 09:30:03.900000000  ...            1.0
    12       2022-04-18 09:30:07.900000000  ...            2.5
    13       2022-04-18 09:30:09.400000000  ...            1.5
    ...                                ...  ...            ...
    2190186  2022-09-28 14:59:40.300000000  ...            1.0
    2190210  2022-09-28 14:59:52.800000000  ...            1.0
    2190220  2022-09-28 14:59:58.300000000  ...            1.0
    2190221  2022-09-28 14:59:59.300000000  ...            1.0
    2190223  2022-09-28 14:59:59.999500000  ...            0.6

    """

    @timing
    def process(self, data):
        miss_data = pd.DataFrame(columns=data.columns.tolist())
        dt = data['datetime'].to_frame('str_datetime')  # 转成一个dataframe
        dt['datetime'] = dt.apply(lambda dt: datetime.strptime(dt['str_datetime'][0:21], "%Y-%m-%d %H:%M:%S.%f"), axis=1)
        dt['date'] = dt.apply(lambda dt: dt['str_datetime'][0:10], axis=1)
        date_list = dt.drop_duplicates(subset = 'date')['date'].tolist()
        for date in date_list:
            print(date)
            temp_data = dt[dt['date'] == date]
            temp_data['delta_time'] = temp_data.shift(-1)['datetime'] - temp_data['datetime']
            temp_data['delta_time_sec'] = temp_data.apply(
                lambda item: self.handle_off_time(item), axis=1)
            temp_data = temp_data[temp_data['delta_time_sec'] > constants.FUTURE_TICK_SAMPLE_INTERVAL]
            for index, row_data in temp_data.iterrows():
                delta_time_sec = row_data[4]
                print(delta_time_sec)
                step = constants.FUTURE_TICK_SAMPLE_INTERVAL
                item = data.loc[index]
                str_cur_time = item['datetime']
                while step < delta_time_sec:
                    item['datetime'] = datetime_advance(str_cur_time, step)
                    miss_data = miss_data.append(item)
                    step = step + constants.FUTURE_TICK_SAMPLE_INTERVAL
        data = data.append(miss_data)
        data = data.sort_values(by=['datetime'])
        data = data.reset_index(drop=True)
        return data

    def handle_off_time(self, item):
        seconds = item['delta_time'].total_seconds()
        if seconds >= OFF_TIME_IN_SECOND:
            return seconds - OFF_TIME_IN_SECOND
        else:
            return seconds


class FutureTickDataProcessorPhase1(DataProcessor):
    """Tick处理，3秒为一个时间间隔对齐，生成临时文件

    Parameters
    ----------
    data : DataFrame
        待处理数据.
    """

    _columns = ['datetime', 'open', 'close', 'high', 'low', 'volume', 'interest']

    @timing
    def process(self, data):
        date = data.iloc[1]['date']
        # 将数据对齐到0.5s的整数倍
        data['datetime'] = data.apply(lambda item: date_alignment(str(item['datetime'])), axis=1)
        # 只取有成交的记录
        data['delta_volume'] = data['volume'] - data['volume'].shift(1)
        first_index = data.head(1).index.tolist()[0]
        data.loc[first_index, 'delta_volume'] = data.loc[first_index, 'volume']  # 这里要处理边界情况，主要是第一个值的delta_volume为空
        # data['cur_price'] = data['last_price'].shift(-1) #对于last_price有点误解，这个就是当前时间
        data['cur_price'] = data['last_price']
        data = data[data['delta_volume'] > 0]
        cur_time = date + ' 09:29:00.000000000' #股指期货包含集合竞价
        end_time = date + ' 15:00:00.000000000'
        is_open = True
        organized_data = pd.DataFrame(columns=self._columns)
        while cur_time < end_time:
            if is_open: #判断是否开盘, 如果是开盘从9：29：00 - 9：30：03时间间隔是63秒
                next_time = datetime_advance(cur_time, 63)
                is_open = False
            else:
                next_time = datetime_advance(cur_time, 3)
            temp_data = data[(data['datetime'] >= cur_time) & (data['datetime'] < next_time)]
            last_record = None
            # 该时间区间内无交易处理
            if len(temp_data) == 0:
                if len(organized_data) > 0:
                    last_record = organized_data.iloc[-1]
                    last_record['datetime'] = next_time
                    last_record['volume'] = 0
                    close = last_record['close']
                    last_record['open'] = close
                    last_record['low'] = close
                    last_record['high'] = close
            # 该时间区间内有交易处理
            else:
                min_index = temp_data.index.min()
                max_index = temp_data.index.max()
                open = temp_data.loc[min_index]['cur_price']
                close = temp_data.loc[max_index]['cur_price']
                high = temp_data['cur_price'].max()
                low = temp_data['cur_price'].min()
                volume = temp_data['delta_volume'].sum()
                interest = temp_data.loc[max_index]['open_interest']
                last_record = pd.Series({'datetime': next_time,
                                         'open': open,
                                         'close': close,
                                         'high': high,
                                         'low': low,
                                         'volume': volume,
                                         'interest': interest})
            if last_record is not None:
                if len(organized_data) == 0:
                    cur_index = 1
                else:
                    cur_index = organized_data.index.max() + 1
                delta_data = pd.DataFrame([last_record], [cur_index])
                organized_data = pd.concat([organized_data, delta_data])
            cur_time = next_time
        return organized_data


class FutureTickDataProcessorPhase2(DataProcessor):
    """计算ret 1, 2, 5, 10, 20, 30

    Parameters
    ----------
    data : DataFrame
        待处理数据.
    """

    #收益计算周期
    _ret_period = RET_PERIOD

    @timing
    def process(self, data):
        for period in self._ret_period:
            # 收益率从下一个tick开始计算
            data['ret.' + str(period)] = (data['close'].shift(-(period * 20)-1) - data['open'].shift(-1))/data['open'].shift(-1)
        closing_price = data.iloc[-1]['close']
        # 这里需要区分跨天和不跨天的计算
        for period in self._ret_period:
            data.loc[np.isnan(data['ret.' + str(period)]), 'ret.' + str(period)] = (closing_price - data['open'].shift(-1))/data['open'].shift(-1)
        # 处理最后一条记录
        for period in self._ret_period:
            data.loc[np.isnan(data['ret.' + str(period)]), 'ret.' + str(period)] = 0
        return data

class IndexAbstactExtractor(DataProcessor):
    """提取股指成分股摘要：

    Parameters
    ----------
    data : Dict
        待处理数据.
        time -> list[]

    Returns : Dict
    -------
        starttime - endtime -> list[]

    """

    @timing
    def process(self, data):
        start_time = ''
        end_time = ''
        stock_list = ''
        index_abstract = {}
        for time in list(data.keys()):
            cur_stock_list = '|'.join(sorted(data[time])) #转成字符串来比较
            #初始化
            if (start_time == ''):
                start_time = time.strftime('%Y%m%d')
            if (stock_list == ''): #初始化
                stock_list = cur_stock_list
            elif (stock_list == cur_stock_list): #成分股未发生变化
                end_time = time.strftime('%Y%m%d')
            else: #成分股发生变化
                index_abstract[start_time + '_' + end_time] = [stock.split('.')[0] for stock in stock_list.split('|')]
                start_time = time.strftime('%Y%m%d')
                stock_list = cur_stock_list
        index_abstract[start_time + '_' + end_time] = [stock.split('.')[0] for stock in stock_list.split('|')]
        return index_abstract

    @timing
    def append(self, data, list, start_date, end_date):
        data[start_date + '_' + end_date] = list
        return data

class IndexConstituentStocksInfo():

    def __init__(self):
        self._stocks_abstract_50 = pd.read_pickle(CONFIG_PATH + os.path.sep + '50_stocks_abstract.pkl')
        self._stocks_abstract_300 = pd.read_pickle(CONFIG_PATH + os.path.sep + '300_stocks_abstract.pkl')
        self._stocks_abstract_500 = pd.read_pickle(CONFIG_PATH + os.path.sep + '500_stocks_abstract.pkl')
        self._stocks_map = {
            'IC' : self._stocks_abstract_500,
            'IH' : self._stocks_abstract_50,
            'IF' : self._stocks_abstract_300
        }

    def get_constituent_info(self, product):
        return self._stocks_map[product]

    def get_all_constituent_stocks(self, product):
        """
        获取所有成分股，包括已经去除的
        Parameters
        ----------
        product

        Returns
        -------

        """
        all_stocks = []
        for values in self._stocks_map[product].values():
            all_stocks = all_stocks + values
        results = list(set(all_stocks))
        results.sort()
        return results


if __name__ == '__main__':
    # 期货tick数据测试
    # product = 'IC'
    # instrument = 'IC1701'
    # content = pd.read_csv(constants.FUTURE_TICK_DATA_PATH + product + '/' + FutureTickerHandler().build(instrument))
    # content = FutureTickDataColumnTransform(product, instrument).process(content)
    # content = DataCleaner().process(content)
    # content['date'] = content['datetime'].apply(lambda item: item[0:10])
    # print(len(content.drop_duplicates(subset='date')))
    # content = content[
    #     content['datetime'].str.contains('2016-11-21') | content['datetime'].str.contains('2016-11-22') | content[
    #         'datetime'].str.contains('2016-11-23')]
    # print(len(content))
    # # content.to_csv('/Users/finley/Projects/stock-index-future/data/temp/IC1701.csv')
    # content = FutureTickDataEnricher().process(content)
    # print(len(content))
    # content.to_csv('/Users/finley/Projects/stock-index-future/data/temp/IC1701_enriched.csv')

    # 期货tick处理测试
    product = 'IH'
    instrument = 'IH2208'
    content = pd.read_csv(constants.FUTURE_TICK_DATA_PATH + product + os.path.sep + FutureTickerHandler().build(instrument))
    content = FutureTickDataColumnTransform(product, instrument).process(content)
    content = DataCleaner().process(content)
    content['date'] = content['datetime'].str[0:10]
    date_list = sorted(list(set(content['date'].tolist())))
    date = '2022-06-20'
    # date = date_list[0]
    print(date)
    data = content[content['date'] == date]
    print(FutureTickDataProcessorPhase1().process(data)[['datetime','open','close','high','low','volume','interest']].to_csv('E:\\data\\temp\\IH2208_2022-06-20.csv'))

    # 期货tick处理phase2测试
    # product = 'IF'
    # instrument = 'IF2209'
    # content = read_decompress(constants.FUTURE_TICK_TEMP_DATA_PATH + product + os.path.sep + instrument + os.path.sep + '20220124.pkl')
    # print(FutureTickDataProcessorPhase2().process(content)[['close','ret.30']])

    # 股票tick数据测试
    # From CSV
    # tscode = 'sh688800'
    # content = pd.read_csv(constants.STOCK_TICK_DATA_PATH.format('20220812') + StockTickerHandler('20220812').build(tscode), encoding='gbk')
    # # From pkl
    # content = read_decompress('D:\\liuli\\data\\original\\stock\\tick\\stk_tick10_w_2021\\stk_tick10_w_202107\\20210719\\601998.pkl')
    # content = StockTickDataColumnTransform().process(content)
    # content = StockTickDataCleaner().process(content)
    # content.to_csv("E:\\data\\temp\\601998_org.csv")
    # content = StockTickDataEnricher().process(content)
    # content.to_csv("E:\\data\\temp\\601998_new.csv")


    # 测试期指摘要处理类
    # data = pd.read_pickle('D:/liuli/workspace/quantitative/data/config/50_stocks.pkl')
    # data = IndexAbstactExtractor().process(data)
    # print(data)
    # pd.to_pickle(data, 'D:/liuli/workspace/quantitative/data/config/50_stocks_abstract.pkl')
    # data = pd.read_pickle('D:/liuli/workspace/quantitative/data/config/300_stocks.pkl')
    # data = IndexAbstactExtractor().process(data)
    # print(data)
    # pd.to_pickle(data, 'D:/liuli/workspace/quantitative/data/config/300_stocks_abstract.pkl')
    # data = pd.read_pickle('D:/liuli/workspace/quantitative/data/config/500_stocks.pkl')
    # print(data)
    # data = IndexAbstactExtractor().process(data)
    # print(data)
    # pd.to_pickle(data, 'D:/liuli/workspace/quantitative/data/config/500_stocks_abstract.pkl')
    # data50 = pd.read_pickle('/Users/finley/Projects/stock-index-future/data/config/50_stocks_abstract.pkl')
    # list50 = data50['20170103_20170609']
    # print(list50)
    # print(len(list50))
    # data300 = pd.read_pickle('/Users/finley/Projects/stock-index-future/data/config/300_stocks_abstract.pkl')
    # list300 = data300['20170103_20170213']
    # print(list300)
    # print(len(list300))
    # data500 = pd.read_pickle('/Users/finley/Projects/stock-index-future/data/config/500_stocks_abstract.pkl')
    # list500 = data500['20170103_20170117']
    # print(list500)
    # print(len(list500))
    # print([item for item in list50 if item in list300])
    # print(len([item for item in list50 if item in list300]))
    # print([item for item in list300 if item in list500])
    # print(len([item for item in list300 if item in list500]))
    # print(len([item for item in list500 if item in list50]))
    # 期货摘要追加
    # start_date = '20220613'
    # end_date = '20221231'
    # data50 = pd.read_pickle('/Users/finley/Projects/stock-index-future/data/config/50_stocks_abstract.pkl')
    # file = open('/Users/finley/Projects/stock-index-future/data/config/sz50')
    # data = file.read()
    # IndexAbstactExtractor().append(data50, data.split('\n'), start_date, end_date)
    # print(data50)
    # pd.to_pickle(data50, '/Users/finley/Projects/stock-index-future/data/config/50_stocks_abstract.pkl')
    # data300 = pd.read_pickle('/Users/finley/Projects/stock-index-future/data/config/300_stocks_abstract.pkl')
    # file = open('/Users/finley/Projects/stock-index-future/data/config/hs300')
    # data = file.read()
    # IndexAbstactExtractor().append(data300, data.split('\n'), start_date, end_date)
    # print(data300)
    # pd.to_pickle(data300, '/Users/finley/Projects/stock-index-future/data/config/300_stocks_abstract.pkl')
    # data500 = pd.read_pickle('/Users/finley/Projects/stock-index-future/data/config/500_stocks_abstract.pkl')
    # file = open('/Users/finley/Projects/stock-index-future/data/config/zz500')
    # data = file.read()
    # IndexAbstactExtractor().append(data500, data.split('\n'), start_date, end_date)
    # print(data500)
    # pd.to_pickle(data500, '/Users/finley/Projects/stock-index-future/data/config/500_stocks_abstract.pkl')

    # print(IndexConstituentStocksInfo().get_constituent_info("IH"))
    # print(len(IndexConstituentStocksInfo().get_all_constituent_stocks("IH")))
    # print(IndexConstituentStocksInfo().get_all_constituent_stocks("IH"))
