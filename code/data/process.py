#! /usr/bin/env python
# -*- coding:utf8 -*-
import math
import os
import sys
from abc import abstractmethod, ABCMeta
from datetime import datetime, timedelta
import pandas as pd

from common import constants
from common.aop import timing
from common.io import read_decompress
from data.analysis import FutureTickerHandler, StockTickerHandler

parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0,parentdir)
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

class StockDataCleaner(DataCleaner):
    """股票数据清洗：
    删除不需要的列
    删除9：15之前的数据

    Parameters
    ----------
    data : DataFrame
        待处理数据.

    """

    def __init__(self):
        self._ignore_columns = ['iopv','total_varieties','total_increase_varieties','total_falling_varieties','total_equal_varieties']

    def process(self, data):
        super().process(data)
        data = data.drop(columns=self._ignore_columns)
        data = data.drop(data.index[data['time'] < constants.TRANSACTION_START_TIME])
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
        self._columns = ['tscode','exchange_tscode','date','time','price','volume','amount','transaction_number','iopv','transaction_flag','bs_flag','daily_accumulated_volume','daily_amount','high','low','open','close','bid_price1','bid_price2','bid_price3','bid_price4','bid_price5','bid_price6','bid_price7','bid_price8','bid_price9','bid_price10','bid_volume1','bid_volume2','bid_volume3','bid_volume4','bid_volume5','bid_volume6','bid_volume7','bid_volume8','bid_volume9','bid_volume10','ask_price1','ask_price2','ask_price3','ask_price4','ask_price5','ask_price6','ask_price7','ask_price8','ask_price9','ask_price10','ask_volume1','ask_volume2','ask_volume3','ask_volume4','ask_volume5','ask_volume6','ask_volume7','ask_volume8','ask_volume9','ask_volume10','weighted_average_bid_price','weighted_average_ask_price','total_bid_volume','total_ask_volume','unwighted_index','total_varieties','total_increase_varieties','total_falling_varieties','total_equal_varieties']

    def process(self, data):
        data.columns = self._columns
        return data

class StockTickDataColumnTransform(DataProcessor):
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
        new_column_list = [column.replace(constants.TICK_FILE_PREFIX + '.' + self._instrument + '.', '') for column in column_list]
        column_map = dict(map(lambda x, y: [x, y], column_list, new_column_list))
        data = data.rename(columns=column_map)
        return data

class TickDataEnrich(DataProcessor):
    """Tick填充：

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
        # data = data.iloc[1:20]
        miss_data = pd.DataFrame(columns=data.columns.tolist())
        dt = data['datetime'].to_frame('str_datetime')
        dt['datetime'] = dt.apply(lambda dt: datetime.strptime(dt['str_datetime'][0:21], "%Y-%m-%d %H:%M:%S.%f"), axis = 1)
        dt['delta_time'] = dt.shift(-1)['datetime'] - dt['datetime']
        dt['delta_time_sec'] = dt.apply(lambda dt: dt['delta_time'].total_seconds(), axis = 1)
        dt = dt[dt['delta_time_sec'] > constants.TICK_SAMPLE_INTERVAL]
        for index, row_data in dt.iterrows():
            delta_time_sec = row_data[2].total_seconds()
            step = constants.TICK_SAMPLE_INTERVAL
            item = data.loc[index]
            str_cur_time = item['datetime']
            while step < delta_time_sec:
                item['datetime'] = self.time_advance(str_cur_time, step)
                miss_data = miss_data.append(item)
                step = step + constants.TICK_SAMPLE_INTERVAL
        data = data.append(miss_data)
        data = data.sort_values(by=['datetime'])
        data = data.reset_index(drop=True)
        return data

    def time_advance(self, str_cur_time, step):
        cur_time = datetime.strptime(str_cur_time[0:21], "%Y-%m-%d %H:%M:%S.%f")
        next_time = cur_time + timedelta(seconds=step)
        return datetime.strftime(next_time, "%Y-%m-%d %H:%M:%S.%f000")


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
            cur_stock_list = ''.join(sorted(data[time]))
            if (start_time == ''):
                start_time = time.strftime('%Y%m%d')
            if (stock_list == ''):
                stock_list = cur_stock_list
            elif (stock_list == cur_stock_list):
                end_date = time.strftime('%Y%m%d')
            else:
                index_abstract[start_time + '_' + end_date] = [stock.split('.')[0] for stock in data[time]]
                start_time = time.strftime('%Y%m%d')
                stock_list = cur_stock_list
        return index_abstract

    @timing
    def append(self, data, list, start_date, end_date):
        data[start_date + '_' + end_date] = list
        return data




if __name__ == '__main__':
    #期货tick数据测试
    # product = 'IF'
    # instrument = 'IF2212'
    # content = pd.read_csv(constants.FUTURE_TICK_DATA_PATH + product + FutureTickerHandler().build(instrument))
    # content = TickDataColumnTransform(product, instrument).process(content)
    # content = DataCleaner().process(content)
    # TickDataEnrich().process(content)
    #股票tick数据测试
    # From CSV
    # tscode = 'sh688800'
    # content = pd.read_csv(constants.STOCK_TICK_DATA_PATH.format('20220812') + StockTickerHandler('20220812').build(tscode), encoding='gbk')
    # From pkl
    content = read_decompress('/Users/finley/Projects/stock-index-future/data/000001.pkl')
    content = StockTickDataColumnTransform().process(content)
    content = StockDataCleaner().process(content)
    print(content)
    #测试期指摘要处理类
    # data = pd.read_pickle('D:/liuli/workspace/quantitative/data/config/50_stocks.pkl')
    # data = IndexAbstactExtractor().process(data)
    # print(data)
    # pd.to_pickle(data, 'D:/liuli/workspace/quantitative/data/config/50_stocks_abstract.pkl')
    # data = pd.read_pickle('/Users/finley/Projects/stock-index-future/data/config/300_stocks.pkl')
    # data = IndexAbstactExtractor().process(data)
    # print(data)
    # pd.to_pickle(data, '/Users/finley/Projects/stock-index-future/data/config/300_stocks_abstract.pkl')
    # data = pd.read_pickle('/Users/finley/Projects/stock-index-future/data/config/500_stocks.pkl')
    # print(data)
    # data = IndexAbstactExtractor().process(data)
    # print(data)
    # pd.to_pickle(data, '/Users/finley/Projects/stock-index-future/data/config/500_stocks_abstract.pkl')
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

