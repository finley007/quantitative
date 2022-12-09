#! /usr/bin/env python
# -*- coding:utf8 -*-
import pandas as pd

from factor.base_factor import Factor, StockTickFactor
from common.constants import TEST_PATH, STOCK_TRANSACTION_START_TIME, STOCK_OPEN_CALL_AUACTION_2ND_STAGE_START_TIME, STOCK_OPEN_CALL_AUACTION_2ND_STAGE_END_TIME, STOCK_OPEN_CALL_AUACTION_1ST_STAGE_START_TIME
from common.localio import read_decompress
from common.aop import timing
from common.visualization import draw_analysis_curve

"""现货类因子
分类编号：02
"""
class TotalCommissionRatioFactor(StockTickFactor):
    """总委比因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_001_TOTAL_COMMISSION_RATIO'
    version = '1.0'

    def __init__(self):
        StockTickFactor.__init__(self)

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['weighted_average_bid_price', 'weighted_average_ask_price', 'total_bid_volume', 'total_ask_volume']
        return columns

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    @timing
    def caculate(self, data):
        columns = data.columns.tolist() + [self.get_key(), 'time', 'second_remainder']
        new_data = pd.DataFrame(columns = columns)
        product = data.iloc[0]['product']
        date_list = list(set(data['date'].tolist()))
        date_list.sort()
        for date in date_list:
            stock_data_per_date = self.get_stock_tick_data(product, date)
            stock_data_per_date = stock_data_per_date[stock_data_per_date['time'] > STOCK_TRANSACTION_START_TIME]
            stock_data_per_date['ask_commission_amount'] = stock_data_per_date['total_ask_volume'] * stock_data_per_date['weighted_average_ask_price']
            stock_data_per_date['bid_commission_amount'] = stock_data_per_date['total_bid_volume'] * stock_data_per_date['weighted_average_bid_price']
            stock_data_per_date['total_commission_amount'] = stock_data_per_date['ask_commission_amount'] + stock_data_per_date['bid_commission_amount']
            stock_data_per_date[self.get_key()] = stock_data_per_date.apply(lambda x: 0 if x['total_commission_amount'] == 0 else x['ask_commission_amount']/x['total_commission_amount'], axis=1)
            stock_data_per_date = stock_data_per_date.rename(columns={'time':'cur_time'})
            stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')[self.get_key()].mean()
            df_stock_data_per_date = pd.DataFrame({self.get_key() : stock_data_per_date_group_by, 'time' : stock_data_per_date_group_by.index})
            #过滤对齐在3秒线的数据
            cur_date_data = self.merge_with_stock_data(data, date, df_stock_data_per_date)
            new_data = pd.concat([new_data, cur_date_data])
        return new_data


class TenGradeCommissionRatioFactor(StockTickFactor):
    """10档委比因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_002_10_GRADE_COMMISSION_RATIO'
    version = '1.0'

    def __init__(self):
        StockTickFactor.__init__(self)

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['bid_price1', 'bid_volume1', 'bid_price2', 'bid_volume2','bid_price3', 'bid_volume3','bid_price4', 'bid_volume4','bid_price5', 'bid_volume5','bid_price6', 'bid_volume6','bid_price7', 'bid_volume7','bid_price8', 'bid_volume8','bid_price9', 'bid_volume9','bid_price10', 'bid_volume10',
                             'ask_price1', 'ask_volume1', 'ask_price2', 'ask_volume2', 'ask_price3', 'ask_volume3','ask_price4', 'ask_volume4','ask_price5', 'ask_volume5','ask_price6', 'ask_volume6','ask_price7', 'ask_volume7','ask_price8', 'ask_volume8','ask_price9', 'ask_volume9','ask_price10', 'ask_volume10']
        return columns

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    @timing
    def caculate(self, data):
        columns = data.columns.tolist() + [self.get_key(), 'time', 'second_remainder']
        new_data = pd.DataFrame(columns = columns)
        product = data.iloc[0]['product']
        date_list = list(set(data['date'].tolist()))
        date_list.sort()
        for date in date_list:
            stock_data_per_date = self.get_stock_tick_data(product, date)
            stock_data_per_date = stock_data_per_date[stock_data_per_date['time'] > STOCK_TRANSACTION_START_TIME]
            stock_data_per_date['10_grade_ask_commission_amount'] = stock_data_per_date.apply(lambda item: self.amount_sum(item, 'ask'), axis=1)
            stock_data_per_date['10_grade_bid_commission_amount'] = stock_data_per_date.apply(lambda item: self.amount_sum(item, 'bid'), axis=1)
            stock_data_per_date['10_grade_total_commission_amount'] = stock_data_per_date['10_grade_ask_commission_amount'] + stock_data_per_date['10_grade_bid_commission_amount']
            stock_data_per_date[self.get_key()] = stock_data_per_date.apply(lambda x: 0 if x['10_grade_total_commission_amount'] == 0 else x['10_grade_ask_commission_amount']/x['10_grade_total_commission_amount'], axis=1)
            stock_data_per_date = stock_data_per_date.rename(columns={'time':'cur_time'})
            stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')[self.get_key()].mean()
            df_stock_data_per_date = pd.DataFrame({self.get_key() : stock_data_per_date_group_by, 'time' : stock_data_per_date_group_by.index})
            # #过滤对齐在3秒线的数据
            cur_date_data = self.merge_with_stock_data(data, date, df_stock_data_per_date)
            new_data = pd.concat([new_data, cur_date_data])
        return new_data

    def amount_sum(self, item, type):
        sum = 0
        for i in range(1, 11):
            sum = sum + ((item[type + '_price' + str(i)]) * (item[type + '_volume' + str(i)]))
        return sum


class TenGradeWeightedCommissionRatioFactor(TenGradeCommissionRatioFactor):
    """10档加权委比因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_003_10_GRADE_WEIGHTED_COMMISSION_RATIO'
    version = '1.0'

    def __init__(self):
        StockTickFactor.__init__(self)

    def amount_sum(self, item, type):
        weighted_list = [10 ,9 , 8 , 7 , 6 , 5, 4, 3 ,2 ,1]
        sum = 0
        for i in range(1, 11):
            sum = sum + ((item[type + '_price' + str(i)]) * (item[type + '_volume' + str(i)]) * weighted_list[i - 1])
        return sum

class FiveGradeCommissionRatioFactor(StockTickFactor):
    """5档委比因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_004_5_GRADE_COMMISSION_RATIO'
    version = '1.0'

    def __init__(self):
        StockTickFactor.__init__(self)

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['bid_price1', 'bid_volume1', 'bid_price2', 'bid_volume2','bid_price3', 'bid_volume3','bid_price4', 'bid_volume4','bid_price5', 'bid_volume5',
                             'ask_price1', 'ask_volume1', 'ask_price2', 'ask_volume2', 'ask_price3', 'ask_volume3','ask_price4', 'ask_volume4','ask_price5', 'ask_volume5']
        return columns

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    @timing
    def caculate(self, data):
        columns = data.columns.tolist() + [self.get_key(), 'time', 'second_remainder']
        new_data = pd.DataFrame(columns = columns)
        product = data.iloc[0]['product']
        date_list = list(set(data['date'].tolist()))
        date_list.sort()
        for date in date_list:
            stock_data_per_date = self.get_stock_tick_data(product, date)
            stock_data_per_date = stock_data_per_date[stock_data_per_date['time'] > STOCK_TRANSACTION_START_TIME]
            stock_data_per_date['5_grade_ask_commission_amount'] = stock_data_per_date.apply(lambda item: self.amount_sum(item, 'ask'), axis=1)
            stock_data_per_date['5_grade_bid_commission_amount'] = stock_data_per_date.apply(lambda item: self.amount_sum(item, 'bid'), axis=1)
            stock_data_per_date['5_grade_total_commission_amount'] = stock_data_per_date['5_grade_ask_commission_amount'] + stock_data_per_date['5_grade_bid_commission_amount']
            stock_data_per_date[self.get_key()] = stock_data_per_date.apply(lambda x: 0 if x['5_grade_total_commission_amount'] == 0 else x['5_grade_ask_commission_amount']/x['5_grade_total_commission_amount'], axis=1)
            stock_data_per_date = stock_data_per_date.rename(columns={'time':'cur_time'})
            stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')[self.get_key()].mean()
            df_stock_data_per_date = pd.DataFrame({self.get_key() : stock_data_per_date_group_by, 'time' : stock_data_per_date_group_by.index})
            # #过滤对齐在3秒线的数据
            cur_date_data = self.merge_with_stock_data(data, date, df_stock_data_per_date)
            new_data = pd.concat([new_data, cur_date_data])
        return new_data

    def amount_sum(self, item, type):
        sum = 0
        for i in range(1, 6):
            sum = sum + ((item[type + '_price' + str(i)]) * (item[type + '_volume' + str(i)]))
        return sum

class FiveGradeWeightedCommissionRatioFactor(FiveGradeCommissionRatioFactor):
    """5档加权委比因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_005_5_GRADE_WEIGHTED_COMMISSION_RATIO'
    version = '1.0'

    def __init__(self):
        StockTickFactor.__init__(self)

    def amount_sum(self, item, type):
        weighted_list = [5, 4, 3 ,2 ,1]
        sum = 0
        for i in range(1, 6):
            sum = sum + ((item[type + '_price' + str(i)]) * (item[type + '_volume' + str(i)]) * weighted_list[i - 1])
        return sum


class RisingStockRatioFactor(StockTickFactor):
    """上涨股票比例因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_006_RISING_STOCK_RATIO'
    version = '1.0'

    def __init__(self, params=[20, 50 ,100 ,200]):
        StockTickFactor.__init__(self)
        self._params = params

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['price']
        return columns

    @timing
    def caculate(self, data):
        key_columns = []
        for param in self._params:
            key_columns = key_columns + [self.get_key(param)]
        print(key_columns)
        columns = data.columns.tolist() + key_columns + ['time', 'second_remainder']
        new_data = pd.DataFrame(columns=columns)
        product = data.iloc[0]['product']
        date_list = list(set(data['date'].tolist()))
        date_list.sort()
        for date in date_list:
            stock_data_per_date = self.get_stock_tick_data(product, date)
            stock_data_per_date = stock_data_per_date[stock_data_per_date['time'] > STOCK_TRANSACTION_START_TIME]
            change_columns = []
            columns_replace = {}
            for param in self._params:
                change_columns = change_columns + ['change.' + str(param)]
                columns_replace['change.' + str(param)] = self.get_key(param)
            stock_data_per_date = stock_data_per_date.rename(columns={'time': 'cur_time'})
            stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')[change_columns].apply(lambda item: self.get_rise_ratio(item))
            stock_data_per_date_group_by['time'] = stock_data_per_date_group_by.index
            # 替换列名
            stock_data_per_date_group_by = stock_data_per_date_group_by.rename(columns = columns_replace)
            print(stock_data_per_date_group_by)
            # #过滤对齐在3秒线的数据
            cur_date_data = self.merge_with_stock_data(data, date, stock_data_per_date_group_by)
            new_data = pd.concat([new_data, cur_date_data])
        return new_data

    def enrich_stock_data(self, data):
        for param in self._params:
            data['change.' + str(param)] = data['price'] - data[
                'price'].shift(-param)
        return data

    def get_rise_ratio(self, item):
        return item[item > 0].count()/item.count()

class SpreadFactor(StockTickFactor):
    """价差因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_007_SPREAD'
    version = '1.0'

    def __init__(self):
        StockTickFactor.__init__(self)

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['bid_price1', 'ask_price1']
        return columns

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    @timing
    def caculate(self, data):
        columns = data.columns.tolist() + [self.get_key(), 'time', 'second_remainder']
        new_data = pd.DataFrame(columns = columns)
        product = data.iloc[0]['product']
        date_list = list(set(data['date'].tolist()))
        date_list.sort()
        for date in date_list:
            stock_data_per_date = self.get_stock_tick_data(product, date)
            stock_data_per_date = stock_data_per_date[stock_data_per_date['time'] > STOCK_TRANSACTION_START_TIME]
            stock_data_per_date[self.get_key()] = (stock_data_per_date['ask_price1'] - stock_data_per_date['bid_price1'])/(stock_data_per_date['bid_price1'] + stock_data_per_date['ask_price1'])
            stock_data_per_date = stock_data_per_date.rename(columns={'time':'cur_time'})
            stock_data_per_date_group_by = stock_data_per_date.groupby('cur_time')[self.get_key()].mean()
            df_stock_data_per_date = pd.DataFrame({self.get_key() : stock_data_per_date_group_by, 'time' : stock_data_per_date_group_by.index})
            #过滤对齐在3秒线的数据
            cur_date_data = self.merge_with_stock_data(data, date, df_stock_data_per_date)
            new_data = pd.concat([new_data, cur_date_data])
        return new_data

class CallAuctionSecondStageIncreaseFactor(StockTickFactor):
    """集合竞价二阶段涨幅因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_008_CALL_AUCTION_SECOND_STAGE_INCREASE'
    version = '1.0'

    def __init__(self):
        StockTickFactor.__init__(self)

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['bid_price1', 'ask_price1']
        return columns

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    @timing
    def caculate(self, data):
        columns = data.columns.tolist() + [self.get_key()]
        new_data = pd.DataFrame(columns=columns)
        product = data.iloc[0]['product']
        date_list = list(set(data['date'].tolist()))
        date_list.sort()
        for date in date_list:
            stock_data_per_date = self.get_stock_tick_data(product, date)
            stock_data_per_date = stock_data_per_date[(stock_data_per_date['time'] >= STOCK_OPEN_CALL_AUACTION_2ND_STAGE_START_TIME) & (stock_data_per_date['time'] <= STOCK_OPEN_CALL_AUACTION_2ND_STAGE_END_TIME)]
            start_price = stock_data_per_date.iloc[0]['ask_price1']
            end_price = stock_data_per_date.iloc[-1]['ask_price1']
            increase_rate = (end_price - start_price)/start_price
            data.loc[data['date'] == date, self.get_key()] = increase_rate
            new_data = pd.concat([new_data, data])
        return new_data


class TwoCallAuctionStageDifferenceFactor(StockTickFactor):
    """集合竞价二阶段涨幅减去集合竞价一阶段涨幅因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_009_TWO_CALL_AUCTION_STAGE_DIFFERENCE'
    version = '1.0'

    def __init__(self):
        StockTickFactor.__init__(self)

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['bid_price1', 'ask_price1']
        return columns

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    @timing
    def caculate(self, data):
        columns = data.columns.tolist() + [self.get_key()]
        new_data = pd.DataFrame(columns=columns)
        product = data.iloc[0]['product']
        date_list = list(set(data['date'].tolist()))
        date_list.sort()
        for date in date_list:
            stock_data_per_date = self.get_stock_tick_data(product, date)
            stage_one_data = stock_data_per_date[(stock_data_per_date['time'] >= STOCK_OPEN_CALL_AUACTION_1ST_STAGE_START_TIME) & (stock_data_per_date['time'] <= STOCK_OPEN_CALL_AUACTION_2ND_STAGE_START_TIME)]
            stage_two_data = stock_data_per_date[(stock_data_per_date['time'] >= STOCK_OPEN_CALL_AUACTION_2ND_STAGE_START_TIME) & (stock_data_per_date['time'] <= STOCK_OPEN_CALL_AUACTION_2ND_STAGE_END_TIME)]
            stage_one_increase_rate = (stage_one_data.iloc[-1]['ask_price1'] - stage_one_data.iloc[0]['ask_price1'])/stage_one_data.iloc[0]['ask_price1']
            stage_two_increase_rate = (stage_two_data.iloc[-1]['ask_price1'] - stage_two_data.iloc[0]['ask_price1'])/stage_two_data.iloc[0]['ask_price1']
            data.loc[data['date'] == date, self.get_key()] = stage_one_increase_rate - stage_two_increase_rate
            new_data = pd.concat([new_data, data])
        return new_data


class CallAuctionSecondStageReturnVolatilityFactor(StockTickFactor):
    """集合竞价二阶段收益率波动率因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_010_CALL_AUCTION_SECOND_STAGE_RETURN_VOLATILITY'
    version = '1.0'

    def __init__(self):
        StockTickFactor.__init__(self)

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['bid_price1', 'ask_price1']
        return columns

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    @timing
    def caculate(self, data):
        columns = data.columns.tolist() + [self.get_key()]
        new_data = pd.DataFrame(columns=columns)
        product = data.iloc[0]['product']
        date_list = list(set(data['date'].tolist()))
        date_list.sort()
        for date in date_list:
            stock_data_per_date = self.get_stock_tick_data(product, date)
            stock_data_per_date = stock_data_per_date[(stock_data_per_date['time'] >= STOCK_OPEN_CALL_AUACTION_2ND_STAGE_START_TIME) & (stock_data_per_date['time'] <= STOCK_OPEN_CALL_AUACTION_2ND_STAGE_END_TIME)]
            stock_data_per_date['mean_price'] = (stock_data_per_date['ask_price1'] + stock_data_per_date['bid_price1'])/2
            stock_data_per_date['mean_price_return'] = (stock_data_per_date['mean_price'].shift(1) - stock_data_per_date['mean_price'])/stock_data_per_date['mean_price']
            return_volatility =  stock_data_per_date['mean_price_return'].std()
            data.loc[data['date'] == date, self.get_key()] = return_volatility
            new_data = pd.concat([new_data, data])
        return new_data


class FirstStageCommissionRatioFactor(StockTickFactor):
    """集合竞价一阶段委比因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_002_11_FIRST_STAGE_COMMISSION_RATIO'
    version = '1.0'

    def __init__(self):
        StockTickFactor.__init__(self)

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['bid_price1', 'bid_volume1', 'bid_price2', 'bid_volume2','bid_price3', 'bid_volume3','bid_price4', 'bid_volume4','bid_price5', 'bid_volume5','bid_price6', 'bid_volume6','bid_price7', 'bid_volume7','bid_price8', 'bid_volume8','bid_price9', 'bid_volume9','bid_price10', 'bid_volume10',
                             'ask_price1', 'ask_volume1', 'ask_price2', 'ask_volume2', 'ask_price3', 'ask_volume3','ask_price4', 'ask_volume4','ask_price5', 'ask_volume5','ask_price6', 'ask_volume6','ask_price7', 'ask_volume7','ask_price8', 'ask_volume8','ask_price9', 'ask_volume9','ask_price10', 'ask_volume10']
        return columns

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    @timing
    def caculate(self, data):
        columns = data.columns.tolist() + [self.get_key()]
        new_data = pd.DataFrame(columns = columns)
        product = data.iloc[0]['product']
        date_list = list(set(data['date'].tolist()))
        date_list.sort()
        for date in date_list:
            stock_data_per_date = self.get_stock_tick_data(product, date)
            stock_data_per_date = stock_data_per_date[(stock_data_per_date['time'] >= STOCK_OPEN_CALL_AUACTION_1ST_STAGE_START_TIME) & (stock_data_per_date['time'] <= STOCK_OPEN_CALL_AUACTION_2ND_STAGE_START_TIME)]
            stock_data_per_date['ask_commission_amount'] = stock_data_per_date.apply(lambda item: self.amount_sum(item, 'ask'), axis=1)
            stock_data_per_date['bid_commission_amount'] = stock_data_per_date.apply(lambda item: self.amount_sum(item, 'bid'), axis=1)
            total_ask_commission_amount = stock_data_per_date['ask_commission_amount'].sum()
            total_bid_commission_amount = stock_data_per_date['bid_commission_amount'].sum()
            data.loc[data['date'] == date, self.get_key()] =  total_ask_commission_amount/(total_ask_commission_amount + total_bid_commission_amount)
            new_data = pd.concat([new_data, data])
        return new_data

    def amount_sum(self, item, type):
        sum = 0
        for i in range(1, 11):
            sum = sum + ((item[type + '_price' + str(i)]) * (item[type + '_volume' + str(i)]))
        return sum


class SecondStageCommissionRatioFactor(StockTickFactor):
    """集合竞价二阶段委比因子

    Parameters
    ----------
    factor_code : string
    version : string
    _params ： list
    """
    factor_code = 'FCT_02_002_12_SECOND_STAGE_COMMISSION_RATIO'
    version = '1.0'

    def __init__(self):
        StockTickFactor.__init__(self)

    def get_columns(self):
        columns = StockTickFactor.get_columns(self)
        columns = columns + ['bid_price1', 'bid_volume1', 'bid_price2', 'bid_volume2','bid_price3', 'bid_volume3','bid_price4', 'bid_volume4','bid_price5', 'bid_volume5','bid_price6', 'bid_volume6','bid_price7', 'bid_volume7','bid_price8', 'bid_volume8','bid_price9', 'bid_volume9','bid_price10', 'bid_volume10',
                             'ask_price1', 'ask_volume1', 'ask_price2', 'ask_volume2', 'ask_price3', 'ask_volume3','ask_price4', 'ask_volume4','ask_price5', 'ask_volume5','ask_price6', 'ask_volume6','ask_price7', 'ask_volume7','ask_price8', 'ask_volume8','ask_price9', 'ask_volume9','ask_price10', 'ask_volume10']
        return columns

    def get_key(self):
        return self.factor_code

    def get_keys(self):
        return [self.get_key()]

    @timing
    def caculate(self, data):
        columns = data.columns.tolist() + [self.get_key()]
        new_data = pd.DataFrame(columns = columns)
        product = data.iloc[0]['product']
        date_list = list(set(data['date'].tolist()))
        date_list.sort()
        for date in date_list:
            stock_data_per_date = self.get_stock_tick_data(product, date)
            stock_data_per_date = stock_data_per_date[(stock_data_per_date['time'] >= STOCK_OPEN_CALL_AUACTION_2ND_STAGE_START_TIME) & (stock_data_per_date['time'] <= STOCK_OPEN_CALL_AUACTION_2ND_STAGE_END_TIME)]
            stock_data_per_date['ask_commission_amount'] = stock_data_per_date.apply(lambda item: self.amount_sum(item, 'ask'), axis=1)
            stock_data_per_date['bid_commission_amount'] = stock_data_per_date.apply(lambda item: self.amount_sum(item, 'bid'), axis=1)
            total_ask_commission_amount = stock_data_per_date['ask_commission_amount'].sum()
            total_bid_commission_amount = stock_data_per_date['bid_commission_amount'].sum()
            data.loc[data['date'] == date, self.get_key()] =  total_ask_commission_amount/(total_ask_commission_amount + total_bid_commission_amount)
            new_data = pd.concat([new_data, data])
        return new_data

    def amount_sum(self, item, type):
        sum = 0
        for i in range(1, 11):
            sum = sum + ((item[type + '_price' + str(i)]) * (item[type + '_volume' + str(i)]))
        return sum

if __name__ == '__main__':
    data = read_decompress(TEST_PATH + '20200928.pkl')
    data['product'] = 'IF'
    data['date'] = data['datetime'].str[0:10]

    print(data)

    #总委比因子
    # total_commision = TotalCommissionRatioFactor()
    # print(total_commision.factor_code)
    # print(total_commision.version)
    # print(total_commision.get_params())
    # print(total_commision.get_category())
    # print(total_commision.get_full_name())
    #
    # data = TotalCommissionRatioFactor().caculate(data)
    # print(data[['datetime', TotalCommissionRatioFactor.factor_code]])
    # data.index = pd.DatetimeIndex(data['datetime'])
    # data = data[(data['datetime'] >= '2020-09-28 10:00:00') & (data['datetime'] <= '2020-09-28 10:30:00')]
    # draw_analysis_curve(data, show_signal=True, signal_keys=total_commision.get_keys())

    # 10档委比因子
    # ten_grade_total_commision = TenGradeCommissionRatioFactor()
    # print(ten_grade_total_commision.factor_code)
    # print(ten_grade_total_commision.version)
    # print(ten_grade_total_commision.get_params())
    # print(ten_grade_total_commision.get_category())
    # print(ten_grade_total_commision.get_full_name())
    #
    # data = TenGradeCommissionRatioFactor().caculate(data)
    # print(data[['datetime', TenGradeCommissionRatioFactor.factor_code]])
    # data.index = pd.DatetimeIndex(data['datetime'])
    # data = data[(data['datetime'] >= '2020-09-28 10:00:00') & (data['datetime'] <= '2020-09-28 10:30:00')]
    # draw_analysis_curve(data, show_signal=True, signal_keys=ten_grade_total_commision.get_keys())

    # 10档加权委比因子
    # ten_grade_weighted_total_commision = TenGradeWeightedCommissionRatioFactor()
    # print(ten_grade_weighted_total_commision.factor_code)
    # print(ten_grade_weighted_total_commision.version)
    # print(ten_grade_weighted_total_commision.get_params())
    # print(ten_grade_weighted_total_commision.get_category())
    # print(ten_grade_weighted_total_commision.get_full_name())
    #
    # data = TenGradeWeightedCommissionRatioFactor().caculate(data)
    # print(data[['datetime', TenGradeWeightedCommissionRatioFactor.factor_code]])
    # data.index = pd.DatetimeIndex(data['datetime'])
    # data = data[(data['datetime'] >= '2020-09-28 10:00:00') & (data['datetime'] <= '2020-09-28 10:30:00')]
    # draw_analysis_curve(data, show_signal=True, signal_keys=ten_grade_weighted_total_commision.get_keys())

    # 5档委比因子
    # five_grade_total_commision = FiveGradeCommissionRatioFactor()
    # print(five_grade_total_commision.factor_code)
    # print(five_grade_total_commision.version)
    # print(five_grade_total_commision.get_params())
    # print(five_grade_total_commision.get_category())
    # print(five_grade_total_commision.get_full_name())
    #
    # data = FiveGradeCommissionRatioFactor().caculate(data)
    # print(data[['datetime', FiveGradeCommissionRatioFactor.factor_code]])
    # data.index = pd.DatetimeIndex(data['datetime'])
    # data = data[(data['datetime'] >= '2020-09-28 10:00:00') & (data['datetime'] <= '2020-09-28 10:30:00')]
    # draw_analysis_curve(data, show_signal=True, signal_keys=five_grade_total_commision.get_keys())

    # 5档加权委比因子
    # five_grade_weighted_total_commision = FiveGradeWeightedCommissionRatioFactor()
    # print(five_grade_weighted_total_commision.factor_code)
    # print(five_grade_weighted_total_commision.version)
    # print(five_grade_weighted_total_commision.get_params())
    # print(five_grade_weighted_total_commision.get_category())
    # print(five_grade_weighted_total_commision.get_full_name())
    #
    # data = FiveGradeWeightedCommissionRatioFactor().caculate(data)
    # print(data[['datetime', FiveGradeWeightedCommissionRatioFactor.factor_code]])
    # data.index = pd.DatetimeIndex(data['datetime'])
    # data = data[(data['datetime'] >= '2020-09-28 10:00:00') & (data['datetime'] <= '2020-09-28 10:30:00')]
    # draw_analysis_curve(data, show_signal=True, signal_keys=five_grade_weighted_total_commision.get_keys())

    # 上涨股票比例因子
    # rising_stock_ratio_factor = RisingStockRatioFactor()
    # print(rising_stock_ratio_factor.factor_code)
    # print(rising_stock_ratio_factor.version)
    # print(rising_stock_ratio_factor.get_params())
    # print(rising_stock_ratio_factor.get_category())
    # print(rising_stock_ratio_factor.get_full_name())
    #
    # data = RisingStockRatioFactor().caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # data = data[(data['datetime'] >= '2020-09-28 10:00:00') & (data['datetime'] <= '2020-09-28 10:30:00')]
    # draw_analysis_curve(data, show_signal=True, signal_keys=rising_stock_ratio_factor.get_keys())

    # 价差因子
    # spread_factor = SpreadFactor()
    # print(spread_factor.factor_code)
    # print(spread_factor.version)
    # print(spread_factor.get_params())
    # print(spread_factor.get_category())
    # print(spread_factor.get_full_name())
    #
    # data = SpreadFactor().caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # data = data[(data['datetime'] >= '2020-09-28 10:00:00') & (data['datetime'] <= '2020-09-28 10:30:00')]
    # draw_analysis_curve(data, show_signal=True, signal_keys=spread_factor.get_keys())

    # 集合竞价二阶段涨幅因子
    # call_auction_second_stage_increase_factor = CallAuctionSecondStageIncreaseFactor()
    # print(call_auction_second_stage_increase_factor.factor_code)
    # print(call_auction_second_stage_increase_factor.version)
    # print(call_auction_second_stage_increase_factor.get_params())
    # print(call_auction_second_stage_increase_factor.get_category())
    # print(call_auction_second_stage_increase_factor.get_full_name())
    #
    # data = CallAuctionSecondStageIncreaseFactor().caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # data = data[(data['datetime'] >= '2020-09-28 10:00:00') & (data['datetime'] <= '2020-09-28 10:30:00')]
    # draw_analysis_curve(data, show_signal=True, signal_keys=call_auction_second_stage_increase_factor.get_keys())

    # 集合竞价二阶段涨幅减去集合竞价一阶段涨幅因子
    # two_call_auction_stage_difference_factor = TwoCallAuctionStageDifferenceFactor()
    # print(two_call_auction_stage_difference_factor.factor_code)
    # print(two_call_auction_stage_difference_factor.version)
    # print(two_call_auction_stage_difference_factor.get_params())
    # print(two_call_auction_stage_difference_factor.get_category())
    # print(two_call_auction_stage_difference_factor.get_full_name())
    #
    # data = TwoCallAuctionStageDifferenceFactor().caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # data = data[(data['datetime'] >= '2020-09-28 10:00:00') & (data['datetime'] <= '2020-09-28 10:30:00')]
    # draw_analysis_curve(data, show_signal=True, signal_keys=two_call_auction_stage_difference_factor.get_keys())

    # 集合竞价二阶段收益率波动率因子
    # call_auction_second_stage_return_volatility_factor = CallAuctionSecondStageReturnVolatilityFactor()
    # print(call_auction_second_stage_return_volatility_factor.factor_code)
    # print(call_auction_second_stage_return_volatility_factor.version)
    # print(call_auction_second_stage_return_volatility_factor.get_params())
    # print(call_auction_second_stage_return_volatility_factor.get_category())
    # print(call_auction_second_stage_return_volatility_factor.get_full_name())
    #
    # data = CallAuctionSecondStageReturnVolatilityFactor().caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # data = data[(data['datetime'] >= '2020-09-28 10:00:00') & (data['datetime'] <= '2020-09-28 10:30:00')]
    # draw_analysis_curve(data, show_signal=True, signal_keys=call_auction_second_stage_return_volatility_factor.get_keys())

    # 集合竞价一阶段委比因子
    # first_stage_comission_ratio_factor = FirstStageCommissionRatioFactor()
    # print(first_stage_comission_ratio_factor.factor_code)
    # print(first_stage_comission_ratio_factor.version)
    # print(first_stage_comission_ratio_factor.get_params())
    # print(first_stage_comission_ratio_factor.get_category())
    # print(first_stage_comission_ratio_factor.get_full_name())
    #
    # data = FirstStageCommissionRatioFactor().caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # data = data[(data['datetime'] >= '2020-09-28 10:00:00') & (data['datetime'] <= '2020-09-28 10:30:00')]
    # draw_analysis_curve(data, show_signal=True,
    #                     signal_keys=first_stage_comission_ratio_factor.get_keys())

    # 集合竞价二阶段委比因子
    second_stage_comission_ratio_factor = SecondStageCommissionRatioFactor()
    print(second_stage_comission_ratio_factor.factor_code)
    print(second_stage_comission_ratio_factor.version)
    print(second_stage_comission_ratio_factor.get_params())
    print(second_stage_comission_ratio_factor.get_category())
    print(second_stage_comission_ratio_factor.get_full_name())

    data = SecondStageCommissionRatioFactor().caculate(data)
    data.index = pd.DatetimeIndex(data['datetime'])
    data = data[(data['datetime'] >= '2020-09-28 10:00:00') & (data['datetime'] <= '2020-09-28 10:30:00')]
    draw_analysis_curve(data, show_signal=True,
                        signal_keys=second_stage_comission_ratio_factor.get_keys())







