#! /usr/bin/env python
# -*- coding:utf8 -*-
import os
import re
import time
import pandas as pd
import random
import uuid

from common.aop import timing
from common.constants import FUTURE_TICK_ORGANIZED_DATA_PATH, CONFIG_PATH, FACTOR_PATH, STOCK_INDEX_PRODUCTS, TEMP_PATH, TEST_PATH
from common.exception.exception import InvalidStatus
from common.localio import read_decompress, list_files_in_path, save_compress
from common.persistence.dbutils import create_session
from common.persistence.po import FutureInstrumentConfig
from factor.volume_price_factor import WilliamFactor
from factor.spot_goods_factor import TotalCommissionRatioFactor, TenGradeCommissionRatioFactor, AmountAndCommissionRatioFactor, FiveGradeCommissionRatioFactor, \
    TenGradeWeightedCommissionRatioFactor, FiveGradeCommissionRatioFactor, RisingFallingAmountRatioFactor, UntradedStockRatioFactor, DailyAccumulatedLargeOrderRatioFactor, \
    RollingAccumulatedLargeOrderRatioFactor, RisingStockRatioFactor, SpreadFactor, OverNightYieldFactor, DeltaTotalCommissionRatioFactor, CallAuctionSecondStageIncreaseFactor,\
    TwoCallAuctionStageDifferenceFactor, CallAuctionSecondStageReturnVolatilityFactor, FirstStageCommissionRatioFactor, SecondStageCommissionRatioFactor, AmountAnd1stGradeCommissionRatioFactor, TotalCommissionRatioDifferenceFactor,\
    AmountAskTotalCommissionRatioFactor, TenGradeCommissionRatioDifferenceFactor, FiveGradeCommissionRatioDifferenceFactor, DailyRisingStockRatioFactor, LargeOrderBidAskVolumeRatioFactor,\
    TenGradeCommissionRatioMeanFactor, FiveGradeCommissionRatioMeanFactor, FiveGradeCommissionRatioStdFactor, FiveGradeCommissionRatioMeanFactor, FiveGradeWeightedCommissionRatioFactor, AuxiliaryFileGenerationFactor,\
    BidLargeAmountBillFactor, AmountBid10GradeCommissionRatioFactor, AskLargeAmountBillFactor, AmountBid10GradeCommissionRatioStdFactor, AmountAsk10GradeCommissionRatioStdFactor, Commission10GradeVolatilityRatioFactor, AmountAndCommissionRatioMeanFactor,\
    AmountAndCommissionRatioStdFactor

from factor.base_factor import StockTickFactor, TimewindowStockTickFactor
from common.log import get_logger
from framework.pagination import Pagination
from framework.localconcurrent import ProcessRunner, ProcessExcecutor
from common.persistence.po import FactorProcessRecord
from data.access import StockDataAccess
from common.timeutils import add_milliseconds_suffix, get_last_or_next_trading_date_by_stock
from common.persistence.dao import FutureInstrumentConfigDao

class FactorCaculator():
    """因子文件生成类
        生成按品种目录的因子文件
    Parameters
    ----------
    """

    @timing
    def caculate(self, process_code, factor_list, include_product_list=[], include_instrument_list=[], performance_test=False, need_resume=True):
        '''
        生成因子文件

        Parameters
        ----------
        factor_list

        Returns
        -------

        '''
        if len(factor_list) == 0:
            raise InvalidStatus('Empty factor list')
        #获取k线文件列模板
        session = create_session()
        if len(include_product_list) == 0:
            include_product_list = STOCK_INDEX_PRODUCTS
        for product in include_product_list:
            temp_file = TEMP_PATH + product + '_' + '_'.join(
                list(map(lambda factor: factor.get_full_name(), factor_list))) + '.temp'
            if need_resume:
                check_handled = session.execute(
                    'select max(instrument) from factor_process_record where process_code = :process_code and product = :product',
                    {'process_code': process_code, 'product': product}).fetchall()
                current_instrument = check_handled[0][0]
            else:
                current_instrument = None
            if current_instrument:
                factor_data = read_decompress(temp_file)
            else:
                factor_data = pd.DataFrame()
            instrument_list = session.execute('select distinct instrument from future_instrument_config where product = :product order by instrument', {'product': product}).fetchall()
            instrument_list = list(filter(lambda instrument : len(include_instrument_list) == 0 or instrument[0] in include_instrument_list, instrument_list))
            instrument_list = list(map(lambda instrument: instrument[0], instrument_list))
            pagination = Pagination(instrument_list, page_size=5)
            skip = False
            while pagination.has_next():
                sub_instrument_list = pagination.next()
                get_logger().info('Start to handle instrument list: {}'.format(sub_instrument_list))
                if current_instrument in sub_instrument_list: #已经处理过的最后一页，下一页开始要处理了
                    skip = True
                    continue
                if current_instrument and current_instrument not in sub_instrument_list and not skip: #已经处理过的页面
                    continue
                params_list = list(map(lambda instrument: [factor_list, instrument, product], sub_instrument_list))
                results = ProcessExcecutor(1).execute(self.caculate_by_instrument, params_list)
                temp_cache = {}
                for result in results:
                    temp_cache[result[0]] = result[1]
                for instrument in sub_instrument_list:
                    factor_data = pd.concat([factor_data, temp_cache[instrument]])
                    factor_process_record = FactorProcessRecord(process_code, product, instrument)
                    session.add(factor_process_record)
                # 每一个分页结束保存临时文件并提交
                get_logger().info('Save temp file for instrument list: {}'.format(sub_instrument_list))
                session.commit()
                if need_resume:
                    save_compress(factor_data, temp_file)
            factor_data = factor_data.reset_index(drop=True)
            if not performance_test:
                target_factor_file = FACTOR_PATH + product + '_' + '_'.join(
                    list(map(lambda factor: factor.get_full_name(), factor_list)))
            else:
                target_factor_file = TEST_PATH + product + '_' + '_'.join(
                    list(map(lambda factor: factor.get_full_name(), factor_list)))
            get_logger().info('Save factor file: {0}'.format(target_factor_file))
            save_compress(factor_data, target_factor_file)
            if os.path.exists(temp_file):
                os.remove(temp_file)

    def caculate_by_instrument(self, *args):
        """
        按合约计算因子值，主要为了并行化

        Parameters
        ----------
        factor_list
        instrument
        product
        session

        Returns
        -------

        """
        factor_list = args[0][0]
        instrument = args[0][1]
        product = args[0][2]
        session = create_session()
        target_instrument_file = FUTURE_TICK_ORGANIZED_DATA_PATH + product + os.path.sep + instrument + '.pkl'
        get_logger().info('Handle instrument: {0} for file: {1}'.format(instrument, target_instrument_file))
        data = read_decompress(target_instrument_file)
        data['date'] = data['datetime'].str[0:10]
        data['product'] = product
        data['instrument'] = instrument
        for factor in factor_list:
            # 这个异常不能捕获，任何一个合约的异常都必须处理，不然最终还要修复
            data = factor.caculate(data)
        # 截取主力合约区间
        date_range = session.execute(
            'select min(date), max(date) from future_instrument_config where product = :product and instrument = :instrument and is_main = 0',
            {'product': product, 'instrument': instrument})
        if date_range.rowcount > 0:
            date_range_query_result = date_range.fetchall()
            start_date = date_range_query_result[0][0]
            end_date = date_range_query_result[0][1]
            data = data[(data['date'] >= start_date) & (data['date'] <= end_date)]
        return instrument, data

    @timing
    def caculate_manually_check(self, factor, stock_count = 2, manually_check_file_count = 1, is_accumulated = False):
        '''
        生成用于检测的因子文件

        Parameters
        ----------
        factor

        Returns
        -------

        '''
        # 获取k线文件列模板
        window_size = 100
        session = create_session()
        future_instrument_config_dao = FutureInstrumentConfigDao()
        product_list = STOCK_INDEX_PRODUCTS
        # 每一个品种生成一个检测文件
        for product in product_list:
            get_logger().info('Handle product {0}'.format(product))
            instrument_list = session.execute('select distinct instrument from future_instrument_config where product = :product order by instrument', {'product': product}).fetchall()
            for i in range(manually_check_file_count):
                rdm_number = random.randint(0, len(instrument_list) - 1)
                # 随机选择一个合约
                instrument = instrument_list[rdm_number][0]
                date_list = session.execute('select date from future_instrument_config where instrument = :instrument and is_main = 0 order by date', {'instrument' : instrument}).fetchall()
                rdm_number = random.randint(0, len(date_list) - 1)
                #随机选择一天
                date = date_list[rdm_number][0]
                data = read_decompress(FUTURE_TICK_ORGANIZED_DATA_PATH + product + os.path.sep + instrument + '.pkl')
                get_logger().info('Handle instrument {0}'.format(instrument))
                data['date'] = data['datetime'].str[0:10]
                data['product'] = product
                data['instrument'] = instrument
                # 股票过滤
                filter_stocks = []
                if isinstance(factor, StockTickFactor):
                    stock_list = factor.get_stock_list_by_date(product, date)
                    while stock_count > 0:
                        rdm_number = random.randint(0, len(stock_list) - 1)
                        stock = stock_list[rdm_number]
                        filter_stocks.append(stock)
                        stock_count = stock_count - 1
                    get_logger().info('Handle stocks {}'.format('|'.join(filter_stocks)))
                    factor.set_stock_filter(filter_stocks)
                data = factor.caculate(data)
                get_logger().info('Complete factor caculation for {0}'.format(str(i)))
                daily_data = data[data['date'] == date]
                if len(daily_data) > 0:
                    daily_data = daily_data.reset_index(drop=True)
                    rdm_index = random.randint(0, len(daily_data) - window_size)
                    # 截取时间片段
                    daily_data = daily_data.loc[rdm_index: rdm_index + window_size]
                    get_logger().info('Prepare the stock data for {0}'.format(str(i)))
                    if len(filter_stocks) > 0: #如果加了股票过滤，截取相应股票数据并且移动到当前目录，便于检查
                        daily_data.to_csv(FACTOR_PATH + 'manually' + os.path.sep + product + '_' + factor.get_full_name() + '_' + '-'.join(filter_stocks) + '_' + str(i) + '.csv')
                        daily_data['time'] = daily_data['datetime'].apply(lambda dt: add_milliseconds_suffix(dt[11:19]))
                        for stock in filter_stocks:
                            data_access = StockDataAccess(False)
                            stock_data = data_access.access(date, stock)
                            if not is_accumulated: #如果不是累加型因子，截取时间段
                                min_time = daily_data['time'].min()
                                max_time = daily_data['time'].max()
                                stock_data = stock_data[(stock_data['time'] >= min_time) & (stock_data['time'] <= max_time)]
                            stock_data.to_csv(FACTOR_PATH + 'manually' + os.path.sep + product + '_' + factor.get_full_name() + '_' + stock + '.csv')
                            if isinstance(factor, TimewindowStockTickFactor):
                                days_before = future_instrument_config_dao.get_last_n_transaction_date_list(date, 3)
                                for dt in days_before:
                                    if dt != date:
                                        stock_data_before = data_access.access(dt, stock)
                                        stock_data_before.to_csv(FACTOR_PATH + 'manually' + os.path.sep + product + '_' + factor.get_full_name() + '_' + stock + '_' + dt + '.csv')
                    else:
                        daily_data.to_csv(
                            FACTOR_PATH + 'manually' + os.path.sep + product + '_' + factor.get_full_name() + '_' + str(i) + '.csv')

    @timing
    def init_instrument_config(self):
        session = create_session()
        session.execute('delete from future_instrument_config')
        main_instrument_config = pd.DataFrame(pd.read_pickle(CONFIG_PATH + 'all-main.pkl'))
        main_instrument_config = main_instrument_config[STOCK_INDEX_PRODUCTS]
        for product in STOCK_INDEX_PRODUCTS:
            instrument_list = list_files_in_path(FUTURE_TICK_ORGANIZED_DATA_PATH + product)
            instrument_list.sort()
            for instrument in instrument_list:
                if not re.search('[0-9]{4}', instrument):
                    continue
                data = read_decompress(FUTURE_TICK_ORGANIZED_DATA_PATH + product + os.path.sep + instrument)
                data['date'] = data['datetime'].str[0:10]
                date_list = list(set(data['date'].tolist()))
                date_list.sort()
                for date in date_list:
                    main_instrument = main_instrument_config[main_instrument_config.index == date][product].tolist()[0]
                    current_instrument = instrument.split('.')[0]
                    if main_instrument == current_instrument:
                        config = FutureInstrumentConfig(product, current_instrument , date, 0)
                    else:
                        config = FutureInstrumentConfig(product, current_instrument, date, 1)
                    session.add(config)
                    session.commit()


if __name__ == '__main__':
    #生成主力合约配置
    # FactorCaculator().init_instrument_config()

    #因子计算
    # william_factor = WilliamFactor()
    # factor_list = [william_factor]
    # total_commission_ratio_factor = TotalCommissionRatioFactor()
    # factor_list = [total_commission_ratio_factor]
    # ten_grade_commission_ratio_factor = TenGradeCommissionRatioFactor()
    # factor_list = [ten_grade_commission_ratio_factor]
    # dail_rising_stock_ratio_factor = DailyRisingStockRatioFactor()
    # factor_list = [dail_rising_stock_ratio_factor]
    # FactorCaculator().caculate('8f771a6c-4233-4239-a12c-defb23963e08', factor_list)
    # FactorCaculator().caculate(factor_list, ['IF1810','IF1811'])
    # FactorCaculator().caculate(factor_list, ['IF1810','IF1811','IF1812','IF1901','IF1902','IF1903','IF1904','IF1905','IF1906','IF1907'])
    # FactorCaculator().caculate('8f771a6c-4233-4239-a12c-defb23963e01', factor_list, include_instrument_list = ['IF1712'], performance_test=True)

    # 直接调用caculate_by_instrument便于cprofile分析
    # FactorCaculator().caculate_by_instrument((factor_list, 'IF1712', 'IF'))

    #生成因子比对文件
    # factor = WilliamFactor([10])
    # factor = TotalCommissionRatioFactor()
    # factor = TenGradeCommissionRatioFactor()
    # factor = FiveGradeCommissionRatioFactor()
    # factor = TenGradeWeightedCommissionRatioFactor()
    # factor = FiveGradeCommissionRatioFactor()
    # factor = AmountAndCommissionRatioFactor()
    # factor = RisingFallingAmountRatioFactor()
    # factor = UntradedStockRatioFactor()
    # factor = DailyAccumulatedLargeOrderRatioFactor()
    # factor = RollingAccumulatedLargeOrderRatioFactor([10])
    # factor = RisingStockRatioFactor()
    # factor = SpreadFactor()
    # factor = OverNightYieldFactor()
    # factor = DeltaTotalCommissionRatioFactor([5])
    # factor = CallAuctionSecondStageIncreaseFactor()
    # factor = TwoCallAuctionStageDifferenceFactor()
    # factor = CallAuctionSecondStageReturnVolatilityFactor()
    # factor = FirstStageCommissionRatioFactor()
    # factor = SecondStageCommissionRatioFactor()
    # factor = AmountAnd1stGradeCommissionRatioFactor()
    # factor = TotalCommissionRatioDifferenceFactor([20, 50, 100, 200])
    # factor = AmountAskTotalCommissionRatioFactor()
    # factor = TenGradeCommissionRatioDifferenceFactor([20, 50, 100, 200])
    # factor = FiveGradeCommissionRatioDifferenceFactor([20, 50, 100, 200])
    # factor = DailyRisingStockRatioFactor()
    # factor = LargeOrderBidAskVolumeRatioFactor()
    # factor = TenGradeCommissionRatioMeanFactor([20,50,100,300,500])
    # factor = FiveGradeCommissionRatioMeanFactor([20,50,100,300,500])
    # factor = FiveGradeCommissionRatioStdFactor([50,100,300,500])
    # factor = FiveGradeCommissionRatioStdFactor([50,100,300,500])
    # factor = TenGradeWeightedCommissionRatioFactor()
    # factor = FiveGradeWeightedCommissionRatioFactor()
    # factor = AuxiliaryFileGenerationFactor()
    # factor = BidLargeAmountBillFactor()
    # factor = AskLargeAmountBillFactor()
    # factor = AmountBid10GradeCommissionRatioFactor([20,50,100,300,500])
    # factor = AmountAsk10GradeCommissionRatioStdFactor([20,50,100,300,500])
    # factor = Commission10GradeVolatilityRatioFactor([20, 50 ,100 ,200])
    # factor = AmountAndCommissionRatioMeanFactor([20, 50 ,100 ,300, 500])
    # factor = AmountAndCommissionRatioStdFactor([50 ,100 ,300, 500])
    factor = LargeOrderBidAskVolumeRatioFactor()
    FactorCaculator().caculate_manually_check(factor, is_accumulated = True)

    # session = create_session()
    # config = FutureInstrumentConfig('IF', 'TEST', '20221204', 0)
    # session.add(config)
    # session.commit()

