#! /usr/bin/env python
# -*- coding:utf8 -*-
import os
import re
import time
import pandas as pd
import random
import uuid

from common.aop import timing
from common.constants import FUTURE_TICK_ORGANIZED_DATA_PATH, CONFIG_PATH, FACTOR_PATH, STOCK_INDEX_PRODUCTS, TEMP_PATH
from common.exception.exception import InvalidStatus
from common.localio import read_decompress, list_files_in_path, save_compress
from common.persistence.dbutils import create_session
from common.persistence.po import FutureInstrumentConfig
from factor.volume_price_factor import WilliamFactor
from factor.spot_goods_factor import TotalCommissionRatioFactor, TenGradeCommissionRatioFactor, TenGradeCommissionRatioFactor, AmountAndCommissionRatioFactor
from factor.base_factor import StockTickFactor
from common.log import get_logger
from framework.pagination import Pagination
from framework.localconcurrent import ProcessRunner, ProcessExcecutor
from common.persistence.po import FactorProcessRecord
from data.access import StockDataAccess
from common.timeutils import add_milliseconds_suffix

class FactorCaculator():
    """因子文件生成类
        生成按品种目录的因子文件
    Parameters
    ----------
    """

    _manually_check_file_count = 2

    @timing
    def caculate(self, process_code, factor_list, include_instrument_list=[]):
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
        for product in STOCK_INDEX_PRODUCTS:
            # factor_data = pd.DataFrame(columns=columns)
            temp_file = 'E:\\data\\temp\\' + product + '_' + '_'.join(
                list(map(lambda factor: factor.get_full_name(), factor_list))) + '.temp'
            check_handled = session.execute('select max(instrument) from factor_process_record where process_code = :process_code', {'process_code':process_code}).fetchall()
            current_instrument = check_handled[0][0]
            if current_instrument:
                factor_data = read_decompress(temp_file)
            else:
                factor_data = pd.DataFrame()
            instrument_list = session.execute('select distinct instrument from future_instrument_config where product = :product order by instrument', {'product': product}).fetchall()
            instrument_list = list(filter(lambda instrument : len(include_instrument_list) == 0 or instrument[0] in include_instrument_list, instrument_list))
            instrument_list = list(map(lambda instrument: instrument[0], instrument_list))
            pagination = Pagination(instrument_list, page_size=10)
            skip = False
            while pagination.has_next():
                sub_instrument_list = pagination.next()
                if current_instrument in sub_instrument_list: #已经处理的最后一页，下一页开始要处理了
                    skip = True
                    continue
                if current_instrument and current_instrument not in sub_instrument_list and not skip:
                    continue
                params_list = list(map(lambda instrument: [factor_list, instrument, product], sub_instrument_list))
                results = ProcessExcecutor(1).execute(self.caculate_by_instrument, params_list)
                temp_cache = {}
                for result in results:
                    temp_cache[result[0]] = result[1]
                for instrument in sub_instrument_list:
                    factor_data = pd.concat([factor_data, temp_cache[instrument]])
                    factor_process_record = FactorProcessRecord(process_code, instrument)
                    session.add(factor_process_record)
                session.commit()
                save_compress(factor_data, temp_file)
            factor_data = factor_data.reset_index()
            # target_factor_file = FACTOR_PATH + product + '_' + '_'.join(
            #     list(map(lambda factor: factor.get_full_name(), factor_list)))
            target_factor_file = 'E:\\data\\test\\' + product + '_' + '_'.join(
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
    def caculate_manually_check(self, factor):
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
        for product in STOCK_INDEX_PRODUCTS:
            instrument_list = session.execute(
                'select distinct instrument from future_instrument_config where product = :product order by instrument',
                {'product': product}).fetchall()
            for i in range(self._manually_check_file_count):
                rdm_number = random.randint(0, len(instrument_list) - 1)
                instrument = instrument_list[rdm_number]
                date_list = session.execute('select date from future_instrument_config where instrument = :instrument and is_main = 0 order by date', {'instrument' : instrument[0]}).fetchall()
                rdm_number = random.randint(0, len(date_list) - 1)
                date = date_list[rdm_number][0]
                data = read_decompress(FUTURE_TICK_ORGANIZED_DATA_PATH + product + os.path.sep + instrument[0] + '.pkl')
                data['date'] = data['datetime'].str[0:10]
                data['product'] = product
                data['instrument'] = instrument[0]
                filter_stocks = []
                if isinstance(factor, StockTickFactor):
                    stock_list = factor.get_stock_list_by_date(product, date)
                    stock_count = 2
                    while stock_count > 0:
                        rdm_number = random.randint(0, len(stock_list) - 1)
                        stock = stock_list[rdm_number]
                        filter_stocks.append(stock)
                        stock_count = stock_count - 1
                    factor.set_stock_filter(filter_stocks)
                data = factor.caculate(data)
                daily_data = data[data['date'] == date]
                if len(daily_data) > 0:
                    daily_data = daily_data.reset_index()
                    rdm_index = random.randint(0, len(daily_data) - window_size)
                    daily_data = daily_data.loc[rdm_index: rdm_index + window_size]
                    if len(filter_stocks) > 0:
                        daily_data.to_csv(FACTOR_PATH + 'manually' + os.path.sep + product + '_' + factor.get_full_name() + '_' + '-'.join(filter_stocks) + '_' + str(i) + '.csv')
                        daily_data['time'] = daily_data['datetime'].apply(lambda dt: add_milliseconds_suffix(dt[11:19]))
                        print(daily_data)
                        for stock in filter_stocks:
                            data_access = StockDataAccess(False)
                            stock_data = data_access.access(date, stock)
                            min_time = daily_data['time'].min()
                            max_time = daily_data['time'].max()
                            stock_data = stock_data[(stock_data['time'] >= min_time) & (stock_data['time'] <= max_time)]
                            stock_data.to_csv(FACTOR_PATH + 'manually' + os.path.sep + product + '_' + factor.get_full_name() + '_' + stock + '.csv')
                    else:
                        daily_data.to_csv(
                            FACTOR_PATH + 'manually' + os.path.sep + product + '_' + factor.get_full_name() + '_' + str(i) + '.csv')

    @timing
    def init_instrument_config(self):
        session = create_session()
        session.execute('delete from future_instrument_config')
        main_instrument_config = pd.DataFrame(pd.read_pickle(CONFIG_PATH + 'all-main.pkl'))
        main_instrument_config = main_instrument_config[STOCK_INDEX_PRODUCTS]
        print(main_instrument_config)
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
    # FactorCaculator().caculate('8f771a6c-4233-4239-a12c-defb23963e08', factor_list)
    # FactorCaculator().caculate(factor_list, ['IF1810','IF1811'])
    # FactorCaculator().caculate(factor_list, ['IF1810','IF1811','IF1812','IF1901','IF1902','IF1903','IF1904','IF1905','IF1906','IF1907'])

    #生成因子比对文件
    # factor = WilliamFactor([10])
    # factor = TotalCommissionRatioFactor()
    factor = TenGradeCommissionRatioFactor()
    # factor = AmountAndCommissionRatioFactor()
    FactorCaculator().caculate_manually_check(factor)

    # session = create_session()
    # config = FutureInstrumentConfig('IF', 'TEST', '20221204', 0)
    # session.add(config)
    # session.commit()

