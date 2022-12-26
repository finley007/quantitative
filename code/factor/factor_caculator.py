#! /usr/bin/env python
# -*- coding:utf8 -*-
import os
import re
import time
import pandas as pd
import random

from common.aop import timing
from common.constants import FUTURE_TICK_ORGANIZED_DATA_PATH, CONFIG_PATH, FACTOR_PATH, STOCK_INDEX_PRODUCTS
from common.exception.exception import InvalidStatus
from common.localio import read_decompress, list_files_in_path, save_compress
from common.persistence.dbutils import create_session
from common.persistence.po import FutureInstrumentConfig
from factor.volume_price_factor import WilliamFactor

class FactorCaculator():
    """因子文件生成类
        生成按品种目录的因子文件
    Parameters
    ----------
    """

    _manually_check_file_count = 10

    # @timing
    def caculate(self, factor_list
                 ):
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
            factor_data = pd.DataFrame()
            instrument_list = session.execute('select distinct instrument from future_instrument_config where product = :product order by instrument', {'product': product})
            for instrument in instrument_list:
                data = read_decompress(FUTURE_TICK_ORGANIZED_DATA_PATH + product + os.path.sep + instrument[0] + '.pkl')
                data['date'] = data['datetime'].str[0:10]
                data['product'] = product
                data['instrument'] = instrument[0]
                for factor in factor_list:
                    data = factor.caculate(data)
                #截取主力合约区间
                date_range = session.execute(
                    'select min(date), max(date) from future_instrument_config where product = :product and instrument = :instrument and is_main = 0',
                    {'product': product, 'instrument': instrument[0]})
                if date_range.rowcount > 0:
                    date_range_query_result = date_range.fetchall()
                    start_date = date_range_query_result[0][0]
                    end_date = date_range_query_result[0][1]
                    data = data[(data['date'] >= start_date) & (data['date'] <= end_date)]
                    factor_data = pd.concat([factor_data, data])
            factor_data = factor_data.reset_index()
            save_compress(factor_data, FACTOR_PATH + product + '_' + '_'.join(list(map(lambda factor: factor.get_full_name(), factor_list))))

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
                data = read_decompress(
                    FUTURE_TICK_ORGANIZED_DATA_PATH + product + os.path.sep + instrument[0] + '.pkl')
                data['date'] = data['datetime'].str[0:10]
                data['product'] = product
                data = factor.caculate(data)
                if len(data) > 0:
                    start_index = max(factor.get_params())
                    rdm_index = random.randint(start_index, len(data) - window_size)
                    data = data.loc[rdm_index: rdm_index + window_size]
                    data.to_csv(FACTOR_PATH + 'manually' + os.path.sep + product + '_' + factor.get_full_name() + '_' + str(i) + '.csv')

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
    # FactorCaculator().caculate(factor_list)

    #生成因子比对文件
    # william_factor = WilliamFactor([10])
    # FactorCaculator().caculate_manually_check(william_factor)

    session = create_session()
    config = FutureInstrumentConfig('IF', 'TEST', '20221204', 0)
    session.add(config)
    session.commit()

