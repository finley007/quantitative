#! /usr/bin/env python
# -*- coding:utf8 -*-
import os
import re
import time
import pandas as pd

from common.aop import timing
from common.constants import FUTURE_TICK_ORGANIZED_DATA_PATH, CONFIG_PATH, FACTOR_PATH
from common.io import read_decompress, list_files_in_path, save_compress
from common.persistence.dbutils import create_session
from common.persistence.po import FutureInstrumentConfig
from factor.volume_price_factor import WilliamFactor

class FactorCaculator():
    """因子文件生成类

    Parameters
    ----------
    """

    @timing
    def caculate(self, factor_list):
        if len(factor_list) == 0:
            raise InvalidStatus('Empty factor list')
        #获取k线文件列模板
        example = read_decompress(FUTURE_TICK_ORGANIZED_DATA_PATH + 'IF' + os.path.sep + 'IF1701.pkl')
        columns = example.columns.tolist()
        for factor in factor_list:
            columns.append(factor.factor_code)
        factor_data = pd.DataFrame(columns=columns)
        products = ['IC', 'IH', 'IF']
        session = create_session()
        for product in products:
            instrument_list = session.execute('select distinct instrument from future_instrument_config where product = :product order by instrument', {'product': product})
            for instrument in instrument_list:
                data = read_decompress(FUTURE_TICK_ORGANIZED_DATA_PATH + product + os.path.sep + instrument[0] + '.pkl')
                data['date'] = data['datetime'].str[0:10]
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
        save_compress(factor_data, FACTOR_PATH + '_'.join(list(map(lambda factor: factor.get_full_name(), factor_list))))

    @timing
    def init_instrument_config(self):
        session = create_session()
        session.execute('delete from future_instrument_config')
        products = ['IC', 'IH', 'IF']
        main_instrument_config = pd.DataFrame(pd.read_pickle(CONFIG_PATH + 'all-main.pkl'))
        main_instrument_config = main_instrument_config[products]
        print(main_instrument_config)
        for product in products:
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
    william_factor = WilliamFactor()
    factor_list = [william_factor]
    FactorCaculator().caculate(factor_list)
