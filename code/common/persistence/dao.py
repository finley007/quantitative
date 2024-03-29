#! /usr/bin/env python
# -*- coding:utf8 -*-

from sqlalchemy import and_
import numpy as np

from common.persistence.dbutils import create_session
from common.persistence.po import Test, FactorConfig, StockReversionConfig, IndexConstituentConfig

class BaseDao():

    def __init__(self):
        self._session = create_session()

    def close(self):
        self._session.close()


class IndexConstituentConfigDao(BaseDao):

    def query_trading_date_by_tscode(self, tscode):
        """
        获取给定股票的交易日，考虑停盘

        Parameters
        ----------
        tscode：string 待查寻股票

        Returns
        -------

        """
        result_list = self._session.execute('select distinct date from index_constituent_config where tscode = :tscode and status = 0 order by date', {'tscode' : tscode}).fetchall()
        result_list = list(map(lambda date: date[0], result_list))
        return result_list

    def query_trading_date_by_tscode_list(self, tscode_list, min_date, max_date):
        """
        批量查询一段时间范围内股票交易日
        Parameters
        ----------
        tscode_list
        min_date
        max_date

        Returns
        -------

        """
        result_list = self._session.execute(
            'select distinct tscode, date from index_constituent_config where tscode in :tscode_list and status = 0 and date >= :min_date and date <= :max_date order by tscode, date',
            {'tscode_list' : tscode_list, 'min_date' : min_date, 'max_date' : max_date}).fetchall()
        result_list = list(map(lambda item: [item[0], item[1]], result_list))
        result_list = np.array(result_list)
        result = {}
        for key in set(result_list[:, 0]):
            filter = result_list[:, 0] == key
            temp_arr = result_list[filter]
            result[key] = temp_arr[:, 1].tolist()
        return result

    def get_invalid_list(self, invalid_status=[1, 2]):
        """
        获取非法数据：
        1 停盘
        2 数据问题
        Parameters
        ----------
        invalid_status

        Returns
        -------

        """
        invalid_list = self._session.execute('select distinct date, tscode from index_constituent_config where status in :status', {'status': invalid_status}).fetchall()
        return set(list(map(lambda invalid: invalid[0] + invalid[1], invalid_list)))

    def get_invalid_date_list(self, invalid_status=[1, 2]):
        """
        获取非法数据：
        1 停盘
        2 数据问题
        Parameters
        ----------
        invalid_status

        Returns
        -------

        """
        invalid_date_list = self._session.execute('select distinct date from index_constituent_config where status in :status order by date', {'status': invalid_status}).fetchall()
        return list(map(lambda result: result[0], invalid_date_list))

    def update_status(self, date, tscode, status):
        self._session.query(IndexConstituentConfig).filter(and_(IndexConstituentConfig.date == date, IndexConstituentConfig.tscode == tscode)).update({IndexConstituentConfig.status: status})
        self._session.commit()

    def get_st_list(self):
        return self._session.execute('select distinct date, tscode from index_constituent_config where is_st = 1 order by tscode, date').fetchall()

class FactorConfigDao(BaseDao):

    def get_all_factors(self):
        """
        获取所有因子列表

        Returns
        -------

        """
        return self._session.query(FactorConfig).all()

class FutureConfigDao(BaseDao):

    def get_start_end_date_by_instrument(self, instrument):
        """
        获取股指合约的起始日期

        Parameters
        ----------
        instrument

        Returns
        -------

        """
        result_list =  self._session.execute('select max(date) as max_date, min(date) as min_date from future_instrument_config where instrument = :instrument', {'instrument': instrument}).fetchall()
        start_date = result_list[0][0]
        end_date = result_list[0][1]
        return start_date, end_date

    def filter_date(self, start_date='', end_date=''):
        if start_date == '':
            result_list = self._session.execute(
                'select distinct date from future_instrument_config where date >= :end_date order by date',
                {'end_date': end_date}).fetchall()
        elif end_date == '':
            result_list = self._session.execute(
                'select distinct date from future_instrument_config where date <= :start_date order by date',
                {'start_date': start_date}).fetchall()
        else:
            result_list = self._session.execute('select distinct date from future_instrument_config where date <= :start_date or date >= :end_date order by date', {'start_date': start_date, 'end_date':end_date}).fetchall()
        result_list = list(map(lambda item: item[0], result_list))
        return result_list

    def get_main_instrument_by_product_and_date(self, product, date):
        result_list = self._session.execute('select instrument from future_instrument_config where is_main = 0 and product = :product and date = :date', {'product': product, 'date':date}).fetchall()
        result_list = list(map(lambda item: item[0], result_list))
        if len(result_list) > 0:
            return result_list[0]
        else:
            return ''

class FutureInstrumentConfigDao(BaseDao):

    def get_next_n_transaction_date(self, current_date, n):
        result_list = self._session.execute('select distinct date from future_instrument_config where date > :current_date order by date', {'current_date': current_date}).fetchall()
        if len(result_list) > n:
            return result_list[n-1][0]
        else:
            return ''

    def get_next_n_transaction_date_list(self, current_date, n):
        result_list = self._session.execute('select distinct date from future_instrument_config where date > :current_date order by date', {'current_date': current_date}).fetchall()
        if len(result_list) > n:
            return list(map(lambda item: item[0], result_list[:n]))
        else:
            return []

    def get_last_n_transaction_date(self, current_date, n):
        result_list = self._session.execute('select distinct date from future_instrument_config where date < :current_date order by date desc', {'current_date': current_date}).fetchall()
        if len(result_list) > n:
            return result_list[n-1][0]
        else:
            return ''

    def get_last_n_transaction_date_list(self, current_date, n):
        result_list = self._session.execute('select distinct date from future_instrument_config where date < :current_date order by date desc', {'current_date': current_date}).fetchall()
        if len(result_list) > n:
            result_list = list(map(lambda item: item[0], result_list[:n]))
            result_list.reverse()
            return result_list
        else:
            return []

class StockReversionConfigDao(BaseDao):

    def delete_records_by_stocks(self, stocks):
        self._session.query(StockReversionConfig).filter(StockReversionConfig.tscode.in_(stocks)).delete()

if __name__ == '__main__':
    constituent_config_dao = IndexConstituentConfigDao()
    # # print(constituent_config_dao.query_trading_date_by_tscode('600519'))
    print(constituent_config_dao.query_trading_date_by_tscode_list(['600519', '000001'], '2022-01-01', '2022-08-09'))
    # # constituent_config_dao.update_status('2018-11-02', '601200', 0)
    # print(constituent_config_dao.get_invalid_list([1,2]))
    # print(constituent_config_dao.get_invalid_date_list([2]))
    # print(constituent_config_dao.get_st_list())

    # session = create_session()

    # test_po = Test('test1', 40, '2022-12-18', '2022-12-18 02:12:13', '12:12:13')
    # session.add(test_po)
    # session.commit()

    # 更新po
    # test_po = session.query(Test).filter(Test.varchar_column == 'test1').one()
    # test_po.int_column = 30
    # session.commit()

    # instrument_config_dao = InstrumentConfigDao()
    # print(instrument_config_dao.get_start_end_date_by_instrument('IF2103'))

    # future_config_dao = FutureConfigDao()
    # print(future_config_dao.filter_date('2017-01-03', '2022-06-13'))
    # print(future_config_dao.filter_date('2017-01-03'))
    # print(future_config_dao.filter_date('', '2022-06-13'))
    # print(future_config_dao.get_main_instrument_by_product_and_date('IH', '2018-03-20'))

    # future_instrument_config_dao = FutureInstrumentConfigDao()
    # print(future_instrument_config_dao.get_next_n_transaction_date('2017-01-03', 5))
    # print(future_instrument_config_dao.get_next_n_transaction_date_list('2017-01-03', 5))
    # print(future_instrument_config_dao.get_last_n_transaction_date('2017-01-03', 5))
    # print(future_instrument_config_dao.get_last_n_transaction_date_list('2017-01-03', 5))

    # 因子配置表查询
    # facto_config_dao = FactorConfigDao()
    # for factor in facto_config_dao.get_all_factors():
    #     print(factor.code)


