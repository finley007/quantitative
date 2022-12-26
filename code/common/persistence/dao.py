#! /usr/bin/env python
# -*- coding:utf8 -*-

from common.persistence.dbutils import create_session
from common.persistence.po import Test, FactorConfig

class BaseDao():

    def __init__(self):
        self._session = create_session()


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

class FactorConfigDao(BaseDao):

    def get_all_factors(self):
        """
        获取所有因子列表

        Returns
        -------

        """
        return self._session.query(FactorConfig).all()

if __name__ == '__main__':
    # print(IndexConstituentConfigDao().query_trading_date_by_tscode('600519'))

    session = create_session()

    # test_po = Test('test1', 40, '2022-12-18', '2022-12-18 02:12:13', '12:12:13')
    # session.add(test_po)
    # session.commit()

    # 更新po
    test_po = session.query(Test).filter(Test.varchar_column == 'test1').one()
    test_po.int_column = 30
    session.commit()


