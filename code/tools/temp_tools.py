#! /usr/bin/env python
# -*- coding:utf8 -*-

from common.persistence.dbutils import create_session
from common.persistence.dao import IndexConstituentConfigDao
from framework.pagination import Pagination
from framework.localconcurrent import ProcessExcecutor

def update_index_constituent_config():
    """
    把20230309-finley 小于10的错误临时更新为正常
    Returns
    -------

    """
    session = create_session()
    results = session.execute("select distinct tscode, date from stock_validation_result where validation_code = '20230309-finley' and result = 1 and issue_count <= 10 order by tscode, date")
    update_list = list(map(lambda item: (item[0], item[1]), results))
    pagination = Pagination(update_list, page_size=50)
    while pagination.has_next():
        date_list = pagination.next()
        ProcessExcecutor(10).execute(update, date_list)

def update(*args):
    stock = args[0][0]
    date = args[0][1]
    date = date[0:4] + '-' + date[4:6] + '-' + date[6:8]
    print('Handle stock:{0} and date:{1}'.format(stock, date))
    constituent_config_dao = IndexConstituentConfigDao()
    constituent_config_dao.update_status(date, stock, 0)



if __name__ == '__main__':
    update_index_constituent_config()
