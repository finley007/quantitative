#! /usr/bin/env python
# -*- coding:utf8 -*-
import uuid
import datetime

from sqlalchemy import Column, String, Integer, DateTime, Date, Time, BigInteger
from sqlalchemy.ext.declarative import declarative_base

from common.constants import RESULT_SUCCESS, FACTOR_TYPE_DETAILS

Base = declarative_base()

class StockValidationResult(Base):
    """股票验证记录表po：
    """
    __tablename__ = "stock_validation_result"

    id = Column(String(32), primary_key=True)
    validation_code = Column(String(32))
    tscode = Column(String(10))
    date = Column(String(10))
    result = Column(Integer)
    err_msg = Column(String(1024))
    record_count = Column(Integer)
    created_time = Column(DateTime)
    modified_time = Column(DateTime)

    def __init__(self, validation_code, validation_result, record_count):
        self.id = uuid.uuid4()
        self.validation_code = validation_code
        self.tscode = validation_result.tscode
        self.date = validation_result.date
        if validation_result.result == RESULT_SUCCESS:
            self.result = 0
        else:
            self.result = 1
        str_validation_result = str(validation_result)
        if (len(str_validation_result) > 1020):
            str_validation_result = str_validation_result[0:1020] + '...'
        self.err_msg = str_validation_result
        self.record_count = record_count
        self.created_time = datetime.datetime.now()
        self.modified_time = datetime.datetime.now()

class FutrueProcessRecord(Base):
    """期货处理记录表po：
    """
    __tablename__ = "future_process_record"

    id = Column(String(32), primary_key=True)
    process_code = Column(String(32))
    instrument = Column(String(10))
    date = Column(String(10))
    status = Column(Integer)
    created_time = Column(DateTime)
    modified_time = Column(DateTime)

    def __init__(self, process_code, instrument, date, status):
        self.id = uuid.uuid4()
        self.process_code = process_code
        self.instrument = instrument
        self.date = date
        self.status = status
        self.created_time = datetime.datetime.now()
        self.modified_time = datetime.datetime.now()

class StockProcessRecord(Base):
    """股票处理记录表po：
    """
    __tablename__ = "stock_process_record"

    id = Column(String(32), primary_key=True)
    process_code = Column(String(32))
    tscode = Column(String(10))
    date = Column(String(10))
    status = Column(Integer)
    invalid_msg = Column(String(1024))
    created_time = Column(DateTime)
    modified_time = Column(DateTime)

    def __init__(self, process_code, tscode, date, status, invalid_msg=''):
        self.id = uuid.uuid4()
        self.process_code = process_code
        self.tscode = tscode
        self.date = date
        self.status = status
        if (len(invalid_msg) > 1020):
            invalid_msg = invalid_msg[0:1020] + '...'
        self.invalid_msg = invalid_msg
        self.created_time = datetime.datetime.now()
        self.modified_time = datetime.datetime.now()

class FutureInstrumentConfig(Base):
    """期货合约配置表po：
    """
    __tablename__ = "future_instrument_config"

    id = Column(String(32), primary_key=True)
    product = Column(String(2))
    instrument = Column(String(8))
    date = Column(String(10))
    is_main = Column(Integer)
    created_time = Column(DateTime)
    modified_time = Column(DateTime)

    def __init__(self, product, instrument, date, is_main):
        self.id = uuid.uuid4()
        self.product = product
        self.instrument = instrument
        self.date = date
        self.is_main = is_main
        self.created_time = datetime.datetime.now()
        self.modified_time = datetime.datetime.now()


class IndexConstituentConfig(Base):
    """股指成分股映射表po：
    """
    __tablename__ = "index_constituent_config"

    id = Column(String(32), primary_key=True)
    product = Column(String(2))
    date = Column(String(10))
    tscode = Column(String(10))
    status = Column(Integer)
    created_time = Column(DateTime)
    modified_time = Column(DateTime)

    def __init__(self, product, date, tscode, status = 0):
        self.id = uuid.uuid4()
        self.product = product
        self.date = date
        self.tscode = tscode
        self.status = status
        self.created_time = datetime.datetime.now()
        self.modified_time = datetime.datetime.now()

class FactorConfig(Base):
    """因子表po：
    code VARCHAR(200),
    type  VARCHAR(10),
    type_name VARCHAR(100),
    number VARCHAR(10),
    name    VARCHAR(100),
    parameter  VARCHAR(100),
    version  VARCHAR(10),
    created_time datetime,
    modified_time datetime,
    """
    __tablename__ = "factor_config"

    code = Column(String(200), primary_key=True)
    version = Column(String(10), primary_key=True)
    type = Column(String(10))
    type_name = Column(String(100))
    number = Column(String(10))
    name = Column(String(100))
    parameter = Column(String(100))
    source = Column(String(20))
    created_time = Column(DateTime)
    modified_time = Column(DateTime)

    def __init__(self, code, version, type, number, name, parameter, source):
        self.code = code
        self.version = version
        self.type = type
        self.type_name = FACTOR_TYPE_DETAILS[type]['name']
        self.number = number
        self.name = name
        self.parameter = parameter
        self.source = source
        self.created_time = datetime.datetime.now()
        self.modified_time = datetime.datetime.now()

    def get_full_name(self):
        return self.code + '_' + self.version

class FactorOperationHistory(Base):
    """因子表po：
    id VARCHAR(40),
    target_factor VARCHAR(2000),
    operation int '1-生成因子文件, 2-生成统计信息，3-合并因子文件',
    status  int comment '0-成功，1-失败',
    time_cost long,
    err_msg  VARCHAR(1024),
    created_time datetime,
    modified_time datetime,
    """
    __tablename__ = "factor_operation_history"

    id = Column(String(40), primary_key=True)
    target_factor = Column(String(2000))
    operation = Column(Integer)
    status = Column(Integer)
    time_cost = Column(BigInteger)
    err_msg = Column(String(1024))
    created_time = Column(DateTime)
    modified_time = Column(DateTime)

    def __init__(self, target_factor, operation, status, time_cost, err_msg = ''):
        self.id = uuid.uuid4()
        self.target_factor = target_factor
        self.operation = operation
        self.status = status
        self.time_cost = time_cost
        if (len(err_msg) > 1020):
            self.err_msg = err_msg[0:1020] + '...'
        self.created_time = datetime.datetime.now()
        self.modified_time = datetime.datetime.now()

class StockMissingData(Base):
    """缺失股票数据表po：
    id VARCHAR(40),
    date    VARCHAR(10),
    tscode  VARCHAR(10),
    created_time datetime,
    modified_time datetime,
    """
    __tablename__ = "stock_missing_data"

    id = Column(String(40), primary_key=True)
    date = Column(String(10))
    tscode = Column(String(10))
    created_time = Column(DateTime)
    modified_time = Column(DateTime)

    def __init__(self, date, tscode):
        self.id = uuid.uuid4()
        self.date = date
        self.tscode = tscode
        self.created_time = datetime.datetime.now()
        self.modified_time = datetime.datetime.now()

class Test(Base):
    """测试表po：
    """
    __tablename__ = "test"

    varchar_column = Column(String(10), primary_key=True)
    int_column = Column(Integer)
    date_column = Column(Date)
    datetime_column = Column(DateTime)
    time_column = Column(Time)
    created_time = Column(DateTime)
    modified_time = Column(DateTime)

    def __init__(self, varchar_column, int_column, date_column, datetime_column, time_column):
        self.varchar_column = varchar_column
        self.int_column = int_column
        self.date_column = date_column
        self.datetime_column = datetime_column
        self.time_column = time_column
        self.created_time = datetime.datetime.now()
        self.modified_time = datetime.datetime.now()
