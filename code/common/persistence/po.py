#! /usr/bin/env python
# -*- coding:utf8 -*-
import uuid

from sqlalchemy import Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base

from common.constants import RESULT_SUCCESS

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

    def __init__(self, validation_code, tscode, date, result, err_msg=None):
        self.id = uuid.uuid4()
        self.validation_code = validation_code
        self.tscode = tscode
        self.date = date
        self.result = result
        self.err_msg = err_msg

    def __init__(self, validation_code, validation_result):
        self.id = uuid.uuid4()
        self.validation_code = validation_code
        self.tscode = validation_result.tscode
        self.date = validation_result.date
        if validation_result.result == RESULT_SUCCESS:
            self.result = 0
        else:
            self.result = 1
        self.err_msg = str(validation_result)


class FutrueProcessRecord(Base):
    """期货处理记录表po：
    """
    __tablename__ = "future_process_record"

    id = Column(String(32), primary_key=True)
    process_code = Column(String(32))
    instrument = Column(String(10))
    date = Column(String(10))
    status = Column(Integer)

    def __init__(self, process_code, instrument, date, status):
        self.id = uuid.uuid4()
        self.process_code = process_code
        self.instrument = instrument
        self.date = date
        self.status = status

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

    def __init__(self, process_code, tscode, date, status, invalid_msg=''):
        self.id = uuid.uuid4()
        self.process_code = process_code
        self.tscode = tscode
        self.date = date
        self.status = status
        self.invalid_msg = invalid_msg

class FutureInstrumentConfig(Base):
    """期货合约配置表po：
    """
    __tablename__ = "future_instrument_config"

    id = Column(String(32), primary_key=True)
    product = Column(String(2))
    instrument = Column(String(8))
    date = Column(String(10))
    is_main = Column(Integer)

    def __init__(self, product, instrument, date, is_main):
        self.id = uuid.uuid4()
        self.product = product
        self.instrument = instrument
        self.date = date
        self.is_main = is_main


class IndexConstituentConfig(Base):
    """股指成分股映射表po：
    """
    __tablename__ = "index_constituent_config"

    id = Column(String(32), primary_key=True)
    product = Column(String(2))
    date = Column(String(10))
    tscode = Column(String(10))

    def __init__(self, product, date, tscode):
        self.id = uuid.uuid4()
        self.product = product
        self.date = date
        self.tscode = tscode
