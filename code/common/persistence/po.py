#! /usr/bin/env python
# -*- coding:utf8 -*-
import uuid

from sqlalchemy import Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base

from common.constants import RESULT_SUCCESS

Base = declarative_base()

class StockValidationResult(Base):

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