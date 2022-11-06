#! /usr/bin/env python
# -*- coding:utf8 -*-
import pymysql

from common.constants import DB_CONNECTION


def create_session():
    """创建一个数据库链接会话
    """
    pymysql.install_as_MySQLdb()
    from sqlalchemy import create_engine
    engine = create_engine(DB_CONNECTION)
    from sqlalchemy.orm import sessionmaker
    DbSession = sessionmaker(bind=engine)
    session = DbSession()
    return session
