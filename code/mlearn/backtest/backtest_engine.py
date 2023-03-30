#! /usr/bin/env python
# -*- coding:utf8 -*-
from abc import abstractmethod, ABCMeta

class BackTestConfig():
    """
    回测配置
    """

    def __init__(self):
        """
        业务数据
        """
        #账户初始金额
        self._initial_capital = 100000
        #手续费
        self._service_charge = 100

        """
        配置数据
        """
        #是否可重复开仓
        self._repeated_opening = False
        #计算手续费
        self._enable_service_charge = False

    def get_initial_capital(self):
        return self._initial_capital

    def get_service_charge(self):
        return self._service_charge

    def get_repeated_opening(self):
        return self._repeated_opening

    def get_enable_service_charge(self):
        return self._enable_service_charge


class Action(metaclass=ABCMeta):
    """
    开仓平仓基类
    """

    _close_date = ''
    _close_price = 0

    def __init__(self, open_date, open_price, config):
        self._open_date = open_date
        self._open_price = open_price
        self._config = config

    def set_close_date(self, close_date):
        self._close_date = close_date

    def set_close_price(self, close_price):
        self._close_price = close_price

    def get_open_date(self):
        return self._open_date

    def get_profit_rate(self):
        return self.get_profit() / self.get_open_price()

    @abstractmethod
    def get_action_type(self):
        pass

    @abstractmethod
    def get_profit(self):
        pass

class LongAction(Action):
    """
    做多
    """

    def get_action_type(self):
        return 'LONG'

    def get_profit(self):
        """
        计算交易收益
        Returns
        -------

        """
        return self.get_close_price() - self.get_open_price()


class OpenStrategy(metaclass=ABCMeta):
    """
    开仓策略
    """

    @abstractmethod
    def open(self, data):
        pass


class CloseStrategy(metaclass=ABCMeta):
    """
    平仓策略
    """

    @abstractmethod
    def close(self, data):
        pass

class BackTestEngine():

    """
    回测引擎基类
    """

    def __init__(self, back_test_config, open_strategy, close_strategy):
        self._back_test_config = back_test_config
        self._open_strategy = open_strategy
        self._close_strategy = close_strategy

    def prepare_data(self):
        #准备数据
        self._data = ''

    def filter_open

    def back_test(self):
        #开仓
        self._open_strategy.open(self._data)
        #平仓
        self._close_strategy.close(self._data)
        if not self._back_test_config.get_repeated_opening():
