#! /usr/bin/env python
# -*- coding:utf8 -*-
from abc import abstractmethod, ABCMeta

from common.log import get_logger

class ModelTrainer(metaclass=ABCMeta):
    """
    模型训练基类
    """

    def prepare(self, data, config):
        """
        训练集划分，默认按天划分
        Parameters
        ----------
        data
        config

        Returns
        -------

        """
        get_logger().info('Total data count: {0}'.format(str(len(data))))
        train_data = data[data['datetime'] <= config.get_train_partition()]
        test_data = data[data['datetime'] > config.get_train_partition()]
        get_logger().info('Train data count: {0}'.format(str(len(train_data))))
        get_logger().info('Test data count: {0}'.format(str(len(test_data))))
        return train_data, test_data

    @abstractmethod
    def train(self, model_name, version, data, config):
        """
        训练方法
        Parameters
        ----------
        model_name
        version
        data
        config

        Returns
        -------

        """
        pass

    @abstractmethod
    def save_model(self, model_name, version, config):
        """
        保存模型
        Parameters
        ----------
        model_name
        version
        config

        Returns
        -------

        """
        pass

    @abstractmethod
    def load(self, model_name, version):
        """
        加载模型
        Parameters
        ----------
        model_name
        version

        Returns
        -------

        """
        pass

    @abstractmethod
    def load_config(self, model_name, version):
        """
        加载配置
        Parameters
        ----------
        model_name
        version

        Returns
        -------

        """

    @abstractmethod
    def load_data(self, model_name, version):
        """
        加载数据集
        Parameters
        ----------
        model_name
        version

        Returns
        -------

        """

    @abstractmethod
    def predict(self, x, y):
        """
        预测模型
        Parameters
        ----------
        x
        y

        Returns
        -------

        """
        pass

    @abstractmethod
    def evaluate(self, predict_value, real_value):
        """
        模型评价
        Parameters
        ----------
        predict_value
        real_value

        Returns
        -------

        """
        pass

    @abstractmethod
    def post_train(self, *args):
        """
        训练结束所要做的工作
        Parameters
        ----------
        args

        Returns
        -------

        """

class ModelConfig(metaclass=ABCMeta):
    """
    模型配置基类
    """

    def __init__(self, labels, target, train_partition, dataset):
        self._labels = labels
        self._target = target
        self._train_partition = train_partition
        self._dataset = dataset

    def get_labels(self):
        return self._labels

    def get_target(self):
        return self._target

    def get_train_partition(self):
        return self._train_partition

    def get_dataset(self):
        return self._dataset

    def get_config(self):
        config = {}
        config['数据集'] = self._dataset
        config['因子集合'] = self._labels
        config['目标收益率'] = self._target
        config['训练集分割'] = self._train_partition
        return config