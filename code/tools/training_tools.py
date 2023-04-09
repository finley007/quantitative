#! /usr/bin/env python
# -*- coding:utf8 -*-

import os
import numpy as np
import random
import xgboost
from memory_profiler import profile
from functools import lru_cache

from common.constants import XGBOOST_MODEL_PATH, CPU_CORE_NUMBER
from common.localio import read_decompress
from mlearn.xgboost.xgboost import XGBoostTrainer, XGBoostConfig
from mlearn.model import ModelTrainer
from mlearn.model_evaluation import BackTestEvaluator
from factor.spot_goods_factor import TenGradeCommissionRatioFactor, FiveGradeCommissionRatioFactor, FiveGradeCommissionRatioDifferenceFactor, TenGradeCommissionRatioDifferenceFactor
from factor.volume_price_factor import WilliamFactor, CloseMinusMovingAverageFactor
from framework.localconcurrent import ProcessExcecutor
from common.exception.exception import InvalidStatus
from common.log import get_logger


def analyze_training_data(data_identification):
    """
    分析待训练数据
    Parameters
    ----------
    data_identification

    Returns
    -------

    """
    data = read_decompress(XGBOOST_MODEL_PATH + os.path.sep + 'input' + os.path.sep + data_identification + '.pkl')
    date_list = list(set(list(map(lambda datetime : datetime[0:10], data['datetime'].tolist()))))
    print(data.columns)
    print(data.shape)
    date_list.sort()
    print(date_list)
    print(len(date_list) * 3/4)
    print(date_list[round(len(date_list) * 3/4)])

def  train_model(model_name, version, data_identification, model_trainer, config):
    """
    训练模型
    Parameters
    ----------
    model_name
    version
    data_identification
    model_trainer
    config

    Returns
    -------

    """
    data = read_decompress(XGBOOST_MODEL_PATH + os.path.sep + 'input' + os.path.sep + data_identification + '.pkl')
    model_trainer.train(model_name, version, data, config)

def return_analysis(model_name, version, product, start_date, end_date):
    """
    收益率分析
    Parameters
    ----------
    model_name
    version
    start_date
    end_date

    Returns
    -------

    """
    trainer = XGBoostTrainer()
    model = trainer.load(model_name, version)
    config = trainer.load_config(model_name, version)
    data = trainer.load_data(model_name, version)
    print(data.columns)
    data_train_x, data_train_y, data_test_x, data_test_y = trainer.prepare(data, config)
    predict_test_y = model.predict(data_test_x)
    data_train, data_test = ModelTrainer.prepare(trainer, data, config)
    data_test['predict_y'] = predict_test_y
    backtest_data = BackTestEvaluator(model_name, version, data_test, config, model).prepare_back_test_data(product)
    backtest_data = backtest_data[(backtest_data['datetime'] > start_date) & (backtest_data['datetime'] < end_date)]
    backtest_data.to_csv(XGBOOST_MODEL_PATH + os.path.sep + 'check' + os.path.sep + model_name + '_' + version + product + '_' + start_date + '_' + end_date + '.csv')

class ParameterTestCase():

    """
    测试用例描述类
    """

    def __init__(self, case_code, dataset, labels, targets, train_partitions, max_depths, n_estimators, learning_rates):
        """

        Parameters
        ----------
        case_code: string
        dataset: string
        labels: list
        targets: list
        train_partitions: list
        max_depths: list
        n_estimators: list
        learning_rates: list
        每一个list包含所有待测场景
        """
        self._case_code = case_code
        self._dataset = dataset
        self._labels = labels
        self._targets = targets
        self._train_partitions = train_partitions
        self._max_depths = max_depths
        self._n_estimators = n_estimators
        self._learning_rates = learning_rates

    def get_description(self):
        """
        用例明细描述
        Returns
        -------

        """
        return {
            'case' : self._case_code,
            'dataset' : self._dataset,
            'labels' : self._labels,
            'targets' : self._targets,
            'train_partitions' : self._train_partitions,
            'max_depths' : self._max_depths,
            'n_estimators' : self._n_estimators,
            'learning_rates' : self._learning_rates
        }

    def get_case_code(self):
        return self._case_code

    def get_dataset(self):
        return self._dataset
    def get_labels(self):
        return self._labels

    def get_targets(self):
        return self._targets

    def get_train_partitions(self):
        return self._train_partitions

    def create_xgboost_params_list(self):
        params_list = []
        for max_depth in self._max_depths:
            for n_estimator in self._n_estimators:
                for learning_rate in self._learning_rates:
                    params_list.append((max_depth, n_estimator, learning_rate))
        return params_list


def parameter_test(test_case):
    """
    测试模型参数
    Returns
    -------

    """
    case_code = test_case.get_case_code()
    case_path = XGBOOST_MODEL_PATH + os.path.sep + 'test' + os.path.sep + case_code
    dataset = test_case.get_dataset()
    if not os.path.exists(case_path):
        os.makedirs(case_path)
    else:
        raise InvalidStatus('The test case: {} has existed'.format(case_code))
    for label in test_case.get_labels():
        for target in test_case.get_targets():
            for partition in test_case.get_train_partitions():
                param_list = test_case.create_xgboost_params_list()
                param_list = list(map(lambda param: (case_code, dataset, label, target, partition, param[0], param[1], param[2]), param_list))
                # for param in param_list:
                #     test_model([param[0], param[1], param[2], param[3], param[4], param[5], param[6], param[7]])
                result = ProcessExcecutor(2).execute(test_model, param_list)

@profile
def test_model(*args):
    case_code = args[0][0]
    data_identification = args[0][1]
    labels = args[0][2]
    target = args[0][3]
    partition = args[0][4]
    max_depth = args[0][5]
    n_estimators = args[0][6]
    learning_rate = args[0][7]
    config = XGBoostConfig(labels, target, partition, data_identification, max_depth, n_estimators, learning_rate = learning_rate)
    data = get_data(data_identification)
    # 随机生成版本号
    version = round(random.uniform(0,10),2)
    data_train_x, data_train_y, data_test_x, data_test_y = prepare_training_data(data, config)
    model = xgboost.XGBRegressor(max_depth=config.get_max_depth(),
                                 base_score=0,
                                 learning_rate=config.get_learning_rate(),
                                 n_estimators=config.get_n_estimators(),
                                 objective=config.get_objective(),
                                 booster=config.get_booster,
                                 n_jobs=CPU_CORE_NUMBER,
                                 random_state=8).fit(data_train_x, data_train_y)
    predict_test_y = model.predict(data_test_x)
    data_train, data_test = prepare_data_partition(data, config)
    data_test['predict_y'] = predict_test_y
    data_path = XGBOOST_MODEL_PATH + os.path.sep + 'test' + os.path.sep + case_code + os.path.sep + 'data'
    if not os.path.exists(data_path):
        os.makedirs(data_path)
    fig_path = XGBOOST_MODEL_PATH + os.path.sep + 'test' + os.path.sep + case_code + os.path.sep + 'fig'
    if not os.path.exists(fig_path):
        os.makedirs(fig_path)
    data_file = data_path + os.path.sep + case_code + '_' + str(version) + '.pkl'
    fig_file = fig_path + os.path.sep + case_code + '_' + str(version) + '.png'
    BackTestEvaluator(case_code, str(version), data_test, config, model).evaluate(data_file, fig_file)
    # 清理内存
    del data, data_train_x, data_train_y, data_test_x, data_test_y, data_train, data_test


@lru_cache(maxsize=3)
def get_data(data_identification):
    return read_decompress(XGBOOST_MODEL_PATH + os.path.sep + 'input' + os.path.sep + data_identification + '.pkl')

def prepare_data_partition(data, config):
    train_data = data[data['datetime'] <= config.get_train_partition()]
    test_data = data[data['datetime'] > config.get_train_partition()]
    return train_data, test_data

def prepare_training_data(data, config):
    data_train, data_test = prepare_data_partition(data, config)
    data_train_y = data_train[config.get_target()]
    data_train_x = data_train[config.get_labels()]
    data_test_y = data_test[config.get_target()]
    data_test_x = data_test[config.get_labels()]
    return data_train_x, data_train_y, data_test_x, data_test_y

if __name__ == '__main__':
    # analyze_training_data('data_20230315')

    # 训练模型
    # labels = TenGradeCommissionRatioFactor().get_keys() + FiveGradeCommissionRatioFactor().get_keys()
    # labels = TenGradeCommissionRatioFactor().get_keys() + FiveGradeCommissionRatioFactor().get_keys() \
    #          + FiveGradeCommissionRatioDifferenceFactor([20, 50, 100, 200]).get_keys() + TenGradeCommissionRatioDifferenceFactor([20, 50, 100, 200]).get_keys()
    labels = FiveGradeCommissionRatioDifferenceFactor([20, 50, 100, 200]).get_keys() \
             + TenGradeCommissionRatioDifferenceFactor([20, 50, 100, 200]).get_keys() \
             + WilliamFactor([100, 200, 500, 1000, 2000, 5000]).get_keys()\
             + TenGradeCommissionRatioFactor().get_keys() \
             + FiveGradeCommissionRatioFactor().get_keys() \
             + CloseMinusMovingAverageFactor([200, 500, 1000, 1500]).get_keys()
    target = 'ret.10'
    train_partition = '2020-09-10' #训练：测试 3：1
    max_depth = 3
    n_estimators = 30
    learning_rate = 0.15
    config = XGBoostConfig(labels, target, train_partition, 'data_20230408', max_depth, n_estimators, learning_rate=learning_rate)
    train_model('INITIAL_MODEL', '0.6.3', 'data_20230408', XGBoostTrainer(), config)

    # 收益率分析
    # return_analysis('INITIAL_MODEL', '0.5', 'IC', '2022-06-13', '2022-06-17')

    # 超参测试
    # labels = [TenGradeCommissionRatioFactor().get_keys() + FiveGradeCommissionRatioFactor().get_keys() + WilliamFactor([100, 200, 500, 1000]).get_keys() + TenGradeCommissionRatioDifferenceFactor([20, 50, 100, 200]).get_keys() + FiveGradeCommissionRatioDifferenceFactor([20, 50, 100, 200]).get_keys()]
    # targets = ['ret.10']
    # train_partitions = ['2021-03-10']
    # max_depths = list(range(1, 9, 1))
    # n_estimators = list(range(5, 31, 5))
    # learning_rates = list(np.arange(0.1, 0.4, 0.1))
    # # 测试用例描述
    # # # print(ParameterTestCase('test1', labels, targets, train_partitions, max_depths, n_estimators, learning_rates).get_description())
    # # # print(ParameterTestCase('test1', labels, targets, train_partitions, max_depths, n_estimators, learning_rates).create_xgboost_params_list())
    # # 生成测试报告
    # parameter_test(ParameterTestCase('param_test_20230408_2', 'data_20230404', labels, targets, train_partitions, max_depths, n_estimators, learning_rates))
