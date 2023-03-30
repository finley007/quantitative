#! /usr/bin/env python
# -*- coding:utf8 -*-

import os

from common.constants import XGBOOST_MODEL_PATH
from common.localio import read_decompress
from mlearn.xgboost.xgboost import XGBoostTrainer, XGBoostConfig
from factor.spot_goods_factor import TenGradeCommissionRatioFactor, FiveGradeCommissionRatioFactor

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

def train_model(model_name, version, data_identification, model_trainer, config):
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

if __name__ == '__main__':
    # analyze_training_data('data_20230315')

    labels = [TenGradeCommissionRatioFactor().factor_code, FiveGradeCommissionRatioFactor.factor_code]
    target = 'ret.10'
    train_partition = '2021-03-10'
    max_depth = 5
    n_estimators = 20
    learning_rate = 0.2
    config = XGBoostConfig(labels, target, train_partition, 'data_20230325', max_depth, n_estimators, learning_rate=learning_rate)
    train_model('INITIAL_MODEL', '0.2', 'data_20230325', XGBoostTrainer(), config)