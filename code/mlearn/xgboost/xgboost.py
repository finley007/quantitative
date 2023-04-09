#! /usr/bin/env python
# -*- coding:utf8 -*-

import xgboost
import os
from scipy.stats import pearsonr

from mlearn.model import ModelTrainer, ModelConfig
from mlearn.model_evaluation import WinningRateEvaluator, CorrelationEvaluator, LossFunctionEvaluator, BackTestEvaluator
from common.constants import CPU_CORE_NUMBER, XGBOOST_MODEL_PATH
from common.localio import save_compress, read_decompress, FileWriter, read_txt
from common.aop import timing
from common.visualization import draw_line
from common.log import get_logger
from common.persistence.dao import FutureInstrumentConfigDao


class XGBoostTrainer(ModelTrainer):
    """
    XGBoost 训练器
    """

    def prepare(self, data, config):
        get_logger().info('Start data preparation')
        data_train, data_test = ModelTrainer.prepare(self, data, config)
        data_train_y = data_train[config.get_target()]
        data_train_x = data_train[config.get_labels()]
        data_test_y = data_test[config.get_target()]
        data_test_x = data_test[config.get_labels()]
        return data_train_x, data_train_y, data_test_x, data_test_y

    @timing
    def train(self, model_name, version, data, config, for_test = False):
        get_logger().info('Begin to train model: {0}.{1}'.format(model_name, version))
        #生成划分训练集
        data_train_x, data_train_y, data_test_x, data_test_y = self.prepare(data, config)
        #训练模型
        self._model = xgboost.XGBRegressor(max_depth=config.get_max_depth(),
                              base_score=0,
                              learning_rate=config.get_learning_rate(),
                              n_estimators=config.get_n_estimators(),
                              objective=config.get_objective(),
                              booster=config.get_booster,
                              n_jobs=CPU_CORE_NUMBER,
                              random_state=8).fit(data_train_x, data_train_y)
        #获取测试集预测结果，评估模型
        predict_test_y = self.predict(data_test_x)
        self.evaluate(predict_test_y, data_test_y)
        #回测
        data_train, data_test = ModelTrainer.prepare(self, data, config)
        data_test['predict_y'] = predict_test_y
        # 测试不保存模型
        if not for_test:
            self.save_model(model_name, version, config)
        self.post_train(model_name, version, data_test, config)

    def save_model(self, model_name, version, config):
        # 保存模型
        save_compress(self._model, XGBOOST_MODEL_PATH + os.path.sep + model_name + '_' + version + '.pkl')
        # 保存配置
        file_writer = FileWriter(XGBOOST_MODEL_PATH + os.path.sep + model_name + '_' + version + '.cfg')
        file_writer.write_file_line(str(config.get_config()))
        file_writer.close_file()

    def load(self, model_name, version):
        return read_decompress(XGBOOST_MODEL_PATH + os.path.sep + model_name + '_' + version + '.pkl')

    def load_config(self, model_name, version):
        map_config = eval(read_txt(XGBOOST_MODEL_PATH + os.path.sep + model_name + '_' + version + '.cfg'))
        config = XGBoostConfig(map_config['因子集合'], map_config['目标收益率'], map_config['训练集分割'], map_config['数据集'], map_config['xgboost配置参数']['max_depth'], map_config['xgboost配置参数']['n_estimators'], map_config['xgboost配置参数']['objective'], map_config['xgboost配置参数']['booster'], map_config['xgboost配置参数']['learning_rate'])
        return config

    def load_data(self, model_name, version):
        dataset_code = self.load_config(model_name, version).get_dataset()
        return read_decompress(XGBOOST_MODEL_PATH + os.path.sep + 'input' + os.path.sep + dataset_code + '.pkl')

    def predict(self, x):
        get_logger().info('Start model prediction')
        return self._model.predict(x)

    def evaluate(self, predict_y, y):
        get_logger().info('Start model evaluation')
        # 计算胜率
        print('胜率：' + str(WinningRateEvaluator().evaluate(predict_y, y)))
        # 计算相关性
        print('相关性：' + str(CorrelationEvaluator().evaluate(predict_y, y)))
        # 计算损失
        print('损失MSE：' + str(LossFunctionEvaluator().evaluate(predict_y, y)))


    def post_train(self, model_name, version, data, config):
        # 回测
        BackTestEvaluator(model_name, version, data, config, self._model).evaluate()

    def draw_diagram(self, data, config):
        data_x = data[config.get_labels()]
        data['predict_y'] = self._model.predict(data_x)
        data['y'] = data[config.get_target()]
        partition_date = config.get_train_partition()
        data = data[['datetime', 'predict_y', 'y']]
        print(data[['datetime', 'predict_y', 'y']])
        train_data = data[data['datetime'] <= partition_date]
        test_data = data[data['datetime'] > partition_date]
        print(pearsonr(train_data['predict_y'], train_data['y']))
        print(pearsonr(test_data['predict_y'], test_data['y']))
        future_instrument_config_dao = FutureInstrumentConfigDao()
        train_start_date = train_data['datetime'].tolist()[0][0:10]
        train_end_date = future_instrument_config_dao.get_next_n_transaction_date(train_start_date, 10)
        test_start_date = config.get_train_partition()
        test_end_date = future_instrument_config_dao.get_next_n_transaction_date(test_start_date, 10)
        train_data_draw_line = train_data[
            (train_data['datetime'] > train_start_date) & (train_data['datetime'] < train_end_date)]
        draw_line(train_data_draw_line, 'Train Data', 'Time', 'Ret',
                  {'x': 'datetime', 'y': [{'key': 'predict_y', 'label': 'Predict_Ret'}]},
                  save_path='E:\\data\\temp\\train_data_draw_line_predict.png')
        draw_line(train_data_draw_line, 'Train Data', 'Time', 'Ret',
                  {'x': 'datetime', 'y': [{'key': 'y', 'label': 'Real_Ret'}]},
                  save_path='E:\\data\\temp\\train_data_draw_line_real.png')
        test_data_draw_line = test_data[
            (test_data['datetime'] > test_start_date) & (test_data['datetime'] < test_end_date)]
        draw_line(test_data_draw_line, 'Test Data', 'Time', 'Ret',
                  {'x': 'datetime', 'y': [{'key': 'predict_y', 'label': 'Predict_Ret'}]},
                  save_path='E:\\data\\temp\\test_data_draw_line_predict.png')
        draw_line(test_data_draw_line, 'Test Data', 'Time', 'Ret',
                  {'x': 'datetime', 'y': [{'key': 'y', 'label': 'Real_Ret'}]},
                  save_path='E:\\data\\temp\\test_data_draw_line_real.png')


class XGBoostConfig(ModelConfig):
    """
    XGBoost 配置

    Parameters
        ----------
        labels: list 因子集合
        target: string 目标回报率
        train_partition: list 训练集划分
        dataset: 数据集
        # XGBoost模型参数
        max_depth
        n_estimators
        objective
        booster
        learning_rate
    """

    def __init__(self, labels, target, train_partition, dataset, max_depth, n_estimators, objective='reg:squarederror', booster='gbtree', learning_rate=0.2):
        ModelConfig.__init__(self, labels, target, train_partition, dataset)
        self._objective = objective
        self._booster = booster
        self._max_depth = max_depth
        self._learning_rate = learning_rate
        self._n_estimators = n_estimators

    def get_max_depth(self):
        return self._max_depth

    def get_n_estimators(self):
        return self._n_estimators

    def get_objective(self):
        return self._objective

    def get_booster(self):
        return self._booster

    def get_learning_rate(self):
        return self._learning_rate

    def get_config(self):
        config = ModelConfig.get_config(self)
        xgboost_config = {}
        xgboost_config['max_depth'] = self._max_depth
        xgboost_config['n_estimators'] = self._n_estimators
        xgboost_config['objective'] = self._objective
        xgboost_config['booster'] = self._booster
        xgboost_config['learning_rate'] = self._learning_rate
        config['xgboost配置参数'] = xgboost_config
        return config


if __name__ == '__main__':
    # xgboost_trainer = XGBoostTrainer()
    # xgboost_trainer.prepare('')

    xgboost_config = XGBoostConfig(['factor1','factor2'], 'ret20', '20170101-20201231',10,10)
    print(xgboost_config.get_config())
