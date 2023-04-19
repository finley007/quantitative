import multiprocessing
import xgboost as xgb
import common_func as func_lib
import numpy as np
import joblib
import pandas as pd
import os
import pickle as pickle
import config


class Model:

	def __init__(self,cycle:str='5m',ret:int=24):
		self.cycle = cycle
		self.ret = ret


	def train_model(self,data, start_date, end_date,signal_name, depth, tree_num):
		"""
		:param data: 数据
		:param start_date: 训练集开始
		:param end_date: 训练集结束
		:param signal_name: 信号源 用于区分存放路径
		:param depth: 深度
		:param tree_num: 树的棵树
		:return:
		"""
		print('=================== train_model start ==========================')
		# 删除成交额小于100亿的行
		# print('刪除成交額行前 :' + str(len(data)))
		# data = data[(data['FCT_amount-20'] > 50)]
		# print('刪除成交額行后 :' + str(len(data)))

		# 删除成交额列
		# data = data.drop(labels='FCT_amount-20', axis=1)
		data.replace([np.inf, -np.inf], np.nan)
		data = data.dropna()
		data = data[(data.index >= pd.Timestamp(start_date)) & (data.index <= pd.Timestamp(end_date))]
		# 数据倒置
		print(len(data))
		print(data.columns)
		data2 = data * -1
		not_reverse_name_list = ['FCT_ATR', 'FCT_VOL', 'FCT_ATR_MA']
		not_reverse_columns = []
		for i in data2.columns:
			for j in not_reverse_name_list:
				if j in i:
					not_reverse_columns.append(i)
		print(not_reverse_columns)
		data2[not_reverse_columns] = data2[not_reverse_columns] * -1;
		data = data.append(data2)
		print('后', len(data))

		# x   	 y
		y = pd.DataFrame()
		y['ret.'+ str(self.ret)] = data['ret.' + str(self.ret)]
		data.drop(['ret.'+ str(self.ret), 'product'], axis=1, inplace=True)

		n_job = multiprocessing.cpu_count()
		ft = xgb.XGBRegressor(max_depth=depth,  # 深度 6  - 7 - 8   9
							  base_score=0,
							  learning_rate=0.1,  # 0.0001 - 0.1   10倍 提升
							  # 设置较小的 eta 就可以多学习几个弱学习器来弥补不足的残差 推荐 [0.01, 0.015, 0.025, 0.05, 0.1] 0.1 可减少迭代用时
							  n_estimators=tree_num,  # 树的棵数 150 -  200 #  50 - 150  50 提升
							  objective='reg:linear',  # 此默认参数与 XGBClassifier 不同
							  booster='gbtree',  # 推荐 gbtree
							  # importance_type='gain',
							  n_jobs=n_job,
							  random_state=8).fit(data, y)  # 随机种子

		model_folder_path = config.MODEL_PATH+signal_name+'\\'
		func_lib.mkdir(model_folder_path)

		joblib.dump(ft,model_folder_path + start_date + "_" + end_date + '_type_' + self.cycle + '_' + str(signal_name) + "_model.pkl")
		print('=================== train_model end ==========================')

	# 加载模型
	def load_model(self ,start_date, end_date, cycle, signal_name):
		path = config.MODEL_PATH+signal_name+'\\'+ start_date + '_' + end_date + '_type_' + cycle + '_' + str(
			signal_name) + '_model.pkl'
		model = joblib.load(path)
		print(path)
		print(model)
		return model

	def new_predict_data(self,start_date, end_date, predict_start, predict_end, signal_name,data):
		"""
		:param start_date:  训练集开始时间
		:param end_date: 训练集结束时间   用来确定模型
		:param predict_start: 预测集开始时间
		:param predict_end:预测集结束时间
		:param signal_name: 信号源
		:param data: 数据
		:return:
		"""
		model = self.load_model(start_date, end_date, self.cycle, signal_name)
		data = data[(data.index>=predict_start)&(data.index<=predict_end)]
		#删除成交额列
		# data = data.drop(labels='FCT_amount-20', axis=1)
		print(data)
		print(set(list(data['product'])))
		temp = pd.DataFrame()
		temp['product'] = data['product']
		temp.index = data.index
		temp.index = temp.index + pd.Timedelta(hours=4)

		data = data.drop(['ret.'+str(self.ret),'product'],axis=1)
		temp['signal'] = model.predict(data)

		product_list = set(list(temp['product']))
		for product in product_list:
			save_path = config.SIGNAL_PATH + self.cycle + '\\' + str(signal_name) + '\\'
			func_lib.mkdir(save_path)

			predict_path = save_path + product
			func_lib.mkdir(predict_path)

			final_path = predict_path + '\\' + product + '-' + predict_start + '-' + predict_end + '-predict-signal.pkl'
			product_data = temp[temp['product']==product]
			f = open(final_path, 'wb')
			pickle.dump(product_data, f)

	# 合并生成好的 各年份信号值
	def merge_predict_data(self, signal_name):
		"""
		:param start_date:  训练集开始时间
		:return:
		"""
		print('=================== merge_predict_data start ==========================')
		path = config.SIGNAL_PATH + self.cycle + '\\' + signal_name + '\\'
		files = func_lib.read_files_in_path(path)
		for i in files:
			print(i)
			file_path = path + '\\' + i
			merge_file = func_lib.read_files_in_path(file_path)
			merge_file = sorted(merge_file)
			result = pd.DataFrame()
			for j in merge_file:
				mmerge_file_path = file_path + '\\' + j
				print(mmerge_file_path)
				data = pd.read_pickle(mmerge_file_path)
				if result.empty:
					result = data
				else:
					result = result.append(data)
			# print(result)
			predict_path = config.PREDICT_MERGED_PATH + self.cycle + '\\' + signal_name + '\\'
			func_lib.mkdir(predict_path)
			final_path = predict_path + '\\' + i + '-predict-signal.pkl'
			f = open(final_path, 'wb')
			pickle.dump(result, f)
		print('=================== merge_predict_data end ==========================')