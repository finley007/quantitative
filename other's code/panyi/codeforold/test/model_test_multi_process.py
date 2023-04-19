from model.model import *
from data.quot_data import QuotData
from model.model import Model
from data import create_base_train_data_ret_list
from multiprocessing.pool import Pool

def get_str_ret_list(ret_list):
	str_ret_list = []
	for ret in ret_list:
		str_ret_list.append('ret.'+str(ret))
	print('str_ret : ' + str(str_ret_list))
	return str_ret_list

def run_task(ret_list,ret,data,cycle):
	str_ret_list = get_str_ret_list(ret_list)
	str_ret_list.remove("ret." + str(ret))
	print('需要drop掉的 ret : ', str(str_ret_list))
	process_data = data.drop(str_ret_list, axis=1)
	#print(process_data)
	start_date = ['2012-01-01', '2012-01-01', '2012-01-01', '2012-01-01']
	end_date = ['2014-12-31', '2016-12-31', '2018-12-31', '2020-12-31']

	predict_start = ['2015-01-01', '2017-01-01', '2019-01-01', '2021-01-01']
	predict_end = ['2016-12-31', '2018-12-31', '2020-12-31', '2022-11-01']

	m = Model(cycle, ret)
	signal_name = 'ret_' + str(ret)
	depth = 7
	tree_num = 200
	print('=================' + signal_name + '-------' + str(depth) + '--------------' + str(tree_num) + '=======================')
	for i in range(0, len(start_date)):
		m.train_model(process_data, start_date[i], end_date[i], signal_name, depth, tree_num)
		m.new_predict_data(start_date[i], end_date[i], predict_start[i], predict_end[i], signal_name, process_data)

	m.merge_predict_data(signal_name)


if __name__  == '__main__':
	# 需要合并的因子
	factor_name_list = [
	    'FCT_arbr_br_new_modify_vol_2','FCT_BIAS_SIGNAL_TWO_LEVEL_2','FCT_BOLL_TOUJ_2','FCT_Camarilla_3_2','FCT_DEMARK_2'
		,'FCT_DONCHIAN_TJ_2','FCT_DONCHIAN_TWO_LEVEL_2','FCT_TSI_2'
		# FCT_SUPPORT_CLOSE_Thr_boll_2','FCT_amount'
	]

	# factor_name_list = [
	# 	'FCT_arbr_br_new_modify_vol_2'
	# ]

	# factor_name_list = [
	# 	'FCT_arbr_br_new_modify_vol_2','FCT_amount'
	# ]

	ret_list = [24]
	cycle = '5m'
	qd = QuotData(cycle)
	#生成收益率
	qd.count_base_data_ret_list(ret_list)
	#创建大表
	create_base_train_data_ret_list(factor_name_list,ret_list,cycle)
	#读取大表数据
	data = func_lib.load('D:\\data\\weisoft\\factor_total\\'+ cycle +'\\total_result.pkl')
	columns = data.columns

	p =Pool(3)

	for ret in ret_list:
		run_task(ret_list,ret,data,cycle)
	# 	p.apply_async(run_task,(ret_list,ret,data,cycle))
	# p.close()w
	# p.join()
	print('運行結束')
