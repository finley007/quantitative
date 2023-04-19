from data.train_data import TrainData
from data.factor import Factor
from data.quot_data import QuotData
import datetime

def get_train_data():
    print(TrainData().get_factor_total_data())


def describe(factor_name_list,cycle:str = '5m'):
    """
        多个因子 导出describe
    :param factor_name_list:
    :param cycle:
    :return:
    """
    for factor_name in factor_name_list:
        Factor(factor_name,cycle).describe()


def plot_multi_factor_mean_and_std(factor_name_list,cycle:str = '5m'):
    """
        多个因子画 平均值和标准差
    :param factor_name_list:
    :return: NONE
    """
    for factor_name in factor_name_list:
        Factor(factor_name,cycle).plot_factor_std_and_mean_by_day()

def plot_multi_factor_ic(factor_name_list, cycle: str = '5m'):
    """
        多个因子画 相关性
    :param factor_name_list:
    :return: NONE
    """
    total_start = datetime.datetime.now()
    for factor_name in factor_name_list:
        start = datetime.datetime.now()
        Factor(factor_name, cycle).plot_factor_ic_by_day()
        end = datetime.datetime.now()
        print(factor_name +'计算完毕 耗时 : '+ str(end-start))
    total_end = datetime.datetime.now();
    print(str(len(factor_name_list)) + '个因子计算完毕 耗时 : ' + str(total_end - total_start))


def export_multi_factor_describe(factor_name_list, cycle: str = '5m'):
    """
        多个因子导出describe
    :param factor_name_list:
    :return: NONE
    """
    print(factor_name_list)
    for factor_name in range(0,len(factor_name_list)):
        print(factor_name)
        Factor(factor_name_list[factor_name], cycle).describe()

def create_multi_factor_base_data(factor_name_list,cycle:str = '5m',ret:int = 24):
    """
    创建大表
    :param factor_name_list: 因子数组
    :param cycle:  周期
    :param ret: 收益率
    :return:
    """
    TrainData(cycle).create_base_train_data(factor_name_list,ret)

def create_base_train_data_ret_list(factor_name_list,ret_list,cycle:str = '5m'):
    """
    创建大表 多個RET
    :param factor_name_list: 因子数组
    :param ret_list: 收益率
    :param cycle:  周期
    :return:
    """
    TrainData(cycle).create_base_train_data_ret_list(factor_name_list,ret_list)



def create_multi_factor_filter_base_data(factor_name_list,cycle:str = '5m',ret:int = 24):
    TrainData(cycle).create_filter_daily_limit_data(factor_name_list, ret)
