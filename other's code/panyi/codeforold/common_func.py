import time

import pandas as pd
import numpy as np
import gzip
import _pickle as cPickle
import joblib
from sklearn.preprocessing import StandardScaler
from xgboost import plot_tree
import os
from scipy import stats
from pandas.plotting import table
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

import pickle


pd.set_option('display.max_columns', 10)  #最多显示5列
pd.set_option('display.float_format',lambda x: '%.6f'%x) #两位
pd.set_option('display.width', 1000)

def sma(df, window=10):
    return df.rolling(window).mean()

def stddev(df, window=10):
    return df.rolling(window).std()

def ts_sum(df, window=10):
    return df.rolling(window).sum()

def ts_min(df, window=10):
    return df.rolling(window).min()

def ts_max(df, window=10):
    return df.rolling(window).max()

# MAD绝对中位值——去离群点
def mad(factor):
    me = factor.median()
    print('me', me)
    mad = abs(factor - me).mean()
    up =me+ (5 * 1.4826 * mad)
    down = me-(5 * 1.4826 * mad)
    num = np.where((factor > up) | (factor < down), 1, 0)
    factor = np.where(factor > up, up, factor)
    factor = np.where(factor < down, down, factor)
    print('总数 : ', len(factor))
    print('过滤极值数 : ', num.sum())
    print('收缩极值占比:  ',num.sum()/len(factor))
    return factor

# MAD绝对中位值——去离群点
def mad_min_value(factor):
    me = factor.mean()
    print('me', me)
    mad = abs(factor - me).mean()
    up = (0.01 * 1.4826 * mad)
    down = -(0.01 * 1.4826 * mad)
    num = np.where((factor < up) & (factor > down), 1, 0)
    factor = np.where((factor < up) & (factor > down), 0, factor)
    
    print('总数 : ', len(factor))
    print('过滤极值数 : ', num.sum())
    print('收缩极值占比:  ',num.sum()/len(factor))
    return factor

# 归一化
def normalization(factor, factor_train_period):
    return zero_divide((factor - ts_min(factor, factor_train_period)), (
                ts_max(factor, factor_train_period) - ts_min(factor, factor_train_period)))

def zero_divide(x, y): 
    index = x.index
    a = np.array(x,dtype=np.float64)
    b = np.array(y,dtype=np.float64)
    res = np.divide(a,b,out=np.zeros_like(a), where=b!=0) 
    c = pd.DataFrame(res,index=index,columns=['A'])
    return c['A']

#标准化
def standard(x_train):
    ss_x = StandardScaler().fit(x_train)
    joblib.dump(ss_x,'D:\\stand_fit.pkl')
    return pd.DataFrame(ss_x.transform(x_train))

#测试集标准化
def standard_cs(x_test):
    ft = joblib.load('D:\\stand_fit.pkl')
    return pd.DataFrame(ft.transform(x_test))

def load(path):
    with gzip.open(path, 'rb', compresslevel=1) as file_object:
        raw_data = file_object.read()
    return cPickle.loads(raw_data)

# 保存文件
def save(data, path):
    serialized = cPickle.dumps(data)
    with gzip.open(path, 'wb', compresslevel=1) as file_object:
        file_object.write(serialized)

def save(data, path):
    serialized = cPickle.dumps(data)
    with gzip.open(path, 'wb', compresslevel=1) as file_object:
        file_object.write(serialized)


# 读取文件夹下面的所有文件和文件夹
def  read_files_in_path(path):
    all_files = list(map(lambda x: x, list(set(os.listdir(path)) - set('.DS_Store'))))
    return all_files


# 画树 
def tree_plot(path):
    ft=joblib.load(path)
    print(ft)
    print(ft.n_estimators)
    path = 'D:\\data\\weisoft\\tree_pic\\'
    if not os.path.exists(path):
        os.makedirs(path)

    for i in range(0,ft.n_estimators):
        plot_tree(ft,num_trees=i)
        fig = plt.gcf()
        fig.set_size_inches(100, 50)
        fig.savefig(path + 'tree_'+str(i)+'_.png')


# 基于零轴的归一化
def normalizationByZero(factor_series_data):
    distance = max(factor_series_data.max() - 0, 0 - factor_series_data.min())
    return zero_divide(factor_series_data, 2 * distance)

#画多数据折线图 （目前针对因子 标准差和平均值 空了可优化）
def plot_more_subplot_line_chart(plot_data_one,plot_data_two,title_list,file_path,product_name):
    #画布
    fig = plt.figure(figsize=(12,10))
    #子图
    ax1 = fig.add_subplot(211)
    ax1.plot(plot_data_one)
    ax1.set_title(title_list[0])

    ax2 = fig.add_subplot(212)
    ax2.plot(plot_data_two)
    ax2.set_title(title_list[1])

    if not os.path.exists(file_path):
        os.makedirs(file_path)
    fig.savefig(file_path+ product_name +'.png')

    plt.close(fig)
    time.sleep(0.01)

def clear_plot():
    plt.clf()

#加载模型
def load_model(path):
	model = joblib.load(path)
	print(path)
	print(model)
	return model

def dump_model(model,path):
    print(path)
    joblib.dump(model,path)
    print(path + ' 模型保存完毕 ===========')

# 导出EXCEL
def to_excel(df,save_path):
    with pd.ExcelWriter(save_path, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='sheet_name')
    print('excel : ' + save_path + '  is exported .')

# 保存未压缩文件
def dump_data(df,save_file_path):
    f = open(save_file_path, 'wb')
    pickle.dump(df, f)

# 创建文件夹
def mkdir(path):
    if not os.path.exists(path):
        os.makedirs(path)
#清除空格
def clear_space(str_list):
    str_list = []
    for str in str_list:
        str_list.append("".join(str.split()))
    print(str_list)

def get_kurtosis(serise_data):
    """
        求峰度
    :param serise_data:
    :return:
    """
    return stats.kurtosis(serise_data)

def get_skew(serise_data):
    """
        求偏度
    :param serise_data:
    :return:
    """
    return stats.skew(serise_data)
