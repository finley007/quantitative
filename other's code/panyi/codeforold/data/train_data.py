import pandas as pd
from data.factor import Factor
from data.quot_data import  QuotData
import common_func as func_lib
import config as config
class TrainData:
    """
        模型训练数据
    """
    def __init__(self,cycle:str = '5m'):
        self.cycle = cycle
        self.path  = config.FACTOR_TOTAL_PATH +self.cycle+'\\'
        self.file_path = self.path +'total_result.pkl'
        self.filter_file_path = self.path + 'total_filter_result.pkl'
        func_lib.mkdir(self.path)


    def create_base_train_data(self,factor_name_list,ret:int=24):
        """
        创建包含所有因子的大表数据
        :param factor_name_list:
        :return: NONE
        """
        result_df = pd.DataFrame()
        for factor_name in factor_name_list:
            factor_data = Factor(factor_name,self.cycle).get_merge_product_factor_data()
            result_df[factor_data.columns] = factor_data
            print(factor_data)
        print(result_df)
        #合并 ret
        result_df['ret.'+str(ret)] = QuotData(self.cycle).get_merge_product_ret(ret,2)['ret.'+str(ret)]

        # result_df['product_b'] = QuotData().get_merge_product_ret(ret)['product'] #验证x y 是否对齐
        print(result_df)

        #转换为float32
        product = result_df['product']
        result_df.drop(['product'],axis=1,inplace=True)
        result_df=result_df.astype('float32')
        result_df['product'] = product
        # print(result_df[result_df['product']!=result_df['product_b']]) #若返回空 则表示对齐了
        #存放大文件
        func_lib.save(result_df,self.file_path)

    def create_base_train_data_ret_list(self,factor_name_list,ret_list):
        """
        创建包含所有因子的大表数据 多個ret
        :param factor_name_list:
        :return: NONE
        """
        result_df = pd.DataFrame()
        for factor_name in factor_name_list:
            factor_data = Factor(factor_name,self.cycle).get_merge_product_factor_data()
            result_df[factor_data.columns] = factor_data
            print(factor_data)
        print(result_df)

        #合并 ret
        for ret in ret_list:
            result_df['ret.'+str(ret)] = QuotData(self.cycle).get_merge_product_ret(ret,2)['ret.'+str(ret)]

        # result_df['product_b'] = QuotData().get_merge_product_ret(ret)['product'] #验证x y 是否对齐
        print(result_df)

        #转换为float32
        product = result_df['product']
        result_df.drop(['product'],axis=1,inplace=True)
        result_df=result_df.astype('float32')
        result_df['product'] = product
        # print(result_df[result_df['product']!=result_df['product_b']]) #若返回空 则表示对齐了
        print(result_df.dtypes)
        #存放大文件
        func_lib.save(result_df,self.file_path)

    def get_factor_total_data(self):
        """
         获取大表数据
        :return: Dataframe 大表数据
        """
        return func_lib.load(self.file_path)

    def create_filter_daily_limit_data(self,factor_name_list,ret:int=24):
        result_df = pd.DataFrame()
        for factor_name in factor_name_list:
            factor_data = Factor(factor_name).get_filter_daily_limit_merged_data()
            result_df[factor_data.columns] = factor_data
            print(result_df)

        # 合并 ret
        result_df['ret.' + str(ret)] = QuotData().get_merge_product_ret(ret, 1)['ret.' + str(ret)]
        # result_df['product_b'] = QuotData().get_merge_product_ret(ret)['product'] #验证x y 是否对齐

        print(result_df)
        # print(result_df[result_df['product']!=result_df['product_b']]) #若返回空 则表示对齐了

        # 存放大文件
        func_lib.save(result_df, self.filter_file_path)

    def add_column(self,old_file_path,factor_name_list):
        data = func_lib.load(old_file_path)

        for factor_name in factor_name_list:
            factor_data = Factor(factor_name).get_merge_product_factor_data()
            data[factor_data.columns] = factor_data

        # 存放大文件
        func_lib.save(data, self.file_path)
        print(data.columns)


# factor_name_list = ['FCT_Jump_2_1','FCT_Jump_2_2','FCT_Jump_2_3']
# # TrainData().create_base_train_data(factor_name_list)
#
# TrainData().add_column('D:\\data\\weisoft\\factor_total\\5m\\total_result_old.pkl',factor_name_list)






