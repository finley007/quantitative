import config as config
import common_func as func_lib
import pandas as pd

class QuotData:
    """
        行情基础数据
        包含字段 高，开，低，收，时间，成交量，持仓量
        ['datetime', 'open', 'high', 'low', 'close', 'volume', 'open_interest']
    """
    def __init__(self,cycle:str ='5m'):
        """
            base_quot_path  基础行情数据
            quot_ret_path 计算了 ret的行情数据
            filter_limit_path 删除了 涨跌停的行情数据
        :param cycle:
        """
        self.path = config.QUOT_DATA_PATH+cycle+'\\'
        self.base_quot_path = self.path + 'base_quot\\'
        self.quot_ret_path = self.path  +'quot_ret\\'
        self.filter_limit_path = self.path + 'filter_limit\\'
        func_lib.mkdir(self.base_quot_path)
        func_lib.mkdir(self.quot_ret_path)
        func_lib.mkdir(self.filter_limit_path)

    def count_base_data_ret(self,ret:int = 24):
        """
        基于基础行情计算未来多少周期收益率
        :param ret: int 默认24
        :return: none 保存计算后的文件
        """
        product_list = sorted(func_lib.read_files_in_path(self.base_quot_path))
        for product in product_list:
            product_file_path = self.base_quot_path + product
            product_data = pd.read_pickle(product_file_path)
            product_data['ret.'+str(ret)] = (product_data.shift(-ret)['close'] - product_data['close']) * 100 / product_data[
                'close']
            product_data.set_index(['datetime'],inplace=True)
            print(product_data)
            func_lib.dump_data(product_data, self.quot_ret_path+product)

    def count_base_data_ret_list(self,ret_list):
        """
        基于基础行情计算未来多少周期收益率
        :param ret_list: list[int]
        :return: none 保存计算后的文件
        """
        product_list = sorted(func_lib.read_files_in_path(self.base_quot_path))
        for product in product_list:
            product_file_path = self.base_quot_path + product
            product_data = pd.read_pickle(product_file_path)
            for ret in ret_list:
                product_data['ret.'+str(ret)] = (product_data.shift(-ret)['close'] - product_data['close']) * 100 / product_data[
                    'close']
            product_data.set_index(['datetime'], inplace=True)
            print(product_data)
            func_lib.dump_data(product_data, self.quot_ret_path+product)

    def get_merge_product_ret(self,ret:int = 24,type_value:int =1):
        """
          按品种顺序 合并ret (ret需要提前计算好)
        :param ret:
        :param type_value: int  1 选择使用 过滤掉涨跌停的  2 其他选择使用正常RET的
        :return:
        """
        path = self.filter_limit_path
        if type_value != 1:
            path = self.quot_ret_path
        product_files = sorted(func_lib.read_files_in_path(path))
        merge_df = pd.DataFrame()

        for one_product_file in product_files:
            data = pd.read_pickle(path+one_product_file)
            data['product'] = one_product_file[2:-4]
            merge_df= pd.concat([merge_df,data[['ret.'+str(ret),'product']]],axis=0)
        print(merge_df)
        return merge_df

    def filter_daily_limit(self,ret:int = 24):
        """
           去掉涨跌停那天的数据
           涨跌停判断标准 : high == low  并且 vol > 100
        :return:
        """
        #各品种 循环进行去涨跌停数据
        product_files = sorted(func_lib.read_files_in_path(self.quot_ret_path))
        total_num = 0
        merge_df = pd.DataFrame()
        for one_product_file in product_files:
            print(one_product_file)
            product_data = pd.read_pickle(self.quot_ret_path + one_product_file)
            product_data = product_data.reset_index()
            product_data['date'] = product_data['datetime'].apply(lambda x: x.strftime('%Y-%m-%d'))
            product_data = product_data.set_index("datetime")
            product_data['high_equal_low'] = product_data['high'] == product_data['low']
            # product_data['shift_high_equal_shift_low'] = (product_data['high_equal_low'].shift(1)==True) | (product_data['high_equal_low'].shift(-1)==True)

            product_data['shift_two_high_equal_shift_low'] = ((product_data['high_equal_low'].shift(2)==True) &(product_data['high_equal_low'].shift(1)==True)) \
                                                             | ((product_data['high_equal_low'].shift(-1)==True)&(product_data['high_equal_low'].shift(1)==True))|\
                                                             ((product_data['high_equal_low'].shift(-1)==True)&(product_data['high_equal_low'].shift(-2)==True))
            product_data['daily_limit'] = (product_data['high_equal_low']==True)& (product_data['shift_two_high_equal_shift_low']==True)&(product_data['volume']>100)
            product_data['delete'] = 0

            delete_date = set(product_data['date'][product_data['daily_limit']==True])
            print(delete_date)
            print(product_data[product_data['date'].isin(delete_date)])
            product_data['delete'][product_data['date'].isin(delete_date)] = 1


            # 保存下来
            func_lib.dump_data(product_data[product_data['delete']==0].drop(['date','high_equal_low','shift_two_high_equal_shift_low','daily_limit','delete'],axis=1),self.filter_limit_path + one_product_file)


            total_num += len(product_data[product_data['delete']==1])
            merge_df = merge_df.append(product_data[product_data['delete']==0][['ret.'+str(ret)]])
            # print(product_data[product_data['daily_limit']==True][200:]) #20577 - 8502 - 8046 7002

        print(total_num) # 57151
        print(merge_df)
        print(len(merge_df))# 6082400
        return merge_df

# QuotData().filter_daily_limit()

# QuotData().count_base_data_ret()
# QuotData().get_merge_product_ret()

