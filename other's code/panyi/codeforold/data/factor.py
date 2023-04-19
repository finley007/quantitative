import time
import datetime
import pandas as pd
import common_func as func_lib
import config as config
from concurrent.futures import ThreadPoolExecutor

class Factor:
    """
        # 因子类 执行因子的操作
        args:
            factor_name 因子名
            cycle 所在周期级别 1m 5m 10m等
    """
    def __init__(self,factor_name:str,cycle:str = '5m'):
        self.factor_name = factor_name
        self.cycle = cycle
        # 因子存放位置
        func_lib.mkdir(config.FACTOR_PATH)

        self.path = config.FACTOR_PATH +  factor_name +'\\' + cycle+'\\'
        self.filter_limit_path = config.FACTOR_FILTER_PATH+factor_name+'\\'+cycle+'\\'

        self.periods = self.get_factor_period_columns()
        self.count_plot_period()


    def count_plot_period(self):
        '''
            计算要画图的周期
        :return:
        '''
        len(self.periods)
        print(len(self.periods))
        period_list = []
        period_list.append(self.periods[0])
        if len(self.periods) > 1:
            period_list.append(self.periods[-1])
        if len(self.periods) > 2:
            print(self.periods[int(len(self.periods) / 2)])
            period_list.append(self.periods[int(len(self.periods) / 2)])
        self.plot_period = period_list
        print(self.factor_name)
        print(self.plot_period)


    def get_factor_period_columns(self):
        """
            获取该因子下 有多少个超参
        :return:  因子的超参数组
        """
        factor_product_files = func_lib.read_files_in_path(self.path)
        data = pd.read_pickle(self.path + factor_product_files[0])
        return data.drop(['datetime'],axis=1).columns

    def get_merge_product_factor_data(self):
        """
        获取合并多个品种后的因子数据
        :return:  df 包含 该因子多品种多周期和 品种列名 ‘product’ DataFrame
        """
        merge_df = pd.DataFrame()
        product_factor_files = sorted(func_lib.read_files_in_path(self.path))

        for one_product_factor_file in product_factor_files:
            data = self.get_factor_data_with_datetime_index(self.path + one_product_factor_file)
            # 预留product 方便后面训练时使用
            data['product'] = one_product_factor_file.split('-')[0][2:]
            merge_df = pd.concat([merge_df,data],axis=0)
        print(merge_df)
        return merge_df


    def get_factor_data_with_datetime_index(self,path):
        """
            获取因子数据 并将datetime 转换为索引
            :param path: str -> 因子存放位置
            :return: DataFrame
        """
        data = pd.read_pickle(path);
        data.set_index('datetime', inplace=True)
        return data

    def describe(self):
        """
            导出因子个品种的describe
            :return: NONE
        """
        print(self.factor_name)
        factor_product_files = func_lib.read_files_in_path(self.path)
        save_folder_path = config.FACTOR_DESCRIBE_SAVE_PATH + self.factor_name+'\\' + self.cycle+'\\'
        func_lib.mkdir(save_folder_path)

        result_df_dict = {}
        periods = self.periods
        for period in periods:
            result_df_dict[period] = pd.DataFrame()

        for factor_product_file in factor_product_files:
            data = self.get_factor_data_with_datetime_index(self.path  + factor_product_file)
            for period in  result_df_dict.keys():
                result_df_dict[period][factor_product_file.split('-')[0][2:]] = data.describe(percentiles=[0.01,0.05,0.1,0.9,0.95,0.99])[period]

        for period in  result_df_dict.keys():
            print(result_df_dict[period])
            func_lib.to_excel(result_df_dict[period], save_folder_path + period+'.xls')
        print(self.factor_name + ' describe 导出完毕')

    def plot_factor_std_and_mean_by_day(self):
        """
            画因子的均值和标准差
        :return: NONE
        """
        def plot_data(data,period,period_save_path):
            print(period + '开始画图')
            temp_period_data = data[[period, 'datetime']]
            temp_period_data['date'] = temp_period_data['datetime'].apply(lambda x: x.strftime('%Y-%m-%d'))
            temp_period_data = temp_period_data.set_index("datetime")
            factor_mean_value = temp_period_data.groupby('date')[period].mean()
            factor_std_value = temp_period_data.groupby('date')[period].std()

            mean_title = period + '--' + factor_product_file.split('-')[0][2:] + '-- mean'
            std_title = period + '--' + factor_product_file.split('-')[0][2:] + '-- std'

            factor_mean_value.index = range(len(factor_mean_value))
            factor_std_value.index = range(len(factor_std_value))
            # 画图
            func_lib.plot_more_subplot_line_chart(factor_mean_value, factor_std_value, [mean_title, std_title],
                                                  period_save_path, factor_product_file.split('-')[0][2:])

        factor_product_files = func_lib.read_files_in_path(self.path)
        total_start = datetime.datetime.now()
        # 循环遍历单个品种文件
        for factor_product_file in sorted(factor_product_files):
            save_path = config.FACTOR_MEAN_AND_STD_SAVE_PATH + self.factor_name+'\\'+self.cycle+'\\'
            func_lib.mkdir(save_path)

            data =  pd.read_pickle(self.path+factor_product_file)
            start = datetime.datetime.now()
            print(start)

            with ThreadPoolExecutor(max_workers=5) as executor:
                for period in self.plot_period:
                    period_save_path = save_path +period+'\\'
                    func_lib.mkdir(period_save_path)
                    req = executor.submit(plot_data,data,period,period_save_path)
                    time.sleep(0.01)
                    print(period + '画图完毕')
            end = datetime.datetime.now()
            print(factor_product_file + '-' + ' 画图完毕  耗时 : ' + str(end-start))
            func_lib.clear_plot()
        total_end =   datetime.datetime.now()
        print(self.factor_name +'画图完毕 =============== 总耗时 ：' + str(total_end - total_start))

    def plot_factor_ic_by_day(self,ret:int=24):
        """
            以日为单位计算因子 IC  画图并保存对应数据
        :param ret:  int 比如 24 （确保行情数据中已计算）
        :return: NONE
        """
        factor_product_files = func_lib.read_files_in_path(self.path)

        # 循环遍历单个品种文件
        for factor_product_file in factor_product_files:
            save_path = config.FACTOR_IC_SAVE_PATH + self.factor_name + '\\' + self.cycle+'\\'
            func_lib.mkdir(save_path)

            data = pd.read_pickle(self.path + factor_product_file)
            product = factor_product_file.split('-')[0]
            #获取行情数据中的 ret
            quot_data_with_ret =  pd.read_pickle(config.QUOT_DATA_PATH+self.cycle+'\\quot_ret\\'+product+'.pkl')
            with ThreadPoolExecutor(max_workers=5) as executor:
                for period in self.plot_period:
                    start = datetime.datetime.now()
                    period_pic_save_path = save_path  +period+ '\\pic\\'
                    period_data_save_path = save_path + period+'\\data\\'
                    func_lib.mkdir(period_pic_save_path)
                    func_lib.mkdir(period_data_save_path)

                    temp_period_data = data[[period, 'datetime']]
                    temp_period_data['date'] = temp_period_data['datetime'].apply(lambda x: x.strftime('%Y-%m-%d'))
                    temp_period_data = temp_period_data.set_index("datetime")
                    temp_period_data['ret'] = quot_data_with_ret['ret.'+str(ret)]

                    # 以日为单位计算 IC 皮尔森 斯皮尔曼
                    spearman_result = temp_period_data.groupby('date').apply(
                        lambda x: x[period].corr(x['ret'], method='spearman'))
                    pearson_result = temp_period_data.groupby('date').apply(
                        lambda x: x[period].corr(x['ret']))

                    sperman_result_mean_value = spearman_result.mean()
                    sperman_result_std_value = spearman_result.std()

                    pearson_result_mean_value = pearson_result.mean()
                    pearson_result_std_value = pearson_result.std()
                    spearman_title = period + '-' + product + '-sprm std :' + str(
                        sperman_result_std_value) + '-sprm mean:' + str(sperman_result_mean_value)
                    pearson_title = period + '-' + product + '-prs std :' + str(
                        pearson_result_std_value) + '- prs mean:' + str(pearson_result_mean_value)

                    export_df = pd.DataFrame()
                    pearson_result.index = range(len(pearson_result))
                    spearman_result.index = range(len(spearman_result))
                    export_df['date'] = sorted(list(set(temp_period_data['date'])))
                    export_df['pearson'] = pearson_result
                    export_df['spearman'] = spearman_result

                    req = executor.submit(func_lib.plot_more_subplot_line_chart, pearson_result, spearman_result,
                                          [pearson_title, spearman_title],period_pic_save_path,product)
                    time.sleep(0.01)
                    print(period + 'ic 画图完毕')
                    end = datetime.datetime.now()
                    print(factor_product_file + '-' + ' 画图完毕  耗时 : ' + str(end - start))
                    # IC 画图
                    func_lib.plot_more_subplot_line_chart(pearson_result, spearman_result,
                                                          [pearson_title, spearman_title], period_pic_save_path, product)
                    #保存IC值
                    export_file_path  = period_data_save_path + product + '.pkl'
                    func_lib.save(export_df,export_file_path)
                func_lib.clear_plot()

    # 获取因子IC
    def get_factor_ic(self,period:int = 120,product:str='RB00'):
        """
        :param period:  int 周期 比如 10
        :param product: str 品种名 比如 'AP00'
        :return: NONE
        """
        file_path = config.FACTOR_IC_SAVE_PATH + self.factor_name + '\\' + self.cycle+'\\'+self.factor_name+'-'+str(period)+'\\data\\'+product+'.pkl'
        data = func_lib.load(file_path)
        print(self.factor_name)
        print('pearson 平均值 : ', data['pearson'].mean())
        print('pearson 标准差 : ', data['pearson'].std())
        print('pearson IR', data['pearson'].mean() / data['pearson'].std())

        print('spearman 平均值: ', data['spearman'].mean())
        print('spearman 标准差: ', data['spearman'].std())
        print('spearman IR： ', data['spearman'].mean() / data['spearman'].std())

    def count_factor_kurtosis(self):
        """
            计算因子 峰度 偏度 均值 标准差
        :return:
        """
        product_factor_files = sorted(func_lib.read_files_in_path(self.path))

        result = pd.DataFrame(index=self.periods, columns=['kurt', 'skew', 'mean', 'std'])

        #拼接各品种数据
        merge_df = self.get_merge_product_factor_data()
        merge_df = merge_df.drop(['product'],axis=1).fillna(0)

        # 根据因子超参循环
        for period in self.periods:
            result.loc[period]['kurt']=func_lib.get_kurtosis(merge_df[period])
            result.loc[period]['skew']=func_lib.get_skew(merge_df[period])
            result.loc[period]['mean']=merge_df[period].mean()
            result.loc[period]['std']=merge_df[period].std()
            #stats.kurtosis(data['signal'])
            #result.loc[factor_names[factor_num]]['skew'] = stats.skew(data['signal'])

        save_folder_path = config.EXCEL_EXPORT_PATH + '峰度偏度\\'+self.factor_name+'\\'+self.cycle+'\\'
        func_lib.mkdir(save_folder_path)
        func_lib.to_excel(result,save_folder_path+'kurt_skew.xls')

    def get_filter_daily_limit_merged_data(self):
        """
         根据去掉涨跌停的行情数据 获取因子数据并组合各品种
        :return: df
        """
        #各品种 循环进行去涨跌停数据
        product_files = sorted(func_lib.read_files_in_path(self.path))
        total_num = 0
        merge_df = pd.DataFrame()
        for one_product_file in product_files:
            quot_data_path = config.QUOT_DATA_PATH + self.cycle + '\\filter_limit\\'
            print(one_product_file)
            product_quot_data = pd.read_pickle(quot_data_path + one_product_file.split('-')[0]+'.pkl')
            index_list = list(product_quot_data.index)
            product_factor_data = pd.read_pickle(self.path + one_product_file)
            product_factor_data.set_index('datetime',inplace=True)
            product_factor_data = product_factor_data[product_factor_data.index.isin(index_list)]
            product_factor_data['product'] = one_product_file.split('-')[0][2:]
            merge_df = merge_df.append(product_factor_data)
        return merge_df


# Factor('FCT_Demark').filter_daily_limit()


