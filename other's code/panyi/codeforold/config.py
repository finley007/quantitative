#初始路径
HEAD_PATH = "D:\\data\\weisoft\\"

#模型路径
MODEL_PATH = HEAD_PATH + "model\\"

#模型路径
SIGNAL_PATH = HEAD_PATH + "signal\\"

#预测值合并后的路径
PREDICT_MERGED_PATH = HEAD_PATH + "merged_signal\\"


#金字塔导出因子目录
FACTOR_PATH =  HEAD_PATH + "factor_data\\"

FACTOR_FILTER_PATH =  HEAD_PATH + "factor_filter_limit_data\\"

#行情数据存放目录
QUOT_DATA_PATH = HEAD_PATH + "quot_data\\"

#所有因子合并的大表目录
FACTOR_TOTAL_PATH = HEAD_PATH +'factor_total\\'

#model or excel or h5 文件存放 首选路径
FILE_SAVE_HEAD_PATH = "D:\\机器学习文件存放"

#EXCEL 导出存放路径
EXCEL_EXPORT_PATH = FILE_SAVE_HEAD_PATH+"\\EXCEL\\"

#因子describe 存放路径
FACTOR_DESCRIBE_SAVE_PATH = EXCEL_EXPORT_PATH+"\\describe\\"

FACTOR_MEAN_AND_STD_SAVE_PATH = FILE_SAVE_HEAD_PATH+"\\因子平均值和标准差\\"

#网格寻参 训练模型存放路径
SAVE_SELECT_MODEL_PATH = FILE_SAVE_HEAD_PATH+"\\寻参模型存放\\"

#实验结果存放位置
TEST_RESULT_SAVE = FILE_SAVE_HEAD_PATH+"\\实验结果存放\\"

#shap_画图存放位置
SHAP_PLOT_PATH =  TEST_RESULT_SAVE + '\\shap_plot\\'

#因子IC存放位置
FACTOR_IC_SAVE_PATH =FILE_SAVE_HEAD_PATH+"\\因子IC相关性\\"


PRODUCT_DICT={'AP00': 'ZQAP00', 'CF00': 'ZQCF00', 'SF00': 'ZQSF00', 'SM00': 'ZQSM00', 'SRX00': 'ZQSRX00', 'OI00': 'ZQOI00', 'MA00': 'ZQMA00', 'FG00': 'ZQFG00', 'RM00': 'ZQRM00', 'CU00': 'SQCU00', 'AL00': 'SQAL00', 'ZN00': 'SQZN00', 'PB00': 'SQPB00', 'NI00': 'SQNI00', 'SN00': 'SQSN00', 'RB00': 'SQRB00', 'HC00': 'SQHC00', 'PK00': 'ZQPK00', 'AX00': 'DQAX00', 'C00': 'DQC00', 'M00': 'DQM00', 'Y00': 'DQY00', 'P00': 'DQP00', 'L00': 'DQL00', 'V00': 'DQV00', 'J00': 'DQJ00', 'JM00': 'DQJM00', 'EB00': 'DQEB00', 'PG00': 'DQPG00', 'LH00': 'DQLH00', 'SC0000': 'INSC0000', 'LU00': 'INLU00', 'CS00': 'DQCS00', 'EG00': 'DQEG00', 'SS00': 'SQSS00', 'SP00': 'SQSP00', 'UR00': 'ZQUR00', 'CJ00': 'ZQCJ00', 'FU00': 'SQFU00', 'BUX00': 'SQBUX00', 'RU00': 'SQRU00', 'I00': 'DQI00', 'JD00': 'DQJD00', 'PP00': 'DQPP00', 'SA00': 'ZQSA00', 'PF00': 'ZQPF00', 'TA00': 'ZQTA00', 'IF00': 'ZJIF00', 'IC00': 'ZJIC00', 'IH00': 'ZJIH00', 'AU00': 'SQAU00', 'AG00': 'SQAG00', 'TF00': 'ZJTF00', 'T00': 'ZJT00'}