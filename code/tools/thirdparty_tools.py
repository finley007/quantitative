# -*- coding:utf8 -*-

import os
import struct
import datetime


# 读取通达信.day文件，并生成对应名称的csv文件
def stock_csv(filepath, name, targetdir) -> None:
    # (通达信.day文件路径, 通达信.day文件名称, 处理后要保存到的文件夹)

    with open(filepath, 'rb') as f:  # 读取通达信.day文件，并处理
        file_object_path = targetdir + name + '.csv'  # 设置处理后保存文件的路径和名称
        file_object = open(file_object_path, 'w+')  # 打开新建的csv文件，开始写入数据
        title_str = "Date,Open,High,Low,Close,Open_interest,Volume,settlement_price\n"  # 定义csv文件标题
        file_object.writelines(title_str)  # 将文件标题写入到csv中
        # while True:
        #     stock_date = f.read(4)  # 读取0-3
        #     stock_open = f.read(4)  # 读取4-7
        #     stock_high = f.read(4)  # 读取8-11
        #     stock_low = f.read(4)  # 读取12-15
        #     stock_close = f.read(4)  # 读取16-19
        #     stock_open_interest = f.read(4)  # 读取20-23
        #     stock_vol = f.read(4)  # 读取24-27
        #     stock_settlement_price = f.read(4)  # 读取28-31
        #
        #     # date,open,high,low,close,open_interest,vol,settlement_price
        #
        #     if not stock_date:
        #         break
        #     stock_date = struct.unpack('l', stock_date)  # 4字节 如20091229
        #     stock_open = struct.unpack('f', stock_open)  # 开盘价
        #     stock_high = struct.unpack('f', stock_high)  # 最高价
        #     stock_low = struct.unpack('f', stock_low)  # 最低价
        #     stock_close = struct.unpack('f', stock_close)  # 收盘价
        #     stock_open_interest = struct.unpack('l', stock_open_interest)  # 持仓量
        #     stock_vol = struct.unpack('l', stock_vol)  # 成交量
        #     stock_settlement_price = struct.unpack("f", stock_settlement_price)  # 结算价
        #
        #     date_format = datetime.datetime.strptime(str(stock_date[0]), '%Y%M%d')  # 格式化日期
        #     day_str = date_format.strftime('%Y-%M-%d') + "," + str(stock_open[0]) + "," + str(stock_high[0]) + "," \
        #               + str(stock_low[0]) + "," + str(stock_close[0]) + "," + str(stock_open_interest[0]) + "," \
        #               + str(stock_vol[0]) + "," + str(stock_settlement_price[0]) + "\n"
        #     file_object.writelines(day_str)  # 将文件写入到csv文件中
        file_object.close()     # 完成数据写入


# 设置通达信.day文件所在的文件夹
path_dir = '/Users/finley/Projects/stock-index-future/data/thirdparty/tdx'
# 设置数据处理好后，要将csv文件保存的文件夹
target_dir = '/Users/finley/Projects/stock-index-future/data/temp/'
# 读取文件夹下的通达信.day文件
listfile = os.listdir(path_dir)
# 逐个处理文件夹下的通达信.day文件，并生成对应的csv文件，保存到../day/文件夹下
for fname in listfile:
    if fname != '.DS_Store':
        stock_csv(path_dir + os.path.sep + fname, fname, target_dir)
else:
    print('The for ' + path_dir + ' to ' + target_dir + '  loop is over')
    print("文件转换已完成")
