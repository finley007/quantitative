# -*- coding:utf8 -*-

import os
import struct
import datetime


def stock_csv(filepath, name, targetdir) -> None:
    """
    读取通达信.day文件，并生成对应名称的csv文件

    Parameters
    ----------
    filepath: string 源文件路径
    name: string  文件名
    targetdir: string  目标文件路径

    Returns
    -------

    """
    with open(filepath, 'rb') as f:
        file_object_path = targetdir + name + '.csv'
        file_object = open(file_object_path, 'w+')
        title_str = "Date,Open,High,Low,Close,Open_interest,Volume,settlement_price\n"  # 定义csv文件标题
        file_object.writelines(title_str)  # 将文件标题写入到csv中
        while True:
            stock_date = f.read(4)  # 读取0-3
            stock_open = f.read(4)  # 读取4-7
            stock_high = f.read(4)  # 读取8-11
            stock_low = f.read(4)  # 读取12-15
            stock_close = f.read(4)  # 读取16-19
            stock_open_interest = f.read(4)  # 读取20-23
            stock_vol = f.read(4)  # 读取24-27
            stock_settlement_price = f.read(4)  # 读取28-31

            # date,open,high,low,close,open_interest,vol,settlement_price

            if not stock_date:
                break
            stock_date = struct.unpack('l', stock_date)  # 4字节 如20091229
            stock_open = struct.unpack('f', stock_open)  # 开盘价
            stock_high = struct.unpack('f', stock_high)  # 最高价
            stock_low = struct.unpack('f', stock_low)  # 最低价
            stock_close = struct.unpack('f', stock_close)  # 收盘价
            stock_open_interest = struct.unpack('l', stock_open_interest)  # 持仓量
            stock_vol = struct.unpack('l', stock_vol)  # 成交量
            stock_settlement_price = struct.unpack("f", stock_settlement_price)  # 结算价

            date_format = datetime.datetime.strptime(str(stock_date[0]), '%Y%M%d')  # 格式化日期
            day_str = date_format.strftime('%Y-%M-%d') + "," + str(stock_open[0]) + "," + str(stock_high[0]) + "," \
                      + str(stock_low[0]) + "," + str(stock_close[0]) + "," + str(stock_open_interest[0]) + "," \
                      + str(stock_vol[0]) + "," + str(stock_settlement_price[0]) + "\n"
            file_object.writelines(day_str)
        file_object.close()


# path_dir = '/Users/finley/Projects/stock-index-future/data/thirdparty/tdx'
path_dir = 'C:\\new_tdx\\vipdoc\\sh\\lday'
# target_dir = '/Users/finley/Projects/stock-index-future/data/temp/'
target_dir = 'E:\\data\\thirdparty\\tdx\\'
listfile = os.listdir(path_dir)
for fname in listfile:
    stock_csv(path_dir + os.path.sep + fname, fname, target_dir)
else:
    print('The for ' + path_dir + ' to ' + target_dir + ' loop is over')
    print("Done!!")
