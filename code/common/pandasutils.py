#! /usr/bin/env python
# -*- coding:utf8 -*-
import pandas as pd

from common.aop import timing
from common.log import get_logger

@timing
def get_index_list_by_value(data, value, columns=[]):
    """
    根据给定的值，在dataframe获取一个索引列表
    Parameters
    ----------
    data: dataframe
    value: 待查找值
    columns: list 过滤的列

    Returns
    -------

    """
    index_list = []
    for index in set(data[data.values == value].index.tolist()):
        for i in range(len(data.loc[index].values)):
            if len(columns) != 0 and data.columns[i] not in columns:
                continue
            try:
                if data.loc[index].values[i] == value:
                    index_list.append((index, data.columns[i]))
            except Exception as e:
                get_logger().error('Error where fix index: {0} and column index {1}'.format(str(index), str(i)))
    return index_list

def get_index_dict_by_value(data, value, columns=[]):
    """
    为了方便操作，把get_index_list_by_value生成的list转为dict
    index -> columns[]
    Parameters
    ----------
    data
    value
    columns

    Returns
    -------

    """
    index_list = get_index_list_by_value(data, value, columns)
    index_dict = {}
    for index in index_list:
        if index[0] not in index_dict:
            index_dict[index[0]] = [index[1]]
        else:
            index_dict[index[0]].append(index[1])
    return index_dict

if __name__ == '__main__':
    data = {'a': [1, 2, 0, 4, 3], 'b': [2, 3, 4, 5, 0], 'c': [0, 2, 0, 4, 5]}
    df = pd.DataFrame(data)
    print(df)
    print(get_index_list_by_value(df, 0))
    print(get_index_list_by_value(df, 0, ['a','c']))
    print(get_index_dict_by_value(df, 0))