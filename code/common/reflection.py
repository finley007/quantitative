#! /usr/bin/env python
# -*- coding:utf8 -*-
import inspect
import pandas as pd

from common.localio import read_decompress
from common.constants import TEST_PATH
from common.visualization import draw_analysis_curve

def create_instance(module_name, class_name, *args, **kwargs):
    """
    反射生成类实例

    Parameters
    ----------
    module_name
    class_name
    args
    kwargs

    Returns
    -------

    """
    module_meta = __import__(module_name, globals(), locals(), [class_name])
    class_meta = getattr(module_meta, class_name)
    obj = class_meta(*args, **kwargs)
    return obj


def get_all_class(module_name):
    """
    获取包下所有的类
    Parameters
    ----------
    module_name

    Returns
    -------

    """
    class_list = []
    module = __import__(module_name)
    for package_name, package in inspect.getmembers(module, inspect.ismodule):
        for class_name, clz in inspect.getmembers(package, inspect.isclass):
            class_list.append(clz)
    return class_list


if __name__ == '__main__':
    # 列出指定包下所有的类
    module_name = 'factor.volume_price_factor'
    all_class = get_all_class(module_name)
    print(all_class)
    filter_class = list(filter(lambda clz: module_name in str(clz), all_class))
    print(filter_class)

    # 反射生成类实例sdxcvbn
    # data = read_decompress(TEST_PATH + 'IC2003.pkl')
    # # data = read_decompress('/Users/finley/Projects/stock-index-future/data/organised/future/IH/IH2209.pkl')
    # factor = create_instance('factor.volume_price_factor', 'WilliamFactor', [10])
    # data = factor.caculate(data)
    # data.index = pd.DatetimeIndex(data['datetime'])
    # draw_analysis_curve(data, show_signal=True, signal_keys=factor.get_keys())