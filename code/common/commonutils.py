#! /usr/bin/env python
# -*- coding:utf8 -*-

def local_round(num, n=0):
    """
    功能：优化Python内置的round()函数有时出现四舍六入的问题，实现真正的四舍五入。
    实现原理：当需要四舍五入的小数点后一位是5时，加1变成6，即可顺利利用round()函数，实现真正的四舍五入。
    参数：
        num: 需要四舍五入的数字；
        n: 保留的小数点位数，默认取整。
    """

    if '.' in str(num):
        if len(str(num).split('.')[1]) > n and str(num).split('.')[1][n] == '5':
            num += 1 * 10 ** -(n + 1)
    if n:
        return round(num, n)
    else:
        return round(num)

def local_divide(a, b):
    if b == 0:
        return 0
    else:
        result = a/b
        return result


if __name__ == '__main__':
    print(local_round(23.12, 0))
    print(local_round(23, 0))
    print(local_round(23.1, 4))
    print(local_round(23.155, 2))
    print(local_round(23.154, 2))
