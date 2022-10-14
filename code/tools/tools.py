#! /usr/bin/env python
# -*- coding:utf8 -*-
import rarfile
import os

def filter_stock_data():
    files = os.listdir("E:/data/")
    for file in files:
        print(file)
    path = "E:/data/20220207.rar"
    path2 = "E:/data"
    rf = rarfile.RarFile(path)
    rf.extractall(path2)


if __name__ == '__main__':
    filter_stock_data()

