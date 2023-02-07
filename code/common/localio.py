#! /usr/bin/env python
# -*- coding:utf8 -*-
import gzip
import os
import _pickle as cPickle

def list_files_in_path(path):
    """列出给定目录下的所有文件

    Parameters
    ----------
    path : 给定目录

    Returns
    -------
    list : 文件列表

    """
    return list(map(lambda x: x, list(set(os.listdir(path)))))

def build_path(*folders):
    """构造全路径

    Parameters
    ----------
    folders : 给定的全路径的各级目录

    Returns
    -------
    string : 全路径

    """
    if len(folders) > 0:
        return "/".join(folders)


def read_decompress(path):
    """读取并解压

    Parameters
    ----------
    path : 给定目录

    Returns
    -------
    data : 文件内容

    """
    with gzip.open(path, 'rb', compresslevel=1) as file_object:
        raw_data = file_object.read()
    return cPickle.loads(raw_data)


def save_compress(data, path):
    """压缩并保存

    Parameters
    ----------
    path : 给定目录
    data : 文件内容
    Returns
    -------

    """
    serialized = cPickle.dumps(data)
    with gzip.open(path, 'wb', compresslevel=1) as file_object:
        file_object.write(serialized)




class FileWriter:

    def __init__(self, path):
        self.path = path
        self.file = open(self.path, mode='w')

    def open_file(self):
        self.file = open(self.path, mode='w')

    def close_file(self):
        if self.file is not None:
            self.file.close

    def write_file(self, content):
        self.file.write(content)

    def write_file_line(self, content):
        self.file.write(content + '\n')


if __name__ == '__main__':
    # print(list_files_in_path(constants.DATA_PATH + 'future/tick/IF/'))
    # data = pd.read_csv('/Users/finley/Projects/stock-index-future/data/original/future/tick/IF/CFFEX.IF2212.csv')
    # save_compress(data, '/Users/finley/Projects/stock-index-future/data/original/future/tick/IF/CFFEX.IF2212.pkl')
    # write_file('/Users/finley/Projects/stock-index-future/data/test','\n'.join(['a','b']))
    # file_writer = FileWriter(TEMP_PATH + '/test.txt')
    # file_writer.write_file_line("aa")
    # file_writer.write_file_line("bb")
    # file_writer.close_file()
    print(build_path('a','b'))
