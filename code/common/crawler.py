#! /usr/bin/env python
# -*- coding:utf8 -*-
from abc import ABCMeta, abstractmethod

import requests

class Crawler(metaclass=ABCMeta):
    """爬虫类基类

    """

    @abstractmethod
    def get_content(self, **params):
        pass


class StockInfoCrawler(Crawler):

    def __init__(self):
        self._url_template = 'https://data.gtimg.cn/flashdata/hushen/daily/{0}/{1}.js'

    def get_content(self, *params):
        url = self._url_template.format(params[0], params[1])
        content = requests.get(url).text
        lines = content.split('\\n\\')
        print('content')

if __name__ == '__main__':
    print(StockInfoCrawler().get_content('21', "sh600845"))
