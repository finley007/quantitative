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
        lines = content.split('\\n\\')[1:]
        content = list(map(lambda item: '20' + item.split(' ')[0][1:], lines))
        return content

if __name__ == '__main__':
    print(StockInfoCrawler().get_content('20', "sh601456"))
