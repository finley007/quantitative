#! /usr/bin/env python
# -*- coding:utf8 -*-

from configparser import ConfigParser

class Config:

    cp = ConfigParser()
    
    def __init__(self):
        self.cp.read('../config.ini')
        
    def get(self, section, key):
        return self.cp.get(section, key)
    
if __name__ == '__main__':
    config = Config()
    print(config.get('common','data_path'))
    