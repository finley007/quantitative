#! /usr/bin/env python
# -*- coding:utf8 -*-
import argparse
import os

from tools.factor_tools import create_factor_files

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Build factor file')
    parser.add_argument('--factor','-f',type=str,required=True,help="Input factor code")

    args = parser.parse_args()
    create_factor_files([args.factor])
