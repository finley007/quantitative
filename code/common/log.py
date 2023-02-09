#! /usr/bin/env python
# -*- coding:utf8 -*-

import sys
from loguru import logger

def log_config():
    """
    日志配置
    Returns
    -------

    """
    logger.remove()
    handler_id = logger.add(sys.stdout, level="INFO")

def get_logger():
    log_config()
    return logger


if __name__ == '__main__':
    get_logger().debug('This is debug information')
    get_logger().info('This is info information')
    get_logger().warning('This is warn information')
    get_logger().error('This is error information')