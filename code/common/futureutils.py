#! /usr/bin/env python
# -*- coding:utf8 -*-

def get_product_by_instrument(instrument):
    """
    根据合约获取品种
    Parameters
    ----------
    instrument

    Returns
    -------

    """
    return instrument[0:2]



if __name__ == '__main__':
    print(get_product_by_instrument('IF1701'))