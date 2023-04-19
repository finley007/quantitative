#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 17 13:31:09 2023

@author: qyzs
"""
import numpy as np
def shift(x,windows):
    x_=np.roll(x,1)
    x_[:windows-1]=np.nan
    return x_
def rolling_sum(x,windows):
    return np.convolve(x, np.ones(windows,))
def rolling_mean(x,windows):
    return np.convolve(x, np.ones(windows,)/windows)
    
    
    
