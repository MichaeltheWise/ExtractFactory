# -*- coding: utf-8 -*-
"""
Created on Sat Apr 2 2022

@author: Michael Lin
"""
import functools
import os
import pandas as pd


def cache(func):
    """
    Decorator creating a cache
    This provides better performance than querying important but relatively static table multiple times
    Cache is stored as a csv file in the desired file path with a desired file name
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        file_name = kwargs['file_name'] + '.csv'
        path = os.path.join(kwargs['file_path'], file_name)
        if not os.path.isfile(path):
            result = func(*args, **kwargs)
        else:
            result = pd.read_csv(path, index_col=[0])
        return result
    return wrapper
