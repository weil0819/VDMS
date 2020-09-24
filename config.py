#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
config

@date: Sep 23, 2020
@author: Wesley
"""

import contextlib
import time
from configparser import ConfigParser

@contextlib.contextmanager
def stopwatch(message):
    
    """

    :function:
        - A context manager to print the run time of a block of code.
    :param message:
        - The message pertaining to the block of code to be executed. (str)
    :return:
        - A dictionary containing the message and elapsed time.
    """
    
    t0 = time.time()
    try:
        yield
    finally:
        t1 = time.time()
        print('Total elapsed time for %s: %.6f ms' % (message, (t1 - t0)*1000))
        return {
            'message': message,
            'elapsed_time': (t1 - t0)
        }


def config(filename='database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db