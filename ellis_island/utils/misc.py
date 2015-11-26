# -*- coding: utf-8 -*-
__title__ = 'ellis_island'
__author__ = 'Steven Cutting'
__author_email__ = 'steven.c.projects@gmail.com'
__created_on__ = '7/13/2015'
__copyright__ = "ellis_island  Copyright (C) 2015  Steven Cutting"
__credits__ = ["Steven Cutting"]
__license__ = "GPL3"
__maintainer__ = "Steven Cutting"
__email__ = 'steven.c.projects@gmail.com'

import logging
LOG = logging.getLogger(__name__)

import os
from os import environ
try:
    from cryptography.fernet import Fernet
except ImportError:
    LOG.warn('cryptography no installed.')

from superserial.utils import spelunker_gen

from estratto.fixEncoding import auto_eng_unicode_dang_it


"""
def spelunker_gen(rootdir):
    for dirname, subdirlist, filelist in os.walk(rootdir):
        for fname in filelist:
            yield '{}/{}'.format(auto_eng_unicode_dang_it(dirname), auto_eng_unicode_dang_it(fname))
"""


def pass_through(stuff):
    return stuff


def encrypt_it(content, key):
    ferob = Fernet(str(key))
    return ferob.encrypt(str(content))


def get_default_data_key(fpath=''.join([environ['HOME'],
                                        '/.defaultdatakey.txt'])):
    try:
        with open(fpath) as keyfile:
            return [i for i in keyfile.readlines()][0].replace('\n', '')
    except(IOError):
        return None
