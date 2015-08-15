# -*- coding: utf-8 -*-
__title__ = 'ellis_island'
__author__ = 'Steven Cutting'
__author_email__ = 'steven.c.projects@gmail.com'
__created_on__ = '7/13/2015'

import os
from os import environ

from cryptography.fernet import Fernet


def spelunker_gen(rootdir):
    for dirname, subdirlist, filelist in os.walk(rootdir):
        for fname in filelist:
            yield '{}/{}'.format(dirname, fname)


def pass_through(stuff):
    return stuff


def encrypt_it(content, key):
    ferob = Fernet(key)
    return ferob.encrypt(content)


def get_default_data_key(fpath=''.join([environ['HOME'],
                                        '/.defaultdatakey.txt'])):
    try:
        with open(fpath) as keyfile:
            return [i for i in keyfile.readlines()][0].replace('\n', '')
    except(IOError):
        return None
