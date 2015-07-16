# -*- coding: utf-8 -*-
__title__ = 'ellis_island'
__author__ = 'Steven Cutting'
__author_email__ = 'steven.c.projects@gmail.com'
__created_on__ = '7/13/2015'

from os import getenv

import dataset

from ellis_island.utils.misc import get_default_data_key

# def create_table(uri, schema):


class SQLStash(object):
    def __init__(self, uri,
                 table=getenv('METADATA_TABLE_NAME', 'docmetadata'),
                 encrypt=False,
                 encryptkey=getenv('DAS_ENCRYPT_KEY', get_default_data_key())):
        self.uri = uri
        self.table = table
        self.conn = dataset.connect(uri)
        self.conn.begin()
        self.tbl = self.conn[self.table]

        self.count = 0  # find a better way

    def stash(self, datumdict):
        self.tbl.insert(datumdict)
        self.count += 1
        if self.count > 200:
            self.conn.commit()
            self.conn.begin()
            self.count = 0

    def close(self):
        self.conn.commit()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()
