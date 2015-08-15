# -*- coding: utf-8 -*-
__title__ = 'ellis_island'
__author__ = 'Steven Cutting'
__author_email__ = 'steven.c.projects@gmail.com'
__created_on__ = '7/15/2015'

from os import getenv

from ellis_island.stashtofile import FileStash
from ellis_island.stashtodatabase import SQLStash
from ellis_island.utils.misc import get_default_data_key

import logging
LOG = logging.getLogger(__name__)


class EnMasseStash(object):
    def __init__(self, metauri, rawuri, texturi, metatable='metadata',
                 extraencrypt=False, encryptkey=getenv('DAS_ENCRYPT_KEY',
                                                       get_default_data_key()),
                 DataStash=FileStash, MetaStash=SQLStash):
        self.metawriter = MetaStash(metauri, metatable, encrypt=extraencrypt,
                                    encryptkey=encryptkey)
        self.rawwriter = DataStash(rawuri, encrypt=extraencrypt,
                                   encryptkey=encryptkey)
        self.textwriter = DataStash(texturi, encrypt=extraencrypt,
                                    encryptkey=encryptkey)

    def stash(self, datumdict):
        self.metawriter.stash(datumdict['meta'])
        self.rawwriter.stash(datumdict['raw'])
        if datumdict['meta']['use']:
            self.textwriter.stash(datumdict['text'])

    def close(self):
        self.metawriter.close()
        self.rawwriter.close()
        self.textwriter.close()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()


def stash_en_masse(dataiter, metauri, rawuri, texturi, metatable,
                   extraencrypt=False,
                   encryptkey=getenv('DAS_ENCRYPT_KEY',
                                     get_default_data_key())):
    I = 0
    with EnMasseStash(metauri, rawuri, texturi, metatable=metatable,
                      extraencrypt=extraencrypt, encryptkey=encryptkey
                      ) as stashobj:
        for i, datum in enumerate(dataiter):
            LOG.info(''.join([str(i), datum['meta']['org_filename']]))
            stashobj.stash(datum)
            I = i
    LOG.info(''.join(['DocsIterd:\t', str(I)]))
