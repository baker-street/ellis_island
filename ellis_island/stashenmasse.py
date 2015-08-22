# -*- coding: utf-8 -*-
__title__ = 'ellis_island'
__author__ = 'Steven Cutting'
__author_email__ = 'steven.c.projects@gmail.com'
__created_on__ = '7/15/2015'
__copyright__ = "ellis_island  Copyright (C) 2015  Steven Cutting"
__credits__ = ["Steven Cutting"]
__license__ = "GPL3"
__maintainer__ = "Steven Cutting"
__email__ = 'steven.c.projects@gmail.com'

from os import getenv

from ellis_island.stashtofile import file_stash
from ellis_island.stashtodatabase import SQLStash
from ellis_island.utils.misc import get_default_data_key

import logging
LOG = logging.getLogger(__name__)


class EnMasseStash(object):
    def __init__(self, metauri, rawuri, texturi, metatable='metadata',
                 extraencrypt=False, encryptkey=getenv('DAS_ENCRYPT_KEY',
                                                       get_default_data_key()),
                 StashFiles=file_stash, StashMeta=SQLStash, **xargs):
        # TODO (steven_c) consider handling the encrypt key through xargs.
        self.metawriter = StashMeta(metauri, metatable, encrypt=extraencrypt,
                                    encryptkey=encryptkey, **xargs)
        self.rawwriter = StashFiles(rawuri, encrypt=extraencrypt,
                                   encryptkey=encryptkey, **xargs)
        self.textwriter = StashFiles(texturi, encrypt=extraencrypt,
                                    encryptkey=encryptkey, **xargs)

    def stash(self, datumdict):
        self.metawriter.stash(datumdict['meta'])
        try:
            self.rawwriter.stash(datumdict['raw'])
        except KeyError:
            pass
        if datumdict['meta']['use']:
            try:
                self.textwriter.stash(datumdict['text'])
            except KeyError:
                pass

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
                                     get_default_data_key()),
                   **xargs):
    # TODO (steven_c) consider handling the encrypt key through xargs.
    I = 0
    with EnMasseStash(metauri, rawuri, texturi, metatable=metatable,
                      extraencrypt=extraencrypt, encryptkey=encryptkey,
                      **xargs) as stashobj:
        for i, datum in enumerate(dataiter):
            try:
                ogfname = datum['meta']['org_filename']
            except KeyError:
                ogfname = datum['meta']['uuid']
            LOG.info('\t'.join([str(i), ogfname]))
            stashobj.stash(datum)
            I = i
    LOG.info('\t'.join(['DocsIterd:', str(I)]))
