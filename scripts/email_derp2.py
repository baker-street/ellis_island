#!/usr/bin/env python
# -*- coding: utf-8 -*-
__title__ = 'ellis_island'
__author__ = 'Steven Cutting'
__author_email__ = 'steven.e.cutting@linux.com'
__created_on__ = '6/29/2015'

import pathlib
import sys
# import random
import os
# import functools
from uuid import uuid4

# import dataset

CASE = unicode(uuid4())
os.environ['CURRENT_CASE_UUID'] = CASE
CASEABV = CASE.split('-')[0]

# import psycopg2
# from psycopg2 import connect
# from gensim.utils import simple_preprocess
from chevy.utils import dir_spelunker as dirs

# from gentrify.parseEmail import email_whole_parse
# from ellis_island import parallel_easy as para
# from ellis_island import threading_easy as thre
# from ellis_island.utils import Serial
# from gentrify.parse import parse_multi_layer_file
# from gentrify.parse import OKEXT

# from ellis_island.registrar import registrar_nested
# from ellis_island.prepforstash import s3_and_psql_prep
from ellis_island import fullprep as fprep
from ellis_island import stashenmasse

# from ellis_island import stach

import logging
LOG = logging.getLogger(__name__)

LOGFMT = '%(levelname)s\tproc:%(process)d thread:%(thread)d module:%(module)s\
\t%(message)s'


ERRORCOUNT = 0

TABLE = 'docmeta'
if CASEABV:
    TABLENAME = TABLE + CASEABV
else:
    TABLENAME = TABLE


def main(k,
         dirroot='/mnt/data1/enron/enron_mail_20110402/textonly/enron/', c=3):
    # stach.psql_create_table(case=CASEABV)
    emaillist = list(dirs.spelunker_gen(dirroot))
    LOG.info(len(emaillist))
    if not k:
        k = len(emaillist)
    emlsmpl = emaillist[0:k]

    batchsize = 50
    vcores = c
    # respackiter = packer_bulk(emlsmpl, vcores, batchsize)
    respackiter = fprep.clean_and_register_en_masse(emlsmpl,
                                                    '/mnt/data1/Case2/parsed/',
                                                    dontstop=True,
                                                    njobs=vcores,
                                                    batchsize=batchsize,
                                                    ordered=False)

    outroottext = pathlib.Path('/mnt/data1/Case2/parsed/' + CASE + '/text/')
    if not outroottext.is_dir():
        outroottext.mkdir(parents=True)
    outroottext = str(outroottext)
    outrootraw = pathlib.Path('/mnt/data1/Case2/parsed/' + CASE + '/raw/')
    if not outrootraw.is_dir():
        outrootraw.mkdir(parents=True)
    outrootraw = str(outrootraw)
    # dbcon = 'postgresql://tester:test12@localhost:2345/docmeta'
    dbcon = ''.join(['sqlite:////mnt/data1/Case2/parsed/',
                     CASE, '/db', CASEABV, '.db'])
    tblname = 'metadata' + CASEABV
    stashenmasse.stash_en_masse(respackiter, metauri=dbcon,
                                rawuri=outrootraw,
                                texturi=outroottext,
                                metatable=tblname)
    LOG.info(''.join(['VCores:\t',
                      str(vcores),
                      ]))
    LOG.info(''.join(['BatchSize:\t',
                      str(batchsize),
                      ]))


if __name__ == '__main__':
    logging.basicConfig(format=LOGFMT,
                        level=logging.DEBUG,
                        stream=sys.stdout)
    logging.root.level = logging.DEBUG
    logging.basicConfig

    try:
        k = int(sys.argv[1])
    except(IndexError):
        k = 0
    try:
        c = int(sys.argv[2])
    except(IndexError):
        c = 3
    try:
        d = sys.argv[3]
    except(IndexError):
        d = '/mnt/data1/enron/enron_mail_20110402/textonly/enron/'
    main(k, d, c)
