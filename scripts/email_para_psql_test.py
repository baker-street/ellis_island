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
from uuid import uuid4

# import psycopg2
# from psycopg2 import connect
# from gensim.utils import simple_preprocess
from chevy.utils import dir_spelunker as dirs

# from gentrify.parseEmail import email_whole_parse
from ellis_island import parallel_easy as para
from ellis_island import threading_easy as thre
# from ellis_island.utils import Serial
# from gentrify.parse import parse_multi_layer_file
# from gentrify.parse import OKEXT

from ellis_island.registrar import registrar_nested
from ellis_island.prepforstash import s3_and_psql_prep

from ellis_island import stach

import logging
LOG = logging.getLogger(__name__)

LOGFMT = '%(levelname)s\tproc:%(process)d thread:%(thread)d module:%(module)s\
\t%(message)s'

CASE = unicode(uuid4())
CASEABV = CASE.split('-')[0]
ERRORCOUNT = 0


def parse_and_log(fname):
    try:
        return [s3_and_psql_prep(bit,
                                 case=CASE,
                                 prefix='/mnt/data1/Case2/parsed/')
                for bit in registrar_nested(fname)]
        # parseddoc = parse_multi_layer_file(fname)
        # for h in parseddoc:
        #    #y = simple_preprocess(h['content']['body'])
        #    # continue
        # return parseddoc
    except:
        LOG.critical(fname)
        return None


def stash_it(respack):
    i, res = respack
    if res:
        for n, doc in enumerate(res):
            stach.psql_write_to(doc['psql'], CASEABV)
            daspath = doc['s3text']['text_pointer']
            if doc['psql']['use']:
                with open(daspath, 'w+') as dasfobj:
                    dasfobj.write(doc['s3text']['content'])
            LOG.info(''.join(['\t', str(i),
                              '\t', str(n),
                              '\t', str(doc['psql']['org_filename']),
                              ]))


def main(k,
         dirroot='/mnt/data1/enron/enron_mail_20110402/textonly/enron/', c=3):
    stach.psql_create_table(case=CASEABV)
    emaillist = list(dirs.spelunker_gen(dirroot))
    LOG.info(len(emaillist))
    if not k:
        k = len(emaillist)
    # emlsmpl = [emaillist[random.randint(0, len(emaillist) - 1)]
    #            for i in xrange(k)]
    emlsmpl = emaillist[0:k]

    batchsize = 50
    maxvcores = c
    vcores = len(emlsmpl) / batchsize
    if vcores > maxvcores:
        vcores = maxvcores

    resultiter = para.imap_easy(parse_and_log,
                                emlsmpl,
                                vcores,
                                batchsize,
                                False)
    LOG.info(''.join(['VCores:\t',
                      str(vcores),
                      ]))
    LOG.info(''.join(['BatchSize:\t',
                      str(batchsize),
                      ]))
    outroot = pathlib.Path('/mnt/data1/Case2/parsed/' + CASE + '/text/')
    if not outroot.is_dir():
        outroot.mkdir(parents=True)
    respackiter = ((i, res)for i, res in enumerate(resultiter))
    with open(os.devnull, 'w') as null:
        thre.threading_easy(stash_it,
                            respackiter,
                            n_threads=100,
                            out_stream=null)
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
