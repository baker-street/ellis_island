# -*- coding: utf-8 -*-
__title__ = 'ellis_island'
__author__ = 'Steven Cutting'
__author_email__ = 'steven.c.projects@gmail.com'
__created_on__ = '7/8/2015'

from os import getenv
from multiprocessing import cpu_count
from functools import partial

from ellis_island.registrar import registrar_nested
from ellis_island.prepforstash import s3_and_psql_prep

from ellis_island.utils.parallel_easy import imap_easy

import logging
LOG = logging.getLogger(__name__)


def clean_and_register(uri, prefix, dontstop=False,
                       case=getenv('CURRENT_CASE_UUID', 'no-case-env')):
    def run():
        return [s3_and_psql_prep(bit, case=case, prefix=prefix)
                for bit in registrar_nested(uri)]
    if dontstop:
        try:
            return run()
        except:
            LOG.critical(uri)
            return [None]
    else:
        return run()


def clean_and_register_bulk(uriiter, prefix, dontstop=False,
                            case=getenv('CURRENT_CASE_UUID', 'no-case-env')):
    for uri in uriiter:
        for reslist in clean_and_register(uri, prefix, dontstop, case):
            for i, res in enumerate(reslist):
                yield res


def default_n_jobs():
    cpus = cpu_count()
    if cpus > 2:
        return cpus / 2
    else:
        return 1


def clean_and_register_en_masse(uriiter, prefix, dontstop=False,
                                case=getenv('CURRENT_CASE_UUID', 'no-case-env'),
                                njobs=default_n_jobs(), batchsize=50,
                                ordered=False):
    c_and_r = partial(clean_and_register,
                      prefix=prefix,
                      dontstop=dontstop,
                      case=case)
    resultiter = imap_easy(c_and_r, uriiter, njobs, batchsize, ordered)
    for reslist in resultiter:
        for i, res in enumerate(reslist):
            try:
                LOG.info(str(i) + '\t' + res['meta']['org_filename'])
                yield res
            except(TypeError):
                continue
