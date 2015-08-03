# -*- coding: utf-8 -*-
__title__ = 'ellis_island'
__author__ = 'Steven Cutting'
__author_email__ = 'steven.c.projects@gmail.com'
__created_on__ = '7/8/2015'

# TODO (steven_c) Work on documentation.

from os import getenv
from multiprocessing import cpu_count
from functools import partial

from ellis_island.registrar import registrar_nested
from ellis_island.prepforstash import prep_many_for_stash_list
from ellis_island.utils.parallel_easy import imap_easy


import logging
LOG = logging.getLogger(__name__)

PROJECTENV = 'CURRENT_PROJECT_UUID'
DEFAULTPROJECTID = 'project'


def chain_procs(var, proclist, i=0):
    try:
        return chain_procs(var=proclist[i](var), proclist=proclist, i=i+1)
    except IndexError:
        return var


def custom_clean_and_register(uri, proclist, dontstop=False):
    """
    """
    newproclist = list(proclist)
    # return chain_procs(uri, newproclist)
    try:
        return chain_procs(uri, newproclist)
    except Exception as e:
        if dontstop:
            LOG.critical('\t'.join([uri, e.message]))
            return [None]
        else:
            raise e


def clean_and_register(uri, dontstop=False,
                       prefix='', project=getenv(PROJECTENV,
                                                 DEFAULTPROJECTID)):
    """
    returns a list with the out put for the file
    and any child files it contains.
    """
    return custom_clean_and_register(uri=uri,
                                     dontstop=dontstop,
                                     proclist=[registrar_nested,
                                               partial(prep_many_for_stash_list,
                                                       prefix=prefix,
                                                       project=project),
                                               ])


def clean_and_register_bulk(uriiter, prefix, dontstop=False,
                            project=getenv(PROJECTENV,
                                           DEFAULTPROJECTID)):
    """
    Provides the output of the files and any child files as one stream.
    Yields dicts.
    """
    for uri in uriiter:
        for res in clean_and_register(uri, prefix, dontstop, project):
            yield res


def default_n_jobs():
    cpus = cpu_count()
    if cpus > 2:
        return cpus / 2
    else:
        return 1


def clean_and_register_en_masse(uriiter, prefix, dontstop=False,
                                project=getenv(PROJECTENV,
                                               DEFAULTPROJECTID),
                                njobs=default_n_jobs(), batchsize=50,
                                ordered=False):
    """
    Provides the output of the files and any child files as one stream.
    Yields dicts.

    Also, able to distribute the work load.
    """
    c_and_r = partial(clean_and_register,
                      prefix=prefix,
                      dontstop=dontstop,
                      project=project)
    resultiter = imap_easy(c_and_r, uriiter, njobs, batchsize, ordered)
    for reslist in resultiter:
        for i, res in enumerate(reslist):
            try:
                LOG.info(str(i) + '\t' + res['meta']['org_filename'])
                yield res
            except(TypeError):
                continue
