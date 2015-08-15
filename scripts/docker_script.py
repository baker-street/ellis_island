#!/usr/bin/env python
# -*- coding: utf-8 -*-
__title__ = 'ellis_island'
__author__ = 'Steven Cutting'
__author_email__ = 'steven.e.cutting@linux.com'
__created_on__ = '6/29/2015'

import pathlib
import sys
import os
from uuid import uuid4

PROJECT = unicode(uuid4())

from ellis_island.utils import misc
from ellis_island import fullprep as fprep
from ellis_island import stashenmasse
from ellis_island import stashtodatabase

# Imports for script portion.
import click


import logging
LOG = logging.getLogger(__name__)

LOGFMT = '%(levelname)s\tproc:%(process)d thread:%(thread)d module:%(module)s\
\t%(message)s'

TESTHELP = '''If test mode,the meta table will end with the first four
characters of the projects name.
'''


@click.command()
@click.argument('input_dir', default='/mnt/input')
# help='Directory to pull files from. Full path.')
@click.argument('metadata_uri',
                default='sqlite:////mnt/output/metadata.db')
# help='Where to store the captured metadata. A uri.')
@click.argument('textdata_uri', default='/mnt/output/textdata/')
# help='Where to store the processed data. A uri.')
@click.option('-n', default=0, type=int,
              help='Number of files to use. If 0, it will use all found files.')
@click.option('-c', default=3,
              help='The number of parallel processes. If 1, will run linear.')
@click.option('--project', default=PROJECT,
              help='The project id to use.')
@click.option('--metatable', default='metadata',
              help='The table to store the metadata in.')
@click.option('--test/--no-test', default=False,
              help=TESTHELP)
def main(input_dir,
         metadata_uri,
         textdata_uri,
         n,
         c,
         project,
         metatable,
         test):
    count = n  # TODO (steven_c) clean this up
    vcores = c
    dirroot = input_dir
    os.environ['CURRENT_PROJECT_UUID'] = project
    if test:
        metatable = metatable + project
    LOG.info('\t'.join(['Project:', project, 'Table:', metatable]))

    emaillist = list(misc.spelunker_gen(dirroot))
    LOG.info(len(emaillist))
    if not count:
        count = len(emaillist)
    emlsmpl = emaillist[0:count]
    batchsize = 50
    respackiter = fprep.clean_and_register_en_masse(emlsmpl,
                                                    textdata_uri,
                                                    dontstop=False,
                                                    njobs=vcores,
                                                    batchsize=batchsize,
                                                    ordered=False,
                                                    project=project)
    outroottext = textdata_uri + project + '/text/'
    outrootraw = textdata_uri + project + '/raw/'
    if textdata_uri.startswith('/') or textdata_uri.startswith('file'):
        outroottext = pathlib.Path(outroottext)
        if not outroottext.is_dir():
            outroottext.mkdir(parents=True)
        outroottext = str(outroottext)
        outrootraw = pathlib.Path(outrootraw)
        if not outrootraw.is_dir():
            outrootraw.mkdir(parents=True)
        outrootraw = str(outrootraw)
    # dbcon = 'postgresql://tester:test12@localhost:2345/docmeta'

    stashtodatabase.default_create_table_sqlalchemy(metadata_uri,
                                                    tablename=metatable)
    stashenmasse.stash_en_masse(respackiter, metauri=metadata_uri,
                                rawuri=outrootraw,
                                texturi=outroottext,
                                metatable=metatable)
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
    main()
