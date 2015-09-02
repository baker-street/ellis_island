#!/usr/bin/env python
# -*- coding: utf-8 -*-
__title__ = 'ellis_island'
__author__ = 'Steven Cutting'
__author_email__ = 'steven.e.cutting@linux.com'
__created_on__ = '6/29/2015'

import logging
LOG = logging.getLogger(__name__)

import sys
import os
# from os import path
from os import getenv
from uuid import uuid4

# import pathlib
from arrow import now

from ellis_island.utils import misc
from ellis_island import fullprep as fprep
# from ellis_island import stashenmasse
# from ellis_island import stashtodatabase
from ellis_island.utils.misc import get_default_data_key
from ellis_island.utils.smartopen import prefix_path_from_uri  # , ParseUri

# Imports for script portion.
import click


if sys.version_info[0] < 3:
    _STRINGTYPES = (basestring,)
else:
    # temp fix, so that 2.7 support wont break
    unicode = str  # adjusting to python3
    _STRINGTYPES = (str, bytes)


LOGFMT = '%(levelname)s\tproc:%(process)d thread:%(thread)d module:%(module)s\
\t%(message)s'

PROJECT = unicode(uuid4()).split('-')[0]

TESTHELP = '''If test mode,the meta table will end with the projects name.
'''


@click.command()
@click.argument('inputdir', default='/mnt/input')
# help='Directory to pull files from. Full path.')
@click.option('--meta',
              default='sqlite:////mnt/output/metadata.db',
              help='Where to store the captured metadata. A uri.')
@click.option('--datafile', default='/mnt/output/textdata/',
              help='Where to store the out put data files. A uri.')
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
@click.option('--encrypt/--no-encrypt', default=False,
              help="Encrypt the files? (does not include metadata")
@click.option('--encryptkey', default=getenv('DAS_ENCRYPT_KEY',
                                             get_default_data_key()),
              help='The encryption key to use')
@click.option('--kmsencrypt/--no-kmsencrypt', default=False,
              help="Use kms to encrypt files going to s3.")
@click.option('--kmskeyid', default='',
              help='The key id of the kms encryption key to use. For s3.')
@click.option('--dontstop/--no-dontstop', default=False,
              help="Skips docs that raise errors.")
def main(inputdir,
         meta,
         datafile,
         n,
         c,
         project,
         metatable,
         test,
         encrypt,
         encryptkey,
         kmsencrypt,
         kmskeyid,
         dontstop):
    try:
        encryptkey = encryptkey.strip()
    except AttributeError:
        LOG.debug('No encryptkey was passed passed or found.')
    starttime = now()
    count = n  # TODO (steven_c) clean this up
    vcores = c
    os.environ['CURRENT_PROJECT_UUID'] = project
    if test:
        metatable = metatable + project
    LOG.info('\t'.join(['Project:', project, 'Table:', metatable]))
    # --
    # IO
    emaillist = list(misc.spelunker_gen(inputdir))
    LOG.info(len(emaillist))
    # --
    # Proc
    if not count:
        count = len(emaillist)
    emlsmpl = emaillist[0:count]
    batchsize = 50
    prefix = prefix_path_from_uri(datafile)
    respackiter = fprep.clean_and_register_bulk(emlsmpl,
                                                prefix=prefix,
                                                dontstop=dontstop,
                                                njobs=vcores,
                                                batchsize=batchsize,
                                                ordered=False,
                                                project=project)
    for i, yy in enumerate(respackiter):
        docuuid = yy['meta']['uuid']
        ogpath = yy['meta']['org_filename']
        LOG.info(str(i) + '\t' + docuuid + '\t' + ogpath)
    # --
    # IO
    """
    outroottext = path.join(datafile, 'text/')
    outrootraw = path.join(datafile, 'raw/')
    if ParseUri(datafile).scheme in {'file'}:
        outroottextobj = pathlib.Path(outroottext)
        if not outroottextobj.is_dir():
            outroottextobj.mkdir(parents=True)
        outrootrawobj = pathlib.Path(outrootraw)
        if not outrootrawobj.is_dir():
            outrootrawobj.mkdir(parents=True)
    # dbcon = 'postgresql://tester:test12@localhost:2345/docmeta'
    """
    """
    stashtodatabase.default_create_table_sqlalchemy(meta,
                                                    tablename=metatable)
    stashenmasse.stash_en_masse(respackiter,
                                metauri=meta,
                                rawuri=outrootraw,
                                texturi=outroottext,
                                metatable=metatable,
                                extraencrypt=encrypt,
                                encryptkey=encryptkey,
                                kmsencrypt=kmsencrypt,
                                kmskeyid=kmskeyid)
    """
    LOG.info('\t'.join(['VCores:',
                        str(vcores),
                        ]))
    LOG.info('\t'.join(['BatchSize:',
                        str(batchsize),
                        ]))
    endtime = now()
    LOG.info('\t'.join(['RunTime:',
                        str(endtime - starttime),
                        ]))


if __name__ == '__main__':
    logging.basicConfig(format=LOGFMT,
                        level=logging.DEBUG,
                        stream=sys.stdout)
    logging.root.level = logging.DEBUG
    logging.basicConfig
    main()
