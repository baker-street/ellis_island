#!/usr/bin/env python
# -*- coding: utf-8 -*-
__title__ = 'ellis_island'
__author__ = 'Steven Cutting'
__author_email__ = 'steven.e.cutting@linux.com'
__created_on__ = '6/29/2015'

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
# from ellis_island.utils import Serial
# from gentrify.parse import parse_multi_layer_file
# from gentrify.parse import OKEXT

from ellis_island.registrar import registrar_nested
from ellis_island.prepforstash import s3_and_psql_prep

from ellis_island import stach
CASE = unicode(uuid4())


def parse_and_log(fname):
    try:
        return [s3_and_psql_prep(bit, CASE) for bit in registrar_nested(fname)]
        # parseddoc = parse_multi_layer_file(fname)
        # for h in parseddoc:
        #    #y = simple_preprocess(h['content']['body'])
        #    # continue
        # return parseddoc
    except:
        with open('/home/steven_c/projects/ellis_island/eml_errors.log.' +
                  str(os.getpid()), 'a') as log:
            log.write(fname + '\n')
        return None


def main(k,
         dirroot='/mnt/data1/enron/enron_mail_20110402/textonly/enron/', c=0):
    stach.psql_create_table()
    emaillist = [e for e in
                 dirs.spelunker_gen(dirroot)]
    print len(emaillist)
    if not k:
        k = len(emaillist)
    # emlsmpl = [emaillist[random.randint(0, len(emaillist) - 1)]
    #            for i in xrange(k)]
    emlsmpl = emaillist[0:k]
    errorcount = 0

    batchsize = 50
    maxvcores = 7
    vcores = len(emlsmpl) / batchsize
    if vcores > maxvcores:
        vcores = maxvcores

    resultiter = para.imap_easy(parse_and_log,
                                emlsmpl,
                                vcores,
                                batchsize,
                                False)
    print 'VCores:\t', vcores
    print 'BatchSize:\t', batchsize
    for i, res in enumerate(resultiter):
        if res is None:
            print 'ERROR!!!!!!!!11'
            errorcount += 1
        else:
            for n, doc in enumerate(res):
                stach.psql_write_to(doc['psql'])
                docid = doc['psql']['uuid']
                print doc['psql']
                ext = ''.join(['-',
                               doc['psql']['org_filename'].split('.')[-1],
                               '.json',
                               ])
                newfname = '/mnt/data1/Case2/parsed/' + docid + ext
                if doc['psql']['use']:
                    with open(newfname, 'w+') as dasfobj:
                        dasfobj.write(doc['s3text']['content'])
                print '\t', i, '\t', n, '\t', doc['psql']['org_filename']
                for key, value in doc['psql'].iteritems():
                    print '\t\t', key, '\t', value
                print '\n'
                print '---------'
                print '\t\t\t', len(''.join([it
                                             for key, value in
                                             doc['psql'].iteritems()
                                             for it in
                                             ('\t\t',
                                              key,
                                              '\t',
                                              unicode(value),
                                              '\n')]))

        print '\n', errorcount, '\t:ERRORCount'
    print '\n' * 2
    print 'VCores:\t', vcores
    print 'BatchSize:\t', batchsize


if __name__ == '__main__':
    try:
        k = int(sys.argv[1])
    except(IndexError):
        k = 0
    try:
        c = int(sys.argv[2])
    except(IndexError):
        c = 0
    try:
        d = sys.argv[3]
    except(IndexError):
        d = '/mnt/data1/enron/enron_mail_20110402/textonly/enron/'
    main(k, d, c)
