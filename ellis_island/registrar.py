# -*- coding: utf-8 -*-
__title__ = 'ellis_island'
__author__ = 'Steven Cutting'
__author_email__ = 'steven.c.projects@gmail.com'
__created_on__ = '6/24/2015'

from uuid import uuid4
from collections import Iterable
from time import strftime
from json import dumps

from gentrify.parse import parse_multi_layer_file


import logging
LOG = logging.getLogger(__name__)


def gen_uuid_list(n, uuid_func=uuid4):
    return (unicode(uuid_func()) for _ in xrange(n))


# TODO (steven_c) Make less complex
def registrar_nested(uri,
                     parentuuid=None,
                     parentdatetime=None,
                     currentgeneration=0,
                     _parseddoclist=None,
                     parse_func=parse_multi_layer_file,
                     dateadded=unicode(strftime('%Y-%m-%d'))):
    """
    Registers all, even those that won't be used for further processing.
    """
    generation = currentgeneration
    if not isinstance(_parseddoclist, Iterable):
        parseddoclist = parse_func(uri)
    else:
        parseddoclist = _parseddoclist
    uuidlist = list(gen_uuid_list(len(parseddoclist)))
    for i, parseddoc in enumerate(parseddoclist):
        docid = uuidlist[i]
        try:
            metadata = {u'type': parseddoc[u'type'],
                        u'mime': parseddoc[u'mime'],
                        u'info': parseddoc[u'info'],
                        # for the sake of the child docs use the filename
                        # supplied by the parsing func, vs uri.
                        u'org_filename': parseddoc[u'filename'],
                        u'use': bool(parseddoc[u'content'][u'body']),
                        u'uuid': docid,
                        u'parent': parentuuid,
                        u'generation': generation,
                        u'date_added': dateadded
                        }
            try:
                metadata['datetime'] = parseddoc[u'content'][u'datetime']
            except(KeyError):
                metadata['datetime'] = parentdatetime
        except(TypeError):
            for doc in registrar_nested(uri,
                                        parentuuid=uuidlist[0],
                                        parentdatetime=parentdatetime,
                                        generation=generation,
                                        parseddoc=parseddoc,
                                        parse_func=parse_func,
                                        dateadded=dateadded):
                yield doc
        if i == 0:
            metadata['children'] = dumps(uuidlist[1:], indent=4)
            parentuuid = uuidlist[0]
            try:
                parentdatetime = parseddoc[u'content'][u'datetime']
            except(KeyError):
                parentdatetime = ''
            generation += 1
        else:
            try:
                metadata['children']
            except(KeyError):
                metadata['children'] = dumps([], indent=4)
        parseddoc['content'].pop('rawbody', None)
        yield {u'uuid': docid,
               u'parsed_doc': parseddoc,
               u'metadata': metadata,
               }
