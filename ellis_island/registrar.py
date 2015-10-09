# -*- coding: utf-8 -*-
__title__ = 'ellis_island'
__author__ = 'Steven Cutting'
__author_email__ = 'steven.c.projects@gmail.com'
__created_on__ = '6/24/2015'
__copyright__ = "ellis_island  Copyright (C) 2015  Steven Cutting"
__credits__ = ["Steven Cutting"]
__license__ = "GPL3"
__maintainer__ = "Steven Cutting"
__email__ = 'steven.c.projects@gmail.com'

import logging
LOG = logging.getLogger(__name__)

import sys
from uuid import uuid4
from collections import Iterable
from time import strftime
from json import dumps

from estratto.parse import parse_multi_layer_file
from estratto.utils import flatten_array_like_strct_gen

if sys.version_info[0] < 3:
    _STRINGTYPES = (basestring,)
else:
    # temp fix, so that 2.7 support wont break
    unicode = str  # adjusting to python3
    xrange = range
    _STRINGTYPES = (str, bytes)


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
                        u'id': docid,
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
        if metadata[u'type'] == 'email':  # TODO (steven_c) this does not belong
            # TODO (steven_c) move this out, shouldn't have doc specific
            parsedcon = parseddoc['content']
            metadata[u'extra'
                     ] = {k:
                          list(flatten_array_like_strct_gen(parsedcon.get(k,
                                                                          [])))
                          for k in ('from', 'to', 'cc', 'bcc')}
            metadata[u'extra'].update({k: parsedcon.get(k, [])
                                       for k in ('message-id', 'thread-index')})
        else:
            metadata[u'extra'] = dict()
        yield {u'id': docid,
               u'parsed_doc': parseddoc,
               u'metadata': metadata,
               }
