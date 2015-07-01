# -*- coding: utf-8 -*-
__title__ = 'ellis_island'
__author__ = 'Steven Cutting'
__author_email__ = 'steven.c.projects@gmail.com'
__created_on__ = '6/24/2015'

from uuid import uuid4
from collections import Iterable

from gentrify.parse import parse_multi_layer_file


def registrar_nested(uri,
                     parentuuid=None,
                     currentgeneration=0,
                     _parseddoclist=None,
                     parse_func=parse_multi_layer_file):
    """
    Registers all, even those that won't be used for further processing.
    """
    generation = currentgeneration
    parentid = parentuuid
    if not isinstance(_parseddoclist, Iterable):
        parseddoclist = parse_func(uri)
    else:
        parseddoclist = _parseddoclist
    for i, parseddoc in enumerate(parseddoclist):
        docid = unicode(uuid4())
        try:
            metadata = {u'type': parseddoc[u'type'],
                        u'mime': parseddoc[u'mime'],
                        u'info': parseddoc[u'info'],
                        # for the sake of the child docs use the filename
                        # supplied by the parsing func, vs uri.
                        u'org_filename': parseddoc[u'filename'],
                        u'use': bool(parseddoc[u'content'][u'body']),
                        u'uuid': docid,
                        u'parent': parentid,
                        u'generation': generation,
                        }
        except(TypeError):
            yield registrar_nested(uri,
                                   parentid,
                                   generation,
                                   parseddoc,
                                   parse_func)
        if i == 0:
            # metadata[u'generation'] = generation
            parentid = docid
            generation += 1
        # else:
        #     # metadata[u'generation'] = generation
        parseddoc['content'].pop('rawbody', None)
        yield {u'uuid': docid,
               u'parsed_doc': parseddoc,
               u'metadata': metadata,
               }
