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
from os.path import join
from json import dumps
from hashlib import md5
from functools import partial

from gentrify.fixEncoding import make_byte
from ellis_island.utils.misc import pass_through

if sys.version_info[0] < 3:
    byte_it = pass_through
else:
    byte_it = partial(make_byte, hohw=True)


# TODO (steven_c) Consider moving encryption option here
def prep_for_stash(docdict, project='project', prefix=''):
    # LOG.debug('doc prefix:\t' + prefix)  # Not really needed. Consider rm.
    docuuid = docdict['uuid']
    docext = docdict['metadata']['org_filename'].split('.')[-1]
    rawfname = ''.join([docuuid,
                        u'.',
                        docext,
                        ])
    textname = ''.join([docuuid,
                        # u'-',
                        # docext,  # consider removing for good.
                        '.json',
                        ])
    rawpointer = join(prefix, 'raw/', rawfname)
    textpointer = join(prefix, 'text/', textname)
    textserializedcntnt = dumps(docdict['parsed_doc']['content'],
                                indent=4)
    rawcontent = docdict['parsed_doc']['rawbody']  # .encode('utf-8')
    newmetadata = docdict['metadata'].copy()
    newmetadata.update({'raw_pointer': rawpointer,
                        'text_pointer': textpointer,
                        # 'text_checksum': md5(byte_it(textserializedcntnt)
                        #                      ).hexdigest(),
                        # 'raw_checksum': md5(byte_it(rawcontent)  # consider rm
                        #                     ).hexdigest(),
                        })
    return {'uuid': docuuid,
            'raw': {'pointer': rawpointer,
                    'content': rawcontent,
                    },
            'text': {'pointer': textpointer,
                     'content': textserializedcntnt,
                     },
            'meta': newmetadata,
            }


def prep_many_for_stash(docdictiterable, project='project', prefix=''):
    for docdict in docdictiterable:
        yield prep_for_stash(docdict=docdict, project=project, prefix=prefix)


def prep_many_for_stash_list(docdictiterable, project='project', prefix=''):
    return [prep_for_stash(docdict=docdict, project=project, prefix=prefix)
            for docdict in docdictiterable]
