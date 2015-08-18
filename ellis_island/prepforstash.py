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

from json import dumps
from hashlib import md5


# TODO (steven_c) Consider moving encryption option here
def prep_for_stash(docdict, project='project', prefix=''):
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
    rawpointer = ''.join([prefix, '/', 'raw', '/', rawfname])
    textpointer = ''.join([prefix, '/', 'text', '/', textname])
    textserializedcontent = dumps(docdict['parsed_doc']['content'],
                                  indent=4)
    rawcontent = docdict['parsed_doc']['rawbody']
    newmetadata = docdict['metadata'].copy()
    newmetadata.update({'raw_pointer': rawpointer,
                        'text_pointer': textpointer,
                        'text_checksum': md5(textserializedcontent).hexdigest(),
                        'raw_checksum': md5(rawcontent).hexdigest(),
                        })
    return {'uuid': docuuid,
            'raw': {'pointer': rawpointer,
                    'content': rawcontent,
                    },
            'text': {'pointer': textpointer,
                     'content': textserializedcontent,
                     },
            'meta': newmetadata,
            }


def prep_many_for_stash(docdictiterable, project='project', prefix=''):
    for docdict in docdictiterable:
        yield prep_for_stash(docdict=docdict, project=project, prefix=prefix)


def prep_many_for_stash_list(docdictiterable, project='project', prefix=''):
    return [prep_for_stash(docdict=docdict, project=project, prefix=prefix)
            for docdict in docdictiterable]
