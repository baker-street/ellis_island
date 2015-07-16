# -*- coding: utf-8 -*-
__title__ = 'ellis_island'
__author__ = 'Steven Cutting'
__author_email__ = 'steven.c.projects@gmail.com'
__created_on__ = '6/24/2015'


from json import dumps


def s3_and_psql_prep(docdatadict, case, prefix='s3://'):
    docuuid = docdatadict['uuid']
    docext = docdatadict['metadata']['org_filename'].split('.')[-1]
    # docid = ''.join([docdatadict['uuid'],
    #                  u'-',
    #                  docdatadict['metadata']['org_filename'].split('.')[-1],
    #                  u'.json',
    #                  ])
    rawfname = ''.join([docuuid,
                        u'.',
                        docext,
                        ])
    textname = ''.join([docuuid,
                        u'-',
                        docext,
                        '.json',
                        ])
    rawpointer = ''.join([prefix, case, '/', 'raw', '/', rawfname])
    textpointer = ''.join([prefix, case, '/', 'text', '/', textname])
    newmetadata = docdatadict['metadata'].copy()
    newmetadata.update({'raw_pointer': rawpointer,
                        'text_pointer': textpointer,
                        })
    return {'uuid': docuuid,
            'raw': {'pointer': rawpointer,
                    'content': docdatadict['parsed_doc']['rawbody'],
                    },
            'text': {'pointer': textpointer,
                     'content': dumps(docdatadict['parsed_doc']['content'],
                                      indent=4),
                     },
            'meta': newmetadata,
            }
