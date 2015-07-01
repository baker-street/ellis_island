# -*- coding: utf-8 -*-
__title__ = 'ellis_island'
__author__ = 'Steven Cutting'
__author_email__ = 'steven.c.projects@gmail.com'
__created_on__ = '6/24/2015'


from json import dumps


def s3_and_psql_prep(docdatadict, case):
    docid = ''.join([docdatadict['uuid'],
                     u'-',
                     docdatadict['metadata']['org_filename'].split('.')[-1],
                     u'.json',
                     ])
    rawpointer = ''.join(['s3://', case, '/', 'raw', '/', docid])
    textpointer = ''.join(['s3://', case, '/', 'text', '/', docid])
    newmetadata = docdatadict['metadata'].copy()
    newmetadata.update({'raw_pointer': rawpointer,
                        'text_pointer': textpointer,
                        })
    return {'uuid': docid,
            's3raw': {'raw_pointer': rawpointer,
                      'content': docdatadict['parsed_doc']['rawbody'],
                      },
            's3text': {'text_pointer': textpointer,
                       'content': dumps(docdatadict['parsed_doc']['content'],
                                        indent=4),
                       },
            'psql': newmetadata,
            }
