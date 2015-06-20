# -*- coding: utf-8 -*-
__title__ = 'ellis_island'
__author__ = 'Steven Cutting'
__author_email__ = 'steven.e.cutting@linux.com'
__created_on__ = '6/20/2015'

import arrow

from email.parser import Parser
from email.Header import decode_header
from email.utils import parseaddr
from base64 import b64decode

from ellis_island.gentrify.fixEncoding import make_unicode_dang_it
from ellis_island.gentrify.utils import normize_datetime_tmzone_north_am

import logging
LOG = logging.getLogger(__name__)


EXTRA_HEADERS = ['thread-index',
                 'message-id',
                 'return-path',
                 ]
EXTRA_ADDRESS_HEADERS = ['bcc',
                         'cc',
                         ]


def email_parse_attachment(msgpart):
    content_disposition = msgpart.get("Content-Disposition", None)
    if content_disposition:
        dispositions = content_disposition.strip().split(";")
        if bool(content_disposition and
                dispositions[0].lower() == "attachment"):
            filedata = msgpart.get_payload()
            try:
                if 'base64' in msgpart.get('Content-Transfer-Encoding',
                                           None).lower():
                    filedata = b64decode(filedata)
            except AttributeError:
                return None
            attachment = {'filedata': filedata,
                          'content_type': msgpart.get_content_type(),
                          'filename': make_unicode_dang_it(msgpart.get_filename(),
                                                           normize=True)
                          }
            for param in dispositions[1:]:
                name, value = param.split("=")
                name = name.strip().lower()
                if name == "filename":
                    attachment['filename'] = value.replace('"', '')
            return attachment
    return None


def email_parse(content,
                extraheaders=EXTRA_HEADERS,
                extraaddress_headers=EXTRA_ADDRESS_HEADERS):
    """
    Returns unicode.

    Converts 'Date' to UTC.
    """
    p = Parser()
    msgobj = p.parsestr(content)
    if msgobj['Subject'] is not None:
        decodefrag = decode_header(msgobj['Subject'])
        subj_fragments = []
        for s, enc in decodefrag:
            if enc:
                s = make_unicode_dang_it(s,
                                         enc,
                                         normize=True)
            subj_fragments.append(s)
        subject = ''.join(subj_fragments)
    else:
        subject = None
    attachments = []
    body_text = u""
    for part in msgobj.walk():
        attachment = email_parse_attachment(part)
        if attachment:
            attachments.append(attachment)
        elif part.get_content_type() == "text/plain":
            body_text += make_unicode_dang_it(part.get_payload(decode=True),
                                              part.get_content_charset(),
                                              'replace',
                                              normize=True)
        # elif part.get_content_type() == "text/html":
        #     body_html += make_unicode_dang_it(part.get_payload(decode=True),
        #                                                   part.get_content_charset(),
        #                                                   'replace',
        #                                                   normize=True)
    try:
        msgbits = {'subject': make_unicode_dang_it(subject, normize=True),
                   'body': body_text,
                   # 'body_html': body_html,
                   'from': tuple([make_unicode_dang_it(addr, normize=True)
                                  for addr in parseaddr(msgobj.get('From'))
                                  ]),
                   'to': [tuple([make_unicode_dang_it(person, normize=True)
                                 for person in parseaddr(bit)])
                          for bit in msgobj['To'].split(',')
                          ],
                   'attachments': attachments,
                   'date': unicode(normize_datetime_tmzone_north_am(
                                   msgobj['date'])),
                   }
    except ValueError:
        raise ValueError('Was not able to parse all required email headers.')
    if extraaddress_headers:
        for field in extraaddress_headers:
            try:
                msgbits[field] = [tuple([make_unicode_dang_it(person,
                                                              normize=True)
                                         for person in parseaddr(bit)])
                                  for bit in msgobj[field].split(',')
                                  ]
            except(KeyError, AttributeError):
                msgbits[field] = [(u'', u'')]
    if extraheaders:
        for field in extraheaders:
            try:
                msgbits[field] = make_unicode_dang_it(msgobj[field])
            except(KeyError, AttributeError):
                msgbits[field] = u''
    return msgbits
