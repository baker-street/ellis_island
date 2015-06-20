# -*- coding: utf-8 -*-
__title__ = 'ellis_island'
__author__ = 'Steven Cutting'
__author_email__ = 'steven.c.projects@gmail.com'
__created_on__ = '6/12/2015'


from email.Header import decode_header
import email
import sys
from email.Parser import Parser as EmailParser
from email.utils import parseaddr
# cStringIOはダメ
from StringIO import StringIO

from ellis_island.gentrify import fixEncoding


"""
def parse_attachment(message_part):
    content_disposition = message_part.get("Content-Disposition", None)
    #print content_disposition
    if content_disposition:
        dispositions = content_disposition.strip().split(";")
        if bool(content_disposition and
                dispositions[0].lower() == "attachment"):

            file_data = message_part.get_payload(decode=True)
            attachment = StringIO(file_data)
            attachment.content_type = message_part.get_content_type()
            attachment.size = len(file_data)
            attachment.name = None
            attachment.create_date = None
            attachment.mod_date = None
            attachment.read_date = None

            for param in dispositions[1:]:
                name, value = param.split("=")
                name = name.lower()

                if name == "filename":
                    attachment.name = value
                elif name == "create-date":
                    attachment.create_date = value  # TODO: datetime
                elif name == "modification-date":
                    attachment.mod_date = value  # TODO: datetime
                elif name == "read-date":
                    attachment.read_date = value  # TODO: datetime
            return attachment
    return None
"""


def parse_attachment(message_part):
    content_disposition = message_part.get("Content-Disposition", None)
    try:
        print content_disposition.replace('%20', ' ')
    except AttributeError:
        print content_disposition
    if content_disposition:
        dispositions = content_disposition.strip().split(";")
        if bool(content_disposition and
                dispositions[0].lower() == "attachment"):

            file_data = message_part.get_payload(decode=True)
            attachment = StringIO(file_data)
            attachment.content_type = message_part.get_content_type()
            attachment.size = len(file_data)
            attachment.name = None
            attachment.create_date = None
            attachment.mod_date = None
            attachment.read_date = None

            for param in dispositions[1:]:
                name, value = param.split("=")
                name = name.lower()

                if name == "filename":
                    attachment.name = value
                elif name == "create-date":
                    attachment.create_date = value  # TODO: datetime
                elif name == "modification-date":
                    attachment.mod_date = value  # TODO: datetime
                elif name == "read-date":
                    attachment.read_date = value  # TODO: datetime
            return attachment
    return None


def parse(content):
    """
    Eメールのコンテンツを受け取りparse,encodeして返す
    """
    msgobj = email.message_from_string(content)
    """
    if msgobj['Subject'] is not None:
        decodefrag = decode_header(msgobj['Subject'])
        subj_fragments = []
        for s , enc in decodefrag:
            if enc:
                s = unicode(s , enc).encode('utf8','replace')
            subj_fragments.append(s)
        subject = ''.join(subj_fragments)
    else:
        subject = None
    """
    subject = fixEncoding.make_unicode_dang_it(msgobj['Subject'])
    attachments = []
    body = None
    html = None
    for part in msgobj.walk():
        attachment = parse_attachment(part)
        if attachment:
            attachments.append(attachment)
        elif part.get_content_type() == "text/plain":
            if body is None:
                body = ""
            body += unicode(part.get_payload(decode=True),
                            part.get_content_charset(),
                            'replace')
        elif part.get_content_type() == "text/html":
            if html is None:
                html = ""
            html += fixEncoding.make_unicode_dang_it(part.get_payload(
                                                     decode=True))
    return {'subject': subject,
            'body': body,
            'html': html,
            'from': parseaddr(msgobj.get('From'))[1],
            #  名前は除いてメールアドレスのみ抽出
            'to': parseaddr(msgobj.get('To'))[1],
            #  名前は除いてメールアドレスのみ抽出
            'attachments': attachments,
            'obj': msgobj,
            }
