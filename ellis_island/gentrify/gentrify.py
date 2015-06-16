# -*- coding: utf-8 -*-
__title__ = 'ellis_island'
__author__ = 'Steven Cutting'
__author_email__ = 'steven.c.projects@gmail.com'
__created_on__ = '6/12/2015'


from subprocess import Popen, PIPE

from docx import Document
# from xlrd import XLRDError
import pandas
import zipfile
import textract
# http://stackoverflow.com/questions/5725278/python-help-using-pdfminer-as-a-library
import pdfminer
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
from cStringIO import StringIO

import email
import bs4
from bs4 import BeautifulSoup

import logging
LOG = logging.getLogger(__name__)


class TextExtraction:
    """
    Extract from binary files.
    """
    @staticmethod
    def handle_zip_files(self, zippath, tmpdir):
        with zipfile.ZipFile(zippath) as daszip:
            namelist = daszip.namelist()
            for name in namelist:
                daszip.extract(name, path=tmpdir)
            return namelist

    @staticmethod
    def auto_textract(self, filepath):
        """
        Use when failure is not an option.
        If an excetpion is raised by textract, "None" is returned.
        """
        try:
            return textract.process(filepath, language='nor')
        except (textract.exceptions.ExtensionNotSupported,
                IndexError,
                textract.exceptions.ShellError):
            return None

    @staticmethod
    def pandas_print_full(self, x):
        pandas.set_option('display.max_rows', len(x))
        g = str(x)
        pandas.reset_option('display.max_rows')
        return g

    @staticmethod
    def convert_pdf_to_txt(self, path):
        rsrcmgr = PDFResourceManager()
        retstr = StringIO()
        codec = 'utf-8'
        laparams = LAParams()
        device = TextConverter(rsrcmgr, retstr, codec=codec, laparams=laparams)
        fp = file(path, 'rb')
        interpreter = PDFPageInterpreter(rsrcmgr, device)
        password = ""
        maxpages = 0
        caching = True
        pagenos = set()
        for page in PDFPage.get_pages(fp, pagenos, maxpages=maxpages,
                                      password=password, caching=caching,
                                      check_extractable=True):
            interpreter.process_page(page)
        fp.close()
        device.close()
        str = retstr.getvalue()
        retstr.close()
        return str

    @staticmethod
    def document_to_text(self, filepath):
        # ----------------------
        # Text doc files
        if filepath[-4:].lower() == ".doc":
            cmd = ['antiword', filepath]
            p = Popen(cmd, stdout=PIPE)
            stdout, stderr = p.communicate()
            return stdout.decode('ascii', 'ignore')
        elif filepath[-5:].lower() == ".docx":
            document = Document(filepath)
            return '\n\n'.join([graph.text for graph in document.paragraphs])
        elif filepath[-4:].lower() == ".odt":
            return None
            # cmd = ['odt2txt', filepath]
            # p = Popen(cmd, stdout=PIPE)
            # stdout, stderr = p.communicate()
            # return stdout.decode('ascii', 'ignore')
        elif filepath[-4:].lower() == ".rtf":
            cmd = ['unrtf', filepath]
            p = Popen(cmd, stdout=PIPE)
            stdout, stderr = p.communicate()
            return bs4.BeautifulSoup(stdout.decode('ascii', 'ignore')).text
        # ----------------------
        # SpreadSheets
        elif filepath[-4:].lower() in [".xls", ".xlsx"]:
            return None
            # try:
            #     return auto_textract(filepath)
            # except (XLRDError,
            #        IndexError,
            #        AssertionError,
            #        OverflowError):
            #    return None
            # try:
            #    # cmd = ['x_x', filepath]
            #    # p = Popen(cmd, stdout=PIPE)
            #    # stdout, stderr = p.communicate()
            #    # return stdout.decode('ascii', 'ignore')
            #     return pandas_print_full(pandas.read_excel(filepath))
            # except (XLRDError,IndexError,AssertionError,OverflowError):
            #     return None
        # ----------------------
        # Other
        elif filepath[-4:].lower() == ".pdf":
            try:
                return self.convert_pdf_to_txt(filepath
                                                         ).replace('\x0c\x0c',
                                                                   '')
            except (pdfminer.pdfdocument.PDFTextExtractionNotAllowed,
                    pdfminer.pdfdocument.PSEOF,
                    pdfminer.pdfdocument.PDFEncryptionError):
                return None
        # will handle zips another way
        # elif filepath[-4:].lower() == ".zip":
        #    with TempDir() as dastmpdir:
        #        try:
        #            extractedfiles = handle_zip_files(filepath, dastmpdir)
        #            return [document_to_text('{}/{}'.format(dastmpdir,path))
        #                    for path in extractedfiles]
        #        except zipfile.BadZipfile:
        #            return None
        else:
            return self.auto_textract(filepath)

    @staticmethod
    def extract_text(self, file_):
        filename = file_.split('/')[-1]
        filepath = file_
        if (len(filename) > 4) and ('.' in filename[-5:]):
            return self.document_to_text(filepath)
        else:
            return None


class ParseEmail:
    @staticmethod
    def parse_attachment(self, message_part):
        content_disposition = message_part.get("Content-Disposition", None)
        if content_disposition:
            dispositions = content_disposition.strip().split(";")
            if bool(content_disposition and dispositions[0].lower() == "attachment"):

                file_data = message_part.get_payload(decode=True)
                attachment = StringIO(file_data)
                attachment.content_type = message_part.get_content_type()
                attachment.size = len(file_data)
                attachment.name = None
                attachment.create_date = None
                attachment.mod_date = None
                attachment.read_date = None

                for param in dispositions[1:]:
                    name,value = param.split("=")
                    name = name.lower()

                    if name == "filename":
                        attachment.name = value
                    elif name == "create-date":
                        attachment.create_date = value  #TODO: datetime
                    elif name == "modification-date":
                        attachment.mod_date = value #TODO: datetime
                    elif name == "read-date":
                        attachment.read_date = value #TODO: datetime
                return attachment
        return None

    @staticmethod
    def parse(self, content):
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
        subject = bytes2unicode(msgobj['Subject'])
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
                html += bytes2unicode(part.get_payload(decode=True))
        return {'subject' : subject,
                'body' : body,
                'html' : html,
                'from' : parseaddr(msgobj.get('From'))[1], # 名前は除いてメールアドレスのみ抽出
                'to' : parseaddr(msgobj.get('To'))[1], # 名前は除いてメールアドレスのみ抽出
                'attachments': attachments,
                'obj': msgobj,
            }


class ParseTextFile:
    @staticmethod
    def is_html(self, html):
        if '<html>' in html:
            return BeautifulSoup(html).text
