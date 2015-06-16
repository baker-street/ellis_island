# -*- coding: utf-8 -*-
__title__ = 'chevy'
__author__ = 'Steven Cutting'
__author_email__ = 'steven.c.projects@gmail.com'
__created_on__ = '6/12/2015'


from subprocess import Popen, PIPE
from docx import Document
# from xlrd import XLRDError
import pandas
import textract
import bs4
from bs4 import BeautifulSoup
# http://stackoverflow.com/questions/5725278/python-help-using-pdfminer-as-a-library
import pdfminer
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
from cStringIO import StringIO


import zipfile


import logging
LOG = logging.getLogger(__name__)


KEEPEXTENSIONS = set(['.doc',
                      '.docx',
                      # '.xls',
                      # '.xlsx',
                      '.pdf',
                      '.zip',
                      # '.odt',
                      '.rtf',
                      '.txt',
                      '.eml',
                      ])


def is_html(html):
    if '<html>' in html:
        return BeautifulSoup(html).text


def handle_zip_files(zippath, tmpdir):
    with zipfile.ZipFile(zippath) as daszip:
        namelist = daszip.namelist()
        for name in namelist:
            daszip.extract(name, path=tmpdir)
        return namelist


def auto_textract(filepath):
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


def pandas_print_full(x):
    pandas.set_option('display.max_rows', len(x))
    g = str(x)
    pandas.reset_option('display.max_rows')
    return g


def convert_pdf_to_txt(path):
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


def document_to_text(filepath):
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
            return convert_pdf_to_txt(filepath).replace('\x0c\x0c', '')
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
        return auto_textract(filepath)


def extract_text(file_):
    filename = file_.split('/')[-1]
    filepath = file_
    if (len(filename) > 4) and ('.' in filename[-5:]):
        return document_to_text(filepath)
    else:
        return None


# def default_file_filter_by_name(filepath, filepathlist, todir, keepextension):
#    savetofile = Path(todir).joinpath(filepath)
#    todirexisting = set(f for f in dirs.spelunker_gen(todir))
