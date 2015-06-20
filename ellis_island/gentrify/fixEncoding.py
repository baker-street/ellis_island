# -*- coding: utf-8 -*-
__title__ = 'ellis_island'
__author__ = 'Steven Cutting'
__author_email__ = 'steven.c.projects@gmail.com'
__created_on__ = '6/12/2015'


try:
    import cchardet as chardet
except ImportError:
    import chardet
import unicodedata
from unidecode import unidecode
from ftfy import ftfy


import logging
LOG = logging.getLogger(__name__)


# ----------------------------------------------------------------------------
# Cleaning up text.
def sane_unicode_bom(bytes_, errors='replace', returnencoding=False):
    """
    Convert a byte string into Unicode.
    By checking for a BOM, and if one is found returns
    the Unicode text minus the BOM.
    """
    if isinstance(bytes_, unicode):
        return bytes_

    encoding_map = (("\xef\xbb\xbf", "utf-8"),
                    ("""\xff\xfe\0\0""", "utf-32"),
                    ("\0\0\xfe\xff", "UTF-32BE"),
                    ("\xff\xfe", "utf-16"),
                    ("\xfe\xff", "UTF-16BE"))
    for bom, encoding in encoding_map:
        if bytes.startswith(bom):
            unitext = unicode(bytes[len(bom):],
                              encoding,
                              errors=errors)
            if returnencoding:
                return (unitext, encoding)
            else:
                return unitext


def sane_unicode(bytes_, encoding='utf-8', errors='replace',
                 returnencoding=False):
    """
    Convert a byte string into Unicode
    using chardet.
    """
    if isinstance(bytes_, unicode):
        return bytes_

    try:
        unitext = bytes_.decode(encoding)
    except UnicodeDecodeError:
        detection = chardet.detect(bytes_)
        encoding = detection.get('encoding')
        unitext = unicode(bytes_, encoding, errors=errors)

    if returnencoding:
        return unitext, encoding
    else:
        return unitext


def insane_unicode(bytes_, *args):
    """
    return the unicode representation of string.
    !! Use as last resort!!
    """
    if isinstance(bytes_, unicode):
        return bytes_
    try:
        return unicode(bytes_, *args)
    except UnicodeDecodeError:
        # obj is byte string
        ascii_text = str(bytes_).encode('string_escape')
        try:
            return sane_unicode(ascii_text)
        except UnicodeDecodeError:
            return insane_unicode(ascii_text)


def insane_str(obj, encoding='utf-8'):
    """
    Return the byte string representation of string.
    !! Use as last resort!!
    """
    try:
        return str(obj)
    except UnicodeEncodeError:
        # obj is unicode
        return insane_unicode(unicode(obj
                                      ).encode('unicode_escape')
                              ).encode(encoding)


def normize_text(text, asciionly=False):
    """
    Normalizes characters and converts all to
    best matching ASCII representation.

    Expects text to be Unicode.

    """
    s = text.replace(u'\r', u'')
    # Consider not replacing tabs with spaces
    # s = s.replace(u'\t', u' '*4)  # TODO (steven_c): find out why tabs are
    #                               # not being removed
    s = s.replace(u'\f', u' ')
    s = s.replace(u"\u2019", u"'")   # Handles windows other single quote.
    s = s.replace(u'0xcc', u' ')
    s = s.replace(u'\xe9', u'e')  # accented e
    b = ftfy(s)
    b = unicodedata.normalize('NFD', b)
    if asciionly:
        b = unidecode(s).decode('utf-8')
        b = b.encode('ascii', 'ignore').decode('ascii')
    return sane_unicode(b)


def make_unicode_n_norm(text, returnencoding=False, asciionly=False):
    if returnencoding:
        uni = sane_unicode(text, returnencoding)
        return (normize_text(uni[0], asciionly), uni[1])
    else:
        return normize_text(sane_unicode(text), asciionly)


def make_unicode_dang_it(text,
                         encoding='utf-8',
                         errors='replace',
                         normize=False,
                         asciionly=False,
                         returnencoding=False):
    """
    Make text unicode Hell or High Water.

    +  normize: if True, normalizes the unicode characters.
    +  asciionly: if True changes all characters to their closest
            ASCII representation. Only applies if normize is True.
    """

    if isinstance(text, unicode):
        encoding = u'unicode'
        uni = text
    else:
        try:
            uni, encoding = sane_unicode(text,
                                         encoding,
                                         errors,
                                         returnencoding=True)
        except UnicodeDecodeError:
            uni, encoding = (insane_unicode(text), '')
        if normize:
            uni = normize_text(uni, asciionly)
    if returnencoding:
        return uni, encoding
    else:
        return uni


def make_byte(text, hohw=False, normize=False, asciionly=False,
              encoding='utf-8'):
    """
    Used for both changing a byte stings encoding and for changing a unicode
    string to byte.

    Default output encoding is UTF-8.

    hohw : Hell or High Water mode
    """
    if hohw:
        uni = make_unicode_dang_it(text,
                                   normize=normize,
                                   asciionly=asciionly)
    else:
        if normize:
            uni = make_unicode_n_norm(text,
                                      asciionly=asciionly)
        else:
            uni = sane_unicode(text)
    return uni.encode(encoding)
