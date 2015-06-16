# -*- coding: utf-8 -*-
__title__ = 'ellis_island'
__author__ = 'Steven Cutting'
__author_email__ = 'steven.c.projects@gmail.com'
__created_on__ = '6/11/2015'


# import regex
# import re

# from ellis_island import utils
from ellis_island.utils import isiter

import logging
LOG = logging.getLogger(__name__)


class ImmigrantRecep(object):
    """
    filetypes can be:
        - Iterable (e.g. list) of extensions (strings).
        - A comma separated list within a string.
        - A single extension within a string.
    """
    def __init__(self, uri=u'', filetypes='*', sep=',', docAsString=u''):
        self.uri = uri
        self.filetypes = ImmigrantRecep._norm_filetypes(filetypes, sep)
        self.sep = sep
        if docAsString:
            self.docAsString = docAsString
        elif uri:
            self.docAsString = get_doc_text(self.uri)  # place holder --- !!!!!!

    @staticmethod
    def _norm_filetypes(self, ftypes, sep):
        """
        ftypes can be: iterable container (e.g. list), or string.
        sep must be a string.
        Returns a set of Unicode strings.
        """
        if isinstance(ftypes, str) and isinstance(sep, str):
            if sep in ftypes:
                return set(ftypes.split(sep))
            else:
                return set([ftypes])
        elif isiter(ftypes):
            return set(ftypes)
        else:
            raise ValueError('ftypes is invalid, read doc for EllisIsland')

    @staticmethod
    def _handle_uri(uri, extIgnore):
        pass

    @property
    def doc_header(self):
        pass

    @property
    def doc_text(self):
        pass
