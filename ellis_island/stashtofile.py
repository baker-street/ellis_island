# -*- coding: utf-8 -*-
__title__ = 'ellis_island'
__author__ = 'Steven Cutting'
__author_email__ = 'steven.c.projects@gmail.com'
__created_on__ = '7/13/2015'
__copyright__ = "ellis_island  Copyright (C) 2015  Steven Cutting"
__credits__ = ["Steven Cutting"]
__license__ = "GPL3"
__maintainer__ = "Steven Cutting"
__email__ = 'steven.c.projects@gmail.com'

from os import getenv
import functools

from pathlib import Path
from six.moves.urllib.parse import urlsplit

from ellis_island.utils.misc import pass_through, encrypt_it
from ellis_island.utils.misc import get_default_data_key
from ellis_island.utils.smartopen import ParseUri

import logging
LOG = logging.getLogger(__name__)


class LocalFileStash(object):  # Add support for tar archives.
    """
    Used for streaming multiple files to the local file system.

    parenturi - is the directory where the files will be stored.

    The close method has no effect, it's only for creating a consistent API
        with all of the Stash context managers.
    """
    def __init__(self, parenturi, encrypt=False, removeExtIfEncrypt=True,
                 encryptkey=getenv('DAS_ENCRYPT_KEY', get_default_data_key())):
        self.parentpath = Path(urlsplit(parenturi).path)
        self.encrypt = encrypt
        self.removeExtIfEncrypt = removeExtIfEncrypt
        if not self.parentpath.is_dir():
            self.parentpath.mkdir(parents=True)
        if encrypt:
            self.envelope = functools.partial(encrypt_it, key=encryptkey)
        else:
            self.envelope = pass_through

    def stash(self, datumdict):
        if self.encrypt and self.removeExtIfEncrypt:
            pointer = datumdict['pointer'].split('.')[0]
        else:
            pointer = datumdict['pointer']

        with open(pointer.encode('utf-8'), 'w+') as fp:
            fp.write(self.envelope(datumdict['content']))

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()


class S3FileStash(object):
    """
    parenturi - is the bucket (and 'folder')  where the files will be stored.
    """
    def __init__(self, parenturi, encrypt=False,
                 encryptkey=getenv('DAS_ENCRYPT_KEY', get_default_data_key())):
        pass


def file_stash(parenturi, encrypt=False,
               encryptkey=getenv('DAS_ENCRYPT_KEY', get_default_data_key())):
    """
    parenturi - is the directory or bucket (and 'folder')
                where the files will be stored.

    Returns the appropriate obj for stashing based on the parenturi.
    """
    parseduri = ParseUri(parenturi)
    if parseduri.scheme in {'file'}:
        return LocalFileStash(parenturi, encrypt=encrypt, encryptkey=encryptkey)
    elif parseduri.scheme in {'s3', 's3n'}:
        return S3FileStash(parenturi, encrypt=encrypt, encryptkey=encryptkey)


# change to a function that returns the objects it's trying to wrap.
class FileStash(object):
    """
    Deprecated - use ellis_island.stashtofile.file_stash instead.

    parenturi - is the directory where the files will be stored.
    """
    def __init__(self, parenturi, encrypt=False,
                 encryptkey=getenv('DAS_ENCRYPT_KEY', get_default_data_key())):
        LOG.debug(''.join(['ParentUri:\t', parenturi]))
        self.parsedparenturi = urlsplit(parenturi)
        if self.parsedparenturi.scheme == 's3':
            self.filewritter = S3FileStash(parenturi, encrypt, encryptkey)
        elif not self.parsedparenturi.scheme or self.parsedparenturi == 'file':
            self.filewritter = LocalFileStash(parenturi, encrypt, encryptkey)

    def stash(self, datumdict):
        self.filewritter.stash(datumdict)

    def close(self):
        self.filewritter.close()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()
