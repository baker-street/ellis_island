# -*- coding: utf-8 -*-
__title__ = 'ellis_island'
__author__ = 'Steven Cutting'
__author_email__ = 'steven.c.projects@gmail.com'
__created_on__ = '6/11/2015'
__copyright__ = "ellis_island  Copyright (C) 2015  Steven Cutting"
__credits__ = ["Steven Cutting"]
__license__ = "GPL3"
__maintainer__ = "Steven Cutting"
__email__ = 'steven.c.projects@gmail.com'

import re
import itertools

import pickle
import json
import yaml

import sys
import os
import tempfile
import shutil
import subprocess

import logging
LOG = logging.getLogger(__name__)


if sys.version_info[0] < 3:
    _STRINGTYPES = (basestring,)
else:
    _STRINGTYPES = (str, bytes)


def isiter(y, ignore=_STRINGTYPES):
    """Test if an object is iterable, but not a string type.

    Test if an object is an iterator or is iterable itself. By default this
    does not return True for string objects.

    The `ignore` argument defaults to a list of string types that are not
    considered iterable. This can be used to also exclude things like
    dictionaries or named tuples.
    """
    if ignore and isinstance(y, ignore):
        return False
    try:
        iter(y)
        return True
    except TypeError:
        return False


def containsAll(str_, set_):
    nummatch = len([bit for pat in set_ for bit in re.findall(pat,
                                                              str_,
                                                              re.IGNORECASE)])
    numinset = len(set_)
    return 4 <= nummatch or nummatch > numinset


# TODO (steven_c) refactor out the stupid classes
class Serial():
    @staticmethod
    def pickle_save(filename_, object_):
        with open(filename_, 'wb') as handle:
            pickle.dump(object_, handle)

    @staticmethod
    def pickle_load(filename_):
        with open(filename_, 'rb') as handle:
            return pickle.load(handle)

    @staticmethod
    def pickle_save_iter_uni_to_utf8(file_, iterable):
        Serial.pickle_save(file_, set(i.encode('utf-8') for i in iterable))

    @staticmethod
    def pickle_load_iter_utf8_to_uni(file_):
        return set(i.decode('utf-8') for i in Serial.pickle_load(file_))

    @staticmethod
    def json_save(obj, file_, indent=None):
        with open(file_, 'w+') as fp:
            json.dump(obj, fp, indent=indent)

    @staticmethod
    def json_load(file_):
        with open(file_) as fp:
            return json.load(fp)

    @staticmethod
    def yaml_load(file_):
        with open(file_) as fp:
            return yaml.load(fp)


class FileUtils:
    @staticmethod
    def file_tail(file_, n=1, dropstuff='\n', replacewith=''):
        """
        Get the last n lines of a file.
        """
        return subprocess.check_output(['tail',
                                        '-' + str(n),
                                        file_
                                        ]).replace(dropstuff,
                                                   replacewith).decode('utf-8')


class GeneratorUtils:
    @staticmethod
    def len_gen(gen):
        """
        Finds the length of a generator without loading it all into mem.
        """
        count = 0
        for i in gen:
            count += 1
        return count

    @staticmethod
    def head(stream, n=10):
            """Convenience fnc: return the first `n` elements of the stream, as
            plain list."""
            return list(itertools.islice(stream, n))


class TempDir(object):
    def __init__(self, dirname=None):
        if dirname:
            self.dirname = dirname
        else:
            self.dirname = tempfile.mkdtemp()

    def __enter__(self):
        return self.dirname

    def __str(self):
        return self.dirname

    def __repr__(self):
        return self.dirname

    def __exit__(self, *args):
        try:
            os.removedirs(self.dirname)
        except OSError:
            shutil.rmtree(self.dirname)
