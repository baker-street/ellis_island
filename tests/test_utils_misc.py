# -*- coding: utf-8 -*-
__title__ = 'ellis_island'
__author__ = 'Steven Cutting'
__author_email__ = 'steven.c.projects@gmail.com'
__created_on__ = '7/8/2015'
__copyright__ = "ellis_island  Copyright (C) 2015  Steven Cutting"
__credits__ = ["Steven Cutting"]
__license__ = "GPL3"
__maintainer__ = "Steven Cutting"
__email__ = 'steven.c.projects@gmail.com'

from ellis_island.utils.misc import get_default_data_key


def test_get_default_data_key():
    key = get_default_data_key()
    assert key == 'LVlwUZaAf66MM_OYbuaxt--ssO0XrgE-0K0GFdjszmc='
