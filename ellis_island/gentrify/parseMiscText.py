__title__ = 'ellis_island'
__author__ = 'Steven Cutting'
__author_email__ = 'steven.c.projects@gmail.com'
__created_on__ = '6/12/2015'


import bs4
from bs4 import BeautifulSoup

import logging
LOG = logging.getLogger(__name__)


def is_html(html, test=True):
    if test and not '<html>' in html:
        return html
    return BeautifulSoup(html).text
