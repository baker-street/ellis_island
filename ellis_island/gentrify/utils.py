# -*- coding: utf-8 -*-
__title__ = 'ellis_island'
__author__ = 'Steven Cutting'
__author_email__ = 'steven.e.cutting@linux.com'
__created_on__ = '6/20/2015'


import re
import arrow


def normize_datetime_tmzone_north_am(s, totmz='utc'):
    """
    Pattern:
        ddd, D MMM YYYY HH:mm:ss
    Likes (ex):
        Sat, 20 Jun 2015 09:15:18 -0700
    Will put up with (ex):
        Sat, 20 Jun 2015 09:15:18 MST
    """
    try:
        return arrow.get(s, 'ddd, D MMM YYYY HH:mm:ss Z').to(totmz)
    except(arrow.arrow.parser.ParserError):
        s = re.sub('EDT', '-0400', s, flags=re.IGNORECASE)
        s = re.sub('CST', '-0600', s, flags=re.IGNORECASE)
        s = re.sub('EST', '-0500', s, flags=re.IGNORECASE)
        s = re.sub('CDT', '-0500', s, flags=re.IGNORECASE)
        s = re.sub('MDT', '-0600', s, flags=re.IGNORECASE)
        s = re.sub('MST', '-0700', s, flags=re.IGNORECASE)
        s = re.sub('PDT', '-0700', s, flags=re.IGNORECASE)
        s = re.sub('PST', '-0800', s, flags=re.IGNORECASE)
        return arrow.get(s, 'ddd, D MMM YYYY HH:mm:ss Z').to(totmz)
