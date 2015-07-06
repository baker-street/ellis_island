# -*- coding: utf-8 -*-
__title__ = 'ellis_island'
__author__ = 'Steven Cutting'
__author_email__ = 'steven.e.cutting@linux.com'
__created_on__ = '6/29/2015'

import psycopg2


def psql_create_table(case=''):
    with psycopg2.connect("dbname='docmeta' user='tester' host='localhost'\
                          password='test12' port='2345'") as conn:
        curs = conn.cursor()
        # curs.execute('DROP TABLE IF EXISTS docmeta')
        table = 'docmeta'
        if case:
            tablename = table + case
        else:
            tablename = table
        curs.execute("""CREATE TABLE IF NOT EXISTS
                        {table}(uuid TEXT PRIMARY KEY,
                                type TEXT,
                                mime TEXT,
                                info TEXT,
                                generation INTEGER,
                                parent TEXT,
                                use BOOL,
                                raw_pointer TEXT,
                                text_pointer TEXT,
                                org_filename TEXT)
                     """.format(table=tablename))


def psql_write_to(load, case=''):
    with psycopg2.connect("dbname='docmeta' user='postgres' host='localhost'\
                          password='test12' port='2345'") as conn:
        table = 'docmeta'
        if case:
            tablename = table + case
        else:
            tablename = table
        curs = conn.cursor()
        curs.execute('''INSERT INTO {table}(uuid,
                                            type,
                                            mime,
                                            info,
                                            generation,
                                            parent,
                                            use,
                                            raw_pointer,
                                            text_pointer,
                                            org_filename)
                     VALUES(%(uuid)s,
                            %(type)s,
                            %(mime)s,
                            %(info)s,
                            %(generation)s,
                            %(parent)s,
                            %(use)s,
                            %(raw_pointer)s,
                            %(text_pointer)s,
                            %(org_filename)s)'''.format(table=tablename),
                     load,
                     )
