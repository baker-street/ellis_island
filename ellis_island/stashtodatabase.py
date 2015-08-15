# -*- coding: utf-8 -*-
__title__ = 'ellis_island'
__author__ = 'Steven Cutting'
__author_email__ = 'steven.c.projects@gmail.com'
__created_on__ = '7/13/2015'

from os import getenv

import dataset
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import(Column,
                       Integer,
                       String,
                       Text,
                       Boolean,
                       DateTime,
                       Date,
                       BLOB)
from sqlalchemy.dialects.postgres import JSON
from sqlalchemy.exc import NoSuchTableError

from ellis_island.utils.misc import get_default_data_key


def default_create_table_sqlalchemy(uri, tablename='metatable'):
    engine = create_engine(uri, convert_unicode=True, encoding='utf-8')
    Base = declarative_base()

    class MetaData(Base):
        __tablename__ = tablename

        uuid = Column(String(length=36), primary_key=True)
        type = Column(Text, nullable=False)
        mime = Column(Text)
        info = Column(Text)
        generation = Column(Integer, nullable=False)
        parent = Column(Text)
        use = Column(Boolean, nullable=False)
        raw_pointer = Column(Text)
        text_pointer = Column(Text)
        org_filename = Column(Text)
        if 'sqlite' in uri:
            datetime = Column(Text)
            date_added = Column(Text, nullable=False)
        else:
            datetime = Column(DateTime)
            date_added = Column(Date, nullable=False)
        if 'psql' in uri or 'postgresql' in uri:
            children = Column(JSON)
        elif 'sqlite' in uri:
            children = Column(BLOB)
        else:
            children = Column(Text)
        raw_checksum = Column(Text)
        text_checksum = Column(Text)

    Base.metadata.create_all(engine)


class SQLStash(object):
    def __init__(self, uri,
                 table=getenv('METADATA_TABLE_NAME', 'docmetadata'),
                 chuncksize=500,
                 types={'uuid': String(36),
                        'type': Text,
                        'mime': Text,
                        'info': Text,
                        'generation': Integer,
                        'parent': Text,
                        'use': Boolean,
                        'raw_pointer': Text,
                        'text_pointer': Text,
                        'org_filename': Text,
                        },
                 encrypt=False,
                 encryptkey=getenv('DAS_ENCRYPT_KEY', get_default_data_key()),
                 index=False, indexcolumns=[['uuid', 'type'],
                                            ['uuid', 'generation'],
                                            ['uuid', 'parent'],
                                            ['uuid', 'use'],
                                            ['uuid'],
                                            ['type'],
                                            ['parent'],
                                            ['use'],
                                            ['generation'],
                                            ]):
        self.uri = uri
        self.chuncksize = chuncksize
        self.table = table
        self.types = types
        self.index = index
        self.indexcolumns = indexcolumns
        self.conn = dataset.connect(uri, reflect_metadata=False,
                                    engine_kwargs={'pool_recycle': 3600,
                                                   'convert_unicode': True,
                                                   'encoding': 'utf-8',
                                                   })
        self.conn.begin()
        try:
            self.tbl = self.conn.load_table(self.table)
        except NoSuchTableError:
            self.tbl = self.conn.get_table(self.table,
                                           primary_id='uuid',
                                           primary_type='String(36)')
        if bool(self.index) and bool(self.indexcolumns):
            if isinstance(self.indexcolumns[0], basestring):
                self.tbl.create_index(columns=self.indexcolumns)
            elif isinstance(self.indexcolumns[0], list):
                for ic in self.indexcolumns:
                    self.tbl.create_index(columns=ic)
        self.stack = []

    def flush_the_stack(self):
        self.tbl.insert_many(rows=self.stack,
                             chunk_size=self.chuncksize,
                             types=self.types)
        self.conn.commit()
        self.conn.begin
        self.stack = []

    def stash(self, datumdict):
        self.stack.append(datumdict)
        if len(self.stack) >= self.chuncksize:
            self.flush_the_stack()

    def close(self):
        self.flush_the_stack()
        self.conn.commit()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()
