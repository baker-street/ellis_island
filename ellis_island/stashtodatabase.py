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
from sqlalchemy.dialects.postgresql import JSON


def default_create_table_sqlalchemy(uri, tablename='metatable'):
    engine = create_engine(uri, convert_unicode=True, encoding='utf-8')
    Base = declarative_base()

    class MetaData(Base):
        __tablename__ = tablename

        id = Column(String(length=36), primary_key=True)
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
            extra = Column(JSON)
        elif 'sqlite' in uri:
            children = Column(BLOB)
            extra = Column(BLOB)
        else:
            children = Column(Text)
            extra = Column(Text)
        raw_checksum = Column(Text)
        text_checksum = Column(Text)

    Base.metadata.create_all(engine)
