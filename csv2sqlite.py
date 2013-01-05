#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Convert CSV format file to SQLite
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, REAL

from clitool.cli import climain
from unicodecsv import UnicodeReader

HEADER = (
    "prefecture_code",
    "prefecture",
    "city_code",
    "city",
    "street_code",
    "street",
    "lat",
    "lon",
    "document_code",
    "category_code"
)


Session = sessionmaker()
Base = declarative_base()


class Address(Base):

    __tablename__ = 'tokyo_street'
    #__table_args__ = {'sqlite_autoincrement' : True}

    id = Column(Integer, primary_key=True)
    address = Column(String)
    prefecture_code = Column(String)
    prefecture = Column(String)
    city_code = Column(String)
    city = Column(String)
    street_code = Column(String)
    street = Column(String)
    lat = Column(REAL)
    lon = Column(REAL)
    document_code = Column(Integer)
    category_code = Column(Integer)


def csv2sqlite(fname, input_encoding, output):
    #dsl = 'sqlite:///:memory:'
    dsl = 'sqlite:///tokyo.street.db'
    engine = create_engine(dsl, echo=True)
    Base.metadata.create_all(engine)
    Session.configure(bind=engine)
    session = Session()

    stream = UnicodeReader(fname, encoding=input_encoding)
    stream.next()  # skip header line
    for row in stream:
        record = dict(zip(HEADER, row))
        r = Address(
            address=record['prefecture'] + record['city'] + record['street'],
            prefecture_code=record['prefecture_code'],
            prefecture=record['prefecture'],
            city_code=record['city_code'],
            city=record['city'],
            street_code=record['street_code'],
            street=record['street'],
            lat=float(record['lat']),
            lon=float(record['lon']),
            document_code=int(record['document_code']),
            category_code=int(record['category_code'])
        )
        session.add(r)
    session.commit()


@climain
def main(files, input_encoding, output):
    for fname in files:
        csv2sqlite(fname, input_encoding, output)


if __name__ == '__main__':
    main()


# vim: set et ts=4 sw=4 cindent fileencoding=utf-8 :
