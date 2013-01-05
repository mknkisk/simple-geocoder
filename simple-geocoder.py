#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Simple geo coding tool is Japan address data
   Usage: python simple-geocoder.py 東京都港区
"""

import argparse
import json

from csv2sqlite import Address
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('word', metavar='W', type=str, nargs=1, help='keyword')
    args = parser.parse_args()
    return args


def search(word):
    Session = sessionmaker()
    Base = declarative_base()

    dsl = 'sqlite:///tokyo.street.db'
    engine = create_engine(dsl, echo=True)
    Base.metadata.create_all(engine)
    Session.configure(bind=engine)
    session = Session()

    items = []
    for row in session.query(Address).filter(
        Address.address.like('%' + unicode(word, 'utf-8') + '%')):

        items.append(
            {'address': row.address, 'lat': row.lat, 'lon': row.lon})

    results = {'results': items}
    print(json.dumps(results, indent=4, sort_keys=True, ensure_ascii=False))


def main():
    args = parse_args()
    search(args.word[0])


if __name__ == '__main__':
    main()


# vim: set et ts=4 sw=4 cindent fileencoding=utf-8 :
