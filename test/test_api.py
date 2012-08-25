#!/usr/bin/env python
# -*- coding: utf-8 -*-
#Author: Tim Henderson
#Email: tim.tadh@gmail.com
#For licensing see the LICENSE file in the top level directory.

from hbasta.api import Client

client = Client('localhost', 9090)
table = '__test__'

def create():
    client.create_table(table, ['x', 'y', 'z'])

def drop():
    try:
        client.disable_table(table)
        client.drop_table(table)
    except:
        client.disable_table(table)
        client.drop_table(table)

def test_create_table():
    create()
    drop()

def test_get_table_names():
    create()
    try:
        assert table in client.get_table_names()
    finally:
        drop()

def test_add_row():
    create()
    try:
        client.add_row(table, 1, {'x':'a', 'y':'b', 'z':'c'})
    finally:
        drop()

def test_get_row():
    create()
    try:
        client.add_row(table, '1', {'x':'a', 'y':'b', 'z':'c'})
        assert client.get_row(table, '1') == {'x:':'a', 'y:':'b', 'z:':'c'}
    finally:
        drop()

def test_get_row_colspec():
    create()
    try:
        client.add_row(table, 1, {'x':'a', 'y':'b', 'z':'c'})
        assert client.get_row(table, 1, ('x',)) == {'x:':'a'}
    finally:
        drop()

def test_delete_row():
    create()
    try:
        client.add_row(table, '1', {'x':'a', 'y':'b', 'z':'c'})
        assert client.get_row(table, '1') == {'x:':'a', 'y:':'b', 'z:':'c'}
        client.delete_row(table, '1')
        assert len([ row for row in client.scan(table, ('x',), '0') ]) == 0
    finally:
        drop()

def test_scan_1_row():
    create()
    try:
        client.add_row(table, '1', {'x':'a', 'y':'b', 'z':'c'})
        row = client.scan(table, ('x',), '1').next()
        assert row == ('1', {'x:':'a'})
    finally:
        drop()

def test_scan_100_rows():
    create()
    try:
        for id in xrange(100):
            client.add_row(table, id, {'x':'a', 'y':'b', 'z':'c'})
        ids = set(id for id, cols in client.scan(table, ('x',), 0))
        print ids
        assert ids == set(id for id in xrange(100))
    finally:
        drop()

def test_scan_prefix():
    create()
    try:
        for id in xrange(100):
            client.add_row(table, str(id), {'x':'a', 'y':'b', 'z':'c'})
        ids = set(id for id, cols in client.scan(table, ('x',), start_prefix='1'))
        assert ids == set(str(id) for id in xrange(10, 20)) | set(['1'])
    finally:
        drop()

def test_scan_stop():
    create()
    try:
        for id in xrange(100):
            client.add_row(table, id, {'x':'a', 'y':'b', 'z':'c'})
        ids = set(id for id, cols in client.scan(table, ('x',), 0, stop_row=20))
        assert ids == set(xrange(20))
    finally:
        drop()

