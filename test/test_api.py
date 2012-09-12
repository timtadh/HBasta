#!/usr/bin/env python
# -*- coding: utf-8 -*-
#Author: Tim Henderson
#Email: tim.tadh@gmail.com
#For licensing see the LICENSE file in the top level directory.

import sys, os, time
sys.path.insert(0, '/home/hendersont/code/HBasta')

import nose

from hbasta.api import Client
from hbasta import api 


client = Client('localhost', 9090)
table = '__test__'

def create():
    try:
        client.create_table(table, ['x', 'y', 'z'])
    except:
        client.disable_table(table)
        client.drop_table(table)
        client.create_table(table, ['x', 'y', 'z'])

def drop():
    global client
    client.stop_caching()
    tries = 1
    while tries > 0:
        try:
            client.disable_table(table)
            client.drop_table(table)
        except:
            tries -= 1
            if tries > 0:
                time.sleep(5)
                client = Client('localhost', 9090)

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
        client.start_caching()
        assert client.get_row(table, '1') == {'x':'a', 'y':'b', 'z':'c'}
        assert client.get_row(table, '1') == {'x':'a', 'y':'b', 'z':'c'}
        assert client.get_row(table, '1') == {'x':'a', 'y':'b', 'z':'c'}
        client.stop_caching()
        assert client.get_row(table, '1') == {'x':'a', 'y':'b', 'z':'c'}
    finally:
        drop()

def test_get_row_colspec():
    create()
    try:
        client.add_row(table, 1, {'x':'a', 'y':'b', 'z':'c'})
        assert client.get_row(table, 1, ('x',)) == {'x':'a'}
    finally:
        drop()

def test_get_rows():
    create()
    try:
        client.add_row(table, 1, {'x':'a', 'y':'b', 'z':'c'})
        client.add_row(table, 2, {'x':'a', 'y':'b', 'z':'c'})
        client.add_row(table, 3, {'x':'a', 'y':'b', 'z':'c'})
        client.start_caching()
        print tuple((k, tuple(v.iteritems())) for k,v in client.get_rows(table, (1,2,3), ('x',)))
        nose.tools.assert_equals(
          tuple(
            (k, tuple(v.iteritems()))
            for k,v in client.get_rows(table, (1,2,3), ('x',))
          ),
          (
            (1, (('x', 'a'),)),
            (2, (('x', 'a'),)),
            (3, (('x', 'a'),))
          )
        )
        nose.tools.assert_equals(
          tuple(
            (k, tuple(v.iteritems()))
            for k,v in client.get_rows(table, (1,2,3), ('x',))
          ),
          (
            (1, (('x', 'a'),)),
            (2, (('x', 'a'),)),
            (3, (('x', 'a'),))
          )
        )
        client.stop_caching()
        assert client.get_row(table, 1, ('x',)) == {'x':'a'}
    finally:
        drop()
def test_delete_row():
    create()
    try:
        client.add_row(table, '1', {'x':'a', 'y':'b', 'z':'c'})
        assert client.get_row(table, '1') == {'x':'a', 'y':'b', 'z':'c'}
        client.delete_row(table, '1')
        assert len([ row for row in client.scan(table, ('x',), '0') ]) == 0
    finally:
        drop()

def test_scan_1_row():
    create()
    try:
        client.add_row(table, '1', {'x':'a', 'y':'b', 'z':'c'})
        row = client.scan(table, ('x',), '1').next()
        assert row == ('1', {'x':'a'})
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
        client.start_caching()
        ids = set(id for id, cols in client.scan(table, ('x',), start_prefix='1'))
        assert ids == set(str(id) for id in xrange(10, 20)) | set(['1'])
        ids = set(id for id, cols in client.scan(table, ('x',), start_prefix='1'))
        assert ids == set(str(id) for id in xrange(10, 20)) | set(['1'])
        ids = set(id for id, cols in client.scan(table, ('x',), start_prefix='1'))
        assert ids == set(str(id) for id in xrange(10, 20)) | set(['1'])
        client.stop_caching()
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

def test_scan_stop_tuple():
    print sys.path
    create()
    try:
        for x in xrange(10):
            for y in xrange(10):
                client.add_row(table, (x,y), {'x':'a', 'y':'b', 'z':'c'})
        ids = set(id 
            for id, cols in client.scan(table, ('x',), (0,0), stop_row=(1,2)))
        assert ids == set((x,y) 
            for x in xrange(10) for y in xrange(10) if (x,y) < (1,2))
    finally:
        drop()

def test_scan_prefix_tuple():
    create()
    try:
        for x in ['lisa', 'sid', 'jonny', 'marlow', 'mandy']:
            for y in xrange(10):
                client.add_row(table, (x,y), {'x':'a', 'y':'b', 'z':'c'})
        ids = set(id 
            for id, cols in client.scan(table, ('x',),
              start_prefix=api.tuple_prefix(('lisa',0), 1)))
        assert ids == set(('lisa',y) for y in xrange(10))
    finally:
        drop()

