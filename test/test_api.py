#!/usr/bin/env python
# -*- coding: utf-8 -*-
#Author: Tim Henderson
#Email: tim.tadh@gmail.com
#For licensing see the LICENSE file in the top level directory.

from hbasta.api import Client

def test_create_table():
    client = Client('localhost', 9090)
    client.create_table('__test__', ['x', 'y', 'z'])
    client.disable_table('__test__')
    client.drop_table('__test__')

