#!/usr/bin/env python
# -*- coding: utf-8 -*-
#Original Author: Adam Ever-Hadani <adamhadani@gmail.com>
#Current Author: Tim Henderson
#Email: tim.tadh@gmail.com
#For licensing see the LICENSE file in the top level directory.

"""
hbasta.api

API Wrapper for HBase Thrift Client
"""

import sys
import logging
from itertools import imap, izip
import struct
import types
 
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
 
from hbase import Hbase
from hbase.ttypes import *

int_format = 'q'
int_struct = struct.Struct('<q')
str_format = 's'

def _row_to_dict(row):
    """Convert an HBase Row as returned by the Thrift API
    to a native python dictionary, mapping column names to values"""
    return dict((name.replace('fam:', ''), _bytes_to_value(cell.value)) 
            for name, cell in row.columns.iteritems())

def _get_format(tag):
    if tag == 'int':
        return int_format
    raise ValueError, str(type(value))

def _get_struct(tag):
    if tag == 'int':
        return int_struct
    return struct.Struct('<'+_get_format(tag))

def _unpack(tag, bytes):
    if tag == 'unicode':
        return bytes.encode('utf8')
    elif tag == 'str':
        return bytes
    strct = _get_struct(tag)
    return strct.unpack(bytes)[0]

def _value_encode(value, recurse=False):
    if isinstance(value, unicode):
        bytes = value.encode('utf8')
        return ( 'unicode', bytes)
    elif isinstance(value, str):
        return ( 'str', value)
    elif isinstance(value, int):
        return 'int', _get_struct('int').pack(value)
    elif not recurse and isinstance(value, tuple):
        return _encode_tuple(value)
    elif isinstance(value, types.FunctionType):
        return value()
    raise ValueError, str(type(value))
    
def _encode_tuple(value):
    tags = list()
    cells = list()
    for cell in value:
        tag, val = _value_encode(cell)
        cells.append(val.encode('hex'))
        tags.append(tag)
    tag = 'tuple:' + ','.join(tags)
    ret = (tag, '\t'.join(cells))
    return ret

def tuple_prefix(tup, index):
    def encoding():
        tags = list()
        cells = list()
        for cell in tup:
            tag, val = _value_encode(cell)
            cells.append(val.encode('hex'))
            tags.append(tag)
        tag = 'tuple:' + ','.join(tags)
        ret = (tag, '\t'.join(cells[:index]))
        return ret
    return encoding

def _decode_tuple(bytes):
    tags, bytes = bytes.split(':', 1)
    tags = (tag for tag in tags.split(','))
    cells = (cell.decode('hex') for cell in bytes.split('\t'))
    ret = tuple(_unpack(tag, cell) for tag, cell in izip(tags, cells))
    return ret

def _value_to_bytes(value):
    tag, bytes = _value_encode(value)
    ret = ':'.join((tag, bytes))
    return ret

def _bytes_to_value(tagged_bytes):
    tag, bytes = tagged_bytes.split(':', 1)
    if tag == 'tuple':
        return _decode_tuple(bytes)
    return _unpack(tag, bytes)

def str_increment(s):
    """Increment and truncate a byte string (for sorting purposes)

    This functions returns the shortest string that sorts after the given
    string when compared using regular string comparison semantics.

    This function increments the last byte that is smaller than ``0xFF``, and
    drops everything after it. If the string only contains ``0xFF`` bytes,
    `None` is returned.
    """
    for i in xrange(len(s) - 1, -1, -1):
        if s[i] != '\xff':
            return s[:i] + chr(ord(s[i]) + 1)
    return None

class WriteOnReadCaching(Exception): pass

class Client(object):
    """HBase API entry point"""

    LOG = logging.getLogger("HBasta")

    def __init__(self, host, port):
        """Initialize client.

        Params:
            hostnport - Tuple of (host, port) to connect to
        """
        self.host = host
        self.port = int(port)
        self._client = None
        self.caching = False
        self.cache = None

    def start_caching(self):
        if not self.caching:
            self.cache = dict()
            self.caching = True

    def stop_caching(self):
        self.caching = False
        self.cache = None

    @property
    def thrift_client(self):
        """Lazy load of the underlying client"""
        if not self._client:
            self.LOG.debug(
                "* Connecting to HBase at: %s %d" % (self.host, self.port))
            socket = TSocket.TSocket(self.host, self.port)
            transport = TTransport.TBufferedTransport(socket)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            self._client  = Hbase.Client(protocol)
            transport.open()
        return self._client

    def create_table(self, table, col_families):
        """Create a new HBase table

        Params
            table - Table name
            col_families - list of column family names
        """
        if self.caching: raise WriteOnReadCaching
        self.thrift_client.createTable(table, 
                [ColumnDescriptor(name='fam:'+c) for c in col_families])

    def enable_table(self, table):
        """Enable an HBase table"""
        if self.caching: raise WriteOnReadCaching
        self.thrift_client.enableTable(table)

    def disable_table(self, table):
        """Disable an HBase table"""
        if self.caching: raise WriteOnReadCaching
        self.thrift_client.disableTable(table)

    def drop_table(self, table):
        """Disable an HBase table"""
        if self.caching: raise WriteOnReadCaching
        self.thrift_client.deleteTable(table)

    def is_table_enabled(self, table):
        """Check if table is enabled"""
        return self.thrift_client.isTableEnabled(table)

    def get_table_names(self):
        """Get list of all available table names"""
        return self.thrift_client.getTableNames()

    def add_row(self, table, row, cols):
        """Add a new row to table.

        Params:
            table - Table name
            key - Row key
            cols - dictionary of fully qualified column name pointing to data 
            (e.g { 'family:colname': value } )
        """
        if self.caching: raise WriteOnReadCaching
        mutations = [
            Mutation(False, 'fam:'+col, _value_to_bytes(val))
            for col, val in cols.iteritems()
        ]
        self.thrift_client.mutateRow(table, _value_to_bytes(row), mutations, {})

    def get_row(self, table, key, colspec=None):
        """Get single row of data, possibly filtered
        using the colspec construct

        Params:
            table - Table name
            key - Row key
            colspec - Specifier of which columns to return, in the form of list 
                      of column names
        """
        key = _value_to_bytes(key)
        cache_key = ('get_row', str(table), key,
            tuple(colspec) if colspec is not None else None)
        if self.caching and cache_key in self.cache:
            return self.cache[cache_key]

        if not colspec:
            rows = self.thrift_client.getRow(table, key, {})
        else:
            rows = self.thrift_client.getRowWithColumns(table,
                key, tuple('fam:'+col for col in colspec), {})

        if rows:
            retval = _row_to_dict(rows[0])
            if self.caching:
                self.cache[cache_key] = retval
            return retval
        else:
            return None

    def get_rows(self, table, keys, colspec):
        """Get single row of data, possibly filtered
        using the colspec construct

        Params:
            table - Table name
            key - Row key
            colspec - Specifier of which columns to return, in the form of list 
                      of column names
        """
        keys = tuple(_value_to_bytes(key) for key in keys)
        cache_key = ('get_rows', str(table), keys, tuple(colspec))
        if self.caching and cache_key in self.cache:
            for row in self.cache[cache_key]:
                yield row
            return

        rows = self.thrift_client.getRowsWithColumns(
          table,
          keys,
          tuple('fam:'+col for col in colspec),
          {}
        )

        if self.caching: all_rows = list()
        for row in rows:
            decoded_row = (
              _bytes_to_value(row.row),
              _row_to_dict(row)
            )
            if self.caching: all_rows.append(decoded_row)
            yield decoded_row
        if self.caching: self.cache[cache_key] = all_rows

    def delete_row(self, table, row):
        """Completely delete all data associated with row"""
        if self.caching: raise WriteOnReadCaching
        self.thrift_client.deleteAllRow(table, _value_to_bytes(row), {})

    def atomic_increment(self, table, row, column, val=1):
        """Atomic increment of value for given column by the
        value specified"""
        if self.caching: raise WriteOnReadCaching
        return self.thrift_client.atomicIncrement(table, _value_to_bytes(row), column, val)

    def scan(self, table, colspec, start_row=None, start_prefix=None,
             stop_row=None, batch_size=500):

        colspec = tuple('fam:'+col for col in colspec)

        def scanner_close(scanner_id):
            """Close a scanner"""
            self.thrift_client.scannerClose(scanner_id)

        def scanner_get(scanner_id):
            """Return current row scanner is pointing to."""

            if batch_size == 1:
                rows = self.thrift_client.scannerGet(scanner_id)
            else:
                rows = self.thrift_client.scannerGetList(scanner_id, batch_size)
            return rows

        assert start_row is not None or start_prefix is not None
        assert start_prefix is None or stop_row is None

        if start_row is None:
            start_row = ''

        start_row = _value_to_bytes(start_row)
        if stop_row:
            stop_row = _value_to_bytes(stop_row)

        if start_prefix is not None:
            start_row = _value_to_bytes(start_prefix)
            stop_row = str_increment(start_row)

        cache_key = ('scan', str(table),
            tuple(colspec) if colspec is not None else None,
            str(start_row), str(start_prefix), str(stop_row))
        if self.caching and cache_key in self.cache:
            for row in self.cache[cache_key]:
                yield row
            return

        scan = TScan(startRow=start_row,
                     stopRow=stop_row,
                     columns=colspec,
                     caching=batch_size)
        scanner_id = self.thrift_client.scannerOpenWithScan(table, scan, {})

        try:
            if self.caching: all_rows = list()
            while True:
                rows = scanner_get(scanner_id)
                for row in rows:
                    decoded_row = (
                      _bytes_to_value(row.row),
                      _row_to_dict(row)
                    )
                    if self.caching: all_rows.append(decoded_row)
                    yield decoded_row
                if len(rows) < batch_size: break
            if self.caching: self.cache[cache_key] = all_rows
        finally:
            scanner_close(scanner_id)

