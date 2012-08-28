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
    return dict((name, _bytes_to_value(cell.value)) 
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
        self.thrift_client.createTable(table, 
                [ColumnDescriptor(name=c+':') for c in col_families])

    def enable_table(self, table):
        """Enable an HBase table"""
        self.thrift_client.enableTable(table)

    def disable_table(self, table):
        """Disable an HBase table"""
        self.thrift_client.disableTable(table)

    def drop_table(self, table):
        """Disable an HBase table"""
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
        mutations = [
            Mutation(False, col, _value_to_bytes(val))
            for col, val in cols.iteritems()
        ]
        self.thrift_client.mutateRow(table, _value_to_bytes(row), mutations, {})

    def get_row(self, table, row, colspec=None):
        """Get single row of data, possibly filtered
        using the colspec construct

        Params:
            table - Table name
            key - Row key
            colspec - Specifier of which columns to return, in the form of list 
                      of column names
        """
        if not colspec:
            rows = self.thrift_client.getRow(table, _value_to_bytes(row), {})
        else:
            rows = self.thrift_client.getRowWithColumns(table,
                _value_to_bytes(row), colspec, {})

        if rows:
            return _row_to_dict(rows[0])
        else:
            return None

    def delete_row(self, table, row):
        """Completely delete all data associated with row"""
        self.thrift_client.deleteAllRow(table, _value_to_bytes(row), {})

    def atomic_increment(self, table, row, column, val=1):
        """Atomic increment of value for given column by the
        value specified"""
        return self.thrift_client.atomicIncrement(table, _value_to_bytes(row), column, val)

    def scan(self, table, colspec, start_row=None, start_prefix=None, stop_row=None):

        def scanner_open(table, start_row, colspec):
            """Open a scanner for table at given start_row,
            fetching columns as specified in colspec"""
            return self.thrift_client.scannerOpen(table,
                _value_to_bytes(start_row), colspec, {})

        def scanner_open_with_stop(table, start_row, stop_row, colspec):
            """Open a scanner for table at given start_row, scanning up to
            specified stop_row"""
            return self.thrift_client.scannerOpenWithStop(table,
                _value_to_bytes(start_row), _value_to_bytes(stop_row), colspec, 
                {})

        def scanner_open_with_prefix(table, start_prefix, colspec):
            """Open a scanner for a given prefix on row name"""
            return self.thrift_client.scannerOpenWithPrefix(table,
                _value_to_bytes(start_prefix), colspec, {})

        def scanner_close(scanner_id):
            """Close a scanner"""
            self.thrift_client.scannerClose(scanner_id)

        def scanner_get(scanner_id):
            """Return current row scanner is pointing to."""

            rows = self.thrift_client.scannerGet(scanner_id)
            if rows:
                return (
                    _bytes_to_value(rows[0].row), 
                    _row_to_dict(rows[0])
                )
            else:
                return None

        # unused for now
        #
        #def scanner_get_list(self, scanner_id, num_rows):
        #    """Returns up to num_rows rows starting at current
        #    scanner location. Returns as a generator expression."""
        #    rows = self.thrift_client.scannerGetList(scanner_id, num_rows)
        #    return map(lambda x: (x.row, _row_to_dict(x)), rows)

        assert start_row is not None or start_prefix is not None
        assert start_prefix is None or stop_row is None

        if start_row is not None and stop_row is not None:
            scanner_id = scanner_open_with_stop(table, start_row, stop_row,
                                                colspec)
        elif start_row is not None: 
            scanner_id = scanner_open(table, start_row, colspec)
        elif start_prefix is not None:
            scanner_id = scanner_open_with_prefix(table, start_prefix, colspec)
        else:
            raise RuntimeError, "Unexpected method input"

        try:
            row = scanner_get(scanner_id)
            while row is not None:
                yield row
                row = scanner_get(scanner_id)
        finally:
            scanner_close(scanner_id)

