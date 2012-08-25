#!/usr/bin/env python
# -*- coding: utf-8 -*-
#Original Author: Adam Ever-Hadani <adamhadani@gmail.com>
#Current Author: Tim Henderson
#Email: tim.tadh@gmail.com
#For licensing see the LICENSE file in the top level directory.

"""
hbasta._api

API Wrapper for HBase Thrift Client
"""

import sys
import logging
from itertools import imap
 
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
 
from hbase import Hbase
from hbase.ttypes import *

def _row_to_dict(row):
    """Convert an HBase Row as returned by the Thrift API
    to a native python dictionary, mapping column names to values"""
    return dict(imap(lambda i: (i[0], i[1].value), \
                     row.columns.iteritems()))

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
        mutations = [Mutation(False, col, val) for col, val in cols.iteritems()]
        self.thrift_client.mutateRow(table, row, mutations, {})

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
            rows = self.thrift_client.getRow(table, row)
        else:
            rows = self.thrift_client.getRowWithColumns(table, row, colspec)

        if rows:
            return _row_to_dict(rows[0])
        else:
            return None

    def delete_row(self, table, row):
        """Completely delete all data associated with row"""
        self.thrift_client.deleteAllRow(table, row)

    def atomic_increment(self, table, row, column, val=1):
        """Atomic increment of value for given column by the
        value specified"""
        return self.thrift_client.atomicIncrement(table, row, column, val)

    def scan(self, table, colspec, start_row=None, start_prefix=None, stop_row=None):

        def scanner_open(table, start_row, colspec):
            """Open a scanner for table at given start_row,
            fetching columns as specified in colspec"""
            return self.thrift_client.scannerOpen(table, start_row, colspec, {})

        def scanner_open_with_stop(table, start_row, stop_row, colspec):
            """Open a scanner for table at given start_row, scanning up to
            specified stop_row"""
            return self.thrift_client.scannerOpenWithStop(table, start_row,
                stop_row, colspec, {})

        def scanner_open_with_prefix(table, start_prefix, colspec):
            """Open a scanner for a given prefix on row name"""
            return self.thrift_client.scannerOpenWithPrefix(table, start_prefix,
                colspec, {})

        def scanner_close(scanner_id):
            """Close a scanner"""
            self.thrift_client.scannerClose(scanner_id)

        def scanner_get(scanner_id):
            """Return current row scanner is pointing to."""

            rows = self.thrift_client.scannerGet(scanner_id)
            if rows:
                return (rows[0].row, _row_to_dict(rows[0]))
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

