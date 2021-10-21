#!/usr/bin/env python
"""
Insert a table into the database.

Use example:

> create_table.py \
    --table_name 'ccxt_ohlcv'

Import as:

import im.ccxt.db.create_table as imccdbcrtab
"""

import argparse
import logging

import helpers.dbg as hdbg
import helpers.parser as hparser
import helpers.sql as hsql
import os

_LOG = logging.getLogger(__name__)

