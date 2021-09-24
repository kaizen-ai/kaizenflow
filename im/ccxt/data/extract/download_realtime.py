#!/usr/bin/env python
"""
Script to download data from CCXT in real-time.
"""
import argparse
import logging
import os
import time

import pandas as pd

import helpers.dbg as dbg
import helpers.io_ as hio
import helpers.parser as hparse
import im.ccxt.data.extract.exchange_class as deecla

_LOG = logging.getLogger(__name__)


if __name__ == "__main__":
    _main(_parse())