#!/usr/bin/env python

"""Read EODData symbol list for exchanges from a directory."""
import csv
import io
import logging
import os
from typing import List

import helpers.io_ as io_
import vendors2.eoddata.metadata.types as mtypes

_LOG = logging.getLogger(__name__)

# #############################################################################


class MetadataLoader:
    @staticmethod
    def read_symbols_from_file(file_: str) -> List[mtypes.Symbol]:
        _LOG.debug("Processing file: '%s' ...", file_)
        exchange_code, _ = os.path.basename(file_).split(".")

        reader = csv.DictReader(io.StringIO(io_.from_file(file_)))
        symbols = [
            mtypes.Symbol.from_csv_row(row=row, exchange_code=exchange_code)
            for row in reader
        ]
        _LOG.debug("Found %s symbols in '%s'", len(symbols), file_)
        return symbols
