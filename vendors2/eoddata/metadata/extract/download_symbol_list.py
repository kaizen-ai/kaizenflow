#!/usr/bin/env python

"""Download symbol list for exchanges from EODData into a CSV.

API reference: http://ws.eoddata.com/data.asmx.

Ideally the destination directory would contain a timestamp,
because we want to track the symbols over time.

E.g:
# download NYSE data to '<current_dir>/08-04-20/NYSE.csv'
download_eoddata_symbol_list.py --dst_dir $(date +"%m-%d-%y") --exchange_codes NYSE

# download data for multiple exchange codes
download_eoddata_symbol_list.py --dst_dir $(date +"%m-%d-%y") --exchange_codes NYSE ASX AMEX

# download data for all exchange codes (omit --exchange_codes)
download_eoddata_symbol_list.py --dst_dir $(date +"%m-%d-%y")
"""
import argparse
import dataclasses
import functools
import logging
import os
from typing import List

import pandas as pd
import zeep

import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.parser as prsr
import helpers.system_interaction as si
import vendors2.eoddata.metadata.types as mtypes

_LOG = logging.getLogger(__name__)

# #############################################################################


@functools.lru_cache()
def get_client() -> zeep.Client:
    return zeep.Client("http://ws.eoddata.com/data.asmx?WSDL")


def _get_token() -> str:
    """Login to EODData API using credentials in env vars and get a token."""
    username = si.get_env_var("P1_EODDATA_USERNAME")
    password = si.get_env_var("P1_EODDATA_PASSWORD")

    _LOG.info("Logging into EODData API ...")

    response = get_client().service.Login(Username=username, Password=password)

    if response["Token"] is None:
        dbg.dfatal("Login Failed: '%s'", response["Message"])

    return str(response["Token"])


def _get_symbols(exchange_code: str, token: str) -> List[mtypes.Symbol]:
    """Get a list of symbols for a certain exchange."""
    _LOG.info("Getting symbols list for exchange: '%s'", exchange_code)
    response = get_client().service.SymbolList(
        Token=token, Exchange=exchange_code
    )

    if response.SYMBOLS is None:
        _LOG.error("No symbols found for exchange: '%s'", exchange_code)
        return []

    symbols = [
        mtypes.Symbol.from_dict(d=obj)
        for obj in zeep.helpers.serialize_object(response.SYMBOLS["SYMBOL"])
    ]

    _LOG.info("Got %s symbols for exchange '%s'", len(symbols), exchange_code)
    return symbols


def _write_symbols_to_csv(
    exchange_code: str, symbols: List[mtypes.Symbol], dst_dir: str
) -> None:
    """Write symbols to `<exchange_code>.csv` in the dst_dir.

    Creates `dst_dir` if it doesn't exist.
    """
    io_.create_dir(dir_name=dst_dir, incremental=True)

    file_path = os.path.join(dst_dir, exchange_code + ".csv")
    symbols_df = pd.DataFrame([dataclasses.asdict(s) for s in symbols])
    symbols_df.to_csv(file_path, index=False)

    _LOG.info("Wrote Downloaded Symbols to '%s'", file_path)


def _get_exchanges(token: str) -> List[mtypes.Exchange]:
    """Get a list of exchange names from EODData."""
    _LOG.info("Getting exchanges from API ...")
    response = get_client().service.ExchangeList(Token=token)

    exchanges = [
        mtypes.Exchange.from_dict(d=obj)
        for obj in zeep.helpers.serialize_object(response.EXCHANGES["EXCHANGE"])
    ]
    _LOG.info("Got %s exchanges", len(exchanges))
    return exchanges


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--exchange_codes",
        nargs="+",
        help="Codes of the exchanges to download symbols for (defaults to all)",
    )
    parser.add_argument("--dst_dir", action="store", help="Destination dir")
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    dbg.dassert_is_not(
        args.dst_dir, None, msg="Must provide a destination directory"
    )

    token = _get_token()

    exchange_codes = args.exchange_codes or [
        e.Code for e in _get_exchanges(token=token)
    ]

    for exchange_code in exchange_codes:
        symbols = _get_symbols(exchange_code=exchange_code, token=token)
        if symbols:
            _write_symbols_to_csv(
                exchange_code=exchange_code, symbols=symbols, dst_dir=args.dst_dir
            )


if __name__ == "__main__":
    _main(_parse())
