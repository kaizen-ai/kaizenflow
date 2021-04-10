"""
Import as:

import instrument_master.ib.metadata.ib_symbols as iimibs
"""
import logging
import os
import functools
import string
from typing import List, Optional

import pandas as pd

import helpers.dbg as dbg
import helpers.s3 as hs3
import helpers.printing as printing
import instrument_master.common.data.types as icdtyp
import instrument_master.common.metadata.symbols as icmsym
import instrument_master.ib.data.config as iidcon

_LOG = logging.getLogger(__name__)

class IbSymbolUniverse(icmsym.SymbolUniverse):
    """
    Store symbols available in IB and already downloaded.
    """

    def __init__(self, symbols_file: str) -> None:
        self._symbols_file = symbols_file

    def get_all_symbols(self) -> List[icmsym.Symbol]:
        """
        Return the symbol list from the file used in the constructor.
        """
        if self._symbols_list is None:
            # Load the symbol list.
            dbg.dassert_is_not(self._symbols_file)
            _LOG.debug("symbol_file=%s", self._symbol_file)
            self._symbols_list = self._parse_symbols_file(self._symbol_file)
        return self._symbols_list

    @staticmethod
    @functools.lru_cache(maxsize=16)
    def _get_latest_symbols_file() -> str:
        """
        Get the latest available file with symbols on S3.
        """
        _S3_SYMBOL_FILE_PREFIX = os.path.join(iidcon.S3_METADATA_PREFIX, "symbols-")
        files = hs3.ls(_S3_SYMBOL_FILE_PREFIX)
        _LOG.debug("files='%s'", files)
        # TODO(gp): Make it more robust with globbing.
        latest_file: str = max(files)
        _LOG.debug("latest_file='%s'", latest_file)
        # Add the prefix.
        latest_file = os.path.join(iidcon.S3_METADATA_PREFIX, latest_file)
        dbg.dassert(hs3.exists(latest_file))
        return latest_file

    @staticmethod
    @functools.lru_cache(maxsize=16)
    def _parse_symbols_file(symbols_file: str) -> List[icmsym.Symbol]:
        """
        Read the passed file and return the list of symbols.

        """
        _LOG.debug("Reading symbols from %s", symbols_file)
        # Prevent to transform values from "NA" to `np.nan`.
        df: pd.DataFrame = pd.read_csv(
            symbols_file,
            sep="\t",
            keep_default_na=False,
            na_values=["_"],
        )
        _LOG.debug("head=%s", df.head())
        # The df looks like:
        # > csvlook instrument_master/ib/metadata/test/TestIbSymbolNamespace.test_parse_symbols_file1/input/test_symbols.csv
        # | market                                 | product   | s_title                        | ib_symbol    | symbol   | currency | url                                                                                                                   |
        # | ---------------------------------------| ----------| ------------------------------ | -------------| -------- | -------- | --------------------------------------------------------------------------------------------------------------------- |
        # | CBOE C2 (CBOE2)                        | Options   | CALLON PETROLEUM CO            | CPE          | CPE      | USD      | https://ndcdyn.interactivebrokers.com/en/index.php?f=2222&exch=cboe2&showcategories=OPTGRP&p=&cc=&limit=100&page=8    |
        # | Chicago Board Options Exchange (CBOE)  | Options   | RESMED INC                     | RMD          | RMD      | USD      | https://ndcdyn.interactivebrokers.com/en/index.php?f=2222&exch=cboe&showcategories=OPTGRP&p=&cc=&limit=100&page=32    |
        # | ICE Futures U.S. (NYBOT)               | Futures   | Cotton No. 2                   | CT           | CT       | USD      | https://ndcdyn.interactivebrokers.com/en/index.php?f=2222&exch=nybot&showcategories=FUTGRP                            |
        # | Korea Stock Exchange (KSE)             | Futures   | SAMSUNG ELECTRO-MECHANICS CO   | 009150       | 123      | KRW      | https://ndcdyn.interactivebrokers.com/en/index.php?f=2222&exch=kse&showcategories=FUTGRP
        # Find unique parsed symbols.
        # TODO(gp): Split.
        symbols = list(
            df.apply(
                lambda row: IbSymbolUniverse._convert_df_to_row_to_symbol(
                    ib_ticker=row["ib_symbol"],
                    ib_exchange=row["market"],
                    ib_asset_class=row["product"],
                    ib_currency=row["currency"],
                ),
                axis=1,
            )
            .dropna()
            .unique()
        )
        symbols.sort()
        _LOG.debug("Parsed %s", printing.perc(len(symbols), df.shape[0]))
        return symbols

    # TODO(gp): Add support also for the exchanges.
    #_S3_EXCHANGE_FILE_PREFIX = os.path.join(
    #    iidcon.S3_METADATA_PREFIX, "exchanges-"
    #)

    # TODO(gp): -> _convert_df_row_to_symbol
    @staticmethod
    def _convert_df_to_row_to_symbol(
        ib_ticker: str,
        ib_exchange: str,
        ib_asset_class: str,
        ib_currency: str,
    ) -> Optional[icmsym.Symbol]:
        """
        Build a Symbol from one row of the IB symbol file.
        """
        # Extract ticker.
        ticker = ib_ticker
        # Extract exchange.
        exchange = IbSymbolUniverse._extract_exchange_code_from_full_name(ib_exchange)
        # Extract asset class.
        # TODO(plyq): Not covered: `ETF`, `Forex`, `SP500`, expiring `Futures`.
        _IB_TO_P1_ASSETS = {
            "Futures": icdtyp.AssetClass.Futures,
            "Indices": None,
            "Stocks": icdtyp.AssetClass.Stocks,
            "Options": None,
            "Warrants": None,
            "Structured Products": None,
            "Bonds": None,
        }
        asset_class = _IB_TO_P1_ASSETS[ib_asset_class]
        # Extract contract type.
        # TODO(plyq): Support expiring contracts.
        contract_type = (
            icdtyp.ContractType.Continuous
            if asset_class == icdtyp.AssetClass.Futures
            else None
        )
        # Extract currency.
        currency = ib_currency
        # Construct the Symbol object, if possible.
        _LOG_vals(logging.DEBUG, "ticker exchange asset_class contract_type currency".split())
        #_LOG.debug("%s, %s, %s, %s, %s", to_str("ticker"), to_str("exchange"), to_str("asset_class"), to_str("contract_type"), to_str("currency"))
        if ib_ticker and exchange and asset_class and currency:
            symbol = icmsym.Symbol(
                ticker=ticker,
                exchange=exchange,
                asset_class=asset_class,
                contract_type=contract_type,
                currency=currency,
            )
        else:
            # Not supported symbol.
            symbol = None
        return symbol

    @staticmethod
    def _extract_exchange_code_from_full_name(exchange: str) -> Optional[str]:
        """
        The exchange code is inside the brackets or it is one word uppercase
        exchange name.
        """
        # Keep only what in brackets or string itself if there are no brackets.
        exchange: str = exchange.split("(")[-1].split(")")[0].strip()
        # Check if it is one uppercase word.
        if not set(exchange).issubset(string.ascii_uppercase):
            return None
        return exchange
