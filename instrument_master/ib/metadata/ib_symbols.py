"""
Import as:

import instrument_master.ib.metadata.ib_symbols as iimibs
"""
import logging
import os
import string
from typing import List, Optional

import pandas as pd

import helpers.dbg as dbg
import helpers.s3 as hs3
import instrument_master.common.data.types as icdtyp
import instrument_master.common.metadata.symbols as icmsym
import instrument_master.ib.data.config as iidcon

_LOG = logging.getLogger(__name__)


class IbSymbolUniverse(icmsym.SymbolUniverse):
    """
    Store symbols available to download with IB Gateway API.
    """

    _S3_SYMBOL_FILE_PREFIX = os.path.join(iidcon.S3_METADATA_PREFIX, "symbols-")
    _S3_EXCHANGE_FILE_PREFIX = os.path.join(
        iidcon.S3_METADATA_PREFIX, "exchanges-"
    )
    _S3_FILE_SEPARATOR = "\t"
    _S3_FILE_SYMBOL_COLUMN = "ib_symbol"
    _S3_FILE_EXCHANGE_COLUMN = "market"
    _S3_FILE_ASSET_CLASS_COLUMN = "product"
    _S3_FILE_CURRENCY_COLUMN = "currency"
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

    def __init__(self, symbols_file: Optional[str] = None) -> None:
        symbol_file = (
            self._get_latest_symbols_file()
            if symbols_file is None
            else symbols_file
        )
        self._symbols_list = self._parse_symbols_file(symbol_file)

    def get_all_symbols(self) -> List[icmsym.Symbol]:
        return self._symbols_list

    @classmethod
    def _get_latest_symbols_file(cls) -> str:
        """
        Get the latest available file with symbols on S3.
        """
        latest_file: str = max(hs3.ls(cls._S3_SYMBOL_FILE_PREFIX))
        # Add a prefix.
        latest_file = os.path.join(iidcon.S3_METADATA_PREFIX, latest_file)
        dbg.dassert(
            hs3.exists(latest_file), "File %s doesn't exist" % latest_file
        )
        return latest_file

    @classmethod
    def _parse_symbols_file(cls, symbols_file: str) -> List[icmsym.Symbol]:
        """
        Read the file, return list of symbols.
        """
        _LOG.debug("Reading symbols from %s", symbols_file)
        # Prevent to transform values from "NA" to `np.nan`.
        df: pd.DataFrame = pd.read_csv(
            symbols_file,
            sep=cls._S3_FILE_SEPARATOR,
            keep_default_na=False,
            na_values=["_"],
        )
        # Find unique parsed symbols.
        symbols = list(
            df.apply(
                lambda row: cls._convert_to_symbol(
                    ib_ticker=row[cls._S3_FILE_SYMBOL_COLUMN],
                    ib_exchange=row[cls._S3_FILE_EXCHANGE_COLUMN],
                    ib_asset_class=row[cls._S3_FILE_ASSET_CLASS_COLUMN],
                    ib_currency=row[cls._S3_FILE_CURRENCY_COLUMN],
                ),
                axis=1,
            )
            .dropna()
            .unique()
        )
        symbols.sort()
        success_percentage = 100.0 * len(symbols) / len(df)
        _LOG.info(
            "Successfully parsed %i/%i=%f%% symbols",
            len(symbols),
            len(df),
            success_percentage,
        )
        return symbols

    @classmethod
    def _convert_to_symbol(
        cls,
        ib_ticker: str,
        ib_exchange: str,
        ib_asset_class: str,
        ib_currency: str,
    ) -> Optional[icmsym.Symbol]:
        # Extract ticker.
        ticker = ib_ticker
        # Extract exchange.
        exchange = cls._extract_exchange_code_from_full_name(ib_exchange)
        # Extract asset class.
        asset_class = cls._IB_TO_P1_ASSETS[ib_asset_class]
        # Extract contract type.
        # TODO(plyq): Support expiring contracts.
        contract_type = (
            icdtyp.ContractType.Continuous
            if asset_class == icdtyp.AssetClass.Futures
            else None
        )
        # Extract currency.
        currency = ib_currency
        # Construct symbol if possible.
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
            _LOG.debug(
                "Symbol %s %s %s %s is not supported",
                ib_ticker,
                ib_exchange,
                ib_asset_class,
                ib_currency,
            )
            symbol = None
        return symbol

    @staticmethod
    def _extract_exchange_code_from_full_name(exchange: str) -> Optional[str]:
        """
        Exchange code is inside the brackets or it is one word uppercase
        exchange name.
        """
        # Keep only what in brackets or string itself if there are no brackets.
        exchange: str = exchange.split("(")[-1].split(")")[0].strip()
        # Check if it is one uppercase word.
        if not set(exchange).issubset(string.ascii_uppercase):
            return None
        return exchange


class IbDownloadedSymbol(icmsym.SymbolUniverse):
    """
    Store symbols available on S3.
    """

    def __init__(self, symbols_file: Optional[str]) -> None:
        self._symbols_list = self._find_all_s3_symbols()

    def get_all_symbols(self) -> List[icmsym.Symbol]:
        return self._symbols_list

    # TODO(plyq): Implement.
    def _find_all_s3_symbols(self) -> List[icmsym.Symbol]:
        symbol_list: List[icmsym.Symbol] = []
        return symbol_list
