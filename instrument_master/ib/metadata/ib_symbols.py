"""
Import as:

import instrument_master.ib.metadata.ib_symbols as iimibs
"""
from typing import List, Optional

import instrument_master.common.metadata.symbols as icmsym


class IbSymbolUniverse(icmsym.SymbolUniverse):
    """
    Store symbols available to download with IB Gateway API.
    """

    def __init__(self, symbols_file: Optional[str]) -> None:
        symbol_file = (
            self._get_latest_symbols_file()
            if symbols_file is None
            else symbols_file
        )
        self._symbols_list = self._parse_symbols_file(symbol_file)

    def get_all_symbols(self) -> List[icmsym.Symbol]:
        return self._symbols_list

    # TODO(plyq): Implement.
    def _get_latest_symbols_file(self) -> str:
        symbols_file = ""
        return symbols_file

    # TODO(plyq): Implement.
    def _parse_symbols_file(self, symbols_file: str) -> List[icmsym.Symbol]:
        symbol_list: List[icmsym.Symbol] = []
        return symbol_list


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
