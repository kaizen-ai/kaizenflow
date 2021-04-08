"""
Import as:

import instrument_master.common.metadata.symbols as icmsym
"""
import abc
import dataclasses
from typing import List, Optional, Tuple

import helpers.dbg as dbg
import helpers.s3 as hs3
import instrument_master.common.data.load.file_path_generator as icdlfi
import instrument_master.common.data.types as icdtyp


@dataclasses.dataclass
class Symbol:
    ticker: str
    exchange: str
    asset_class: icdtyp.AssetClass
    contract_type: Optional[icdtyp.ContractType]
    currency: str

    def __eq__(self, other: object) -> bool:
        dbg.dassert_isinstance(other, Symbol)
        other: Symbol
        return self._to_string_tuple() == other._to_string_tuple()

    def __hash__(self) -> int:
        return hash(self._to_string_tuple())

    def __str__(self) -> str:
        string = "(%s)" % ", ".join(
            [str(part) for part in self._to_string_tuple()]
        )
        return string

    def __lt__(self, other: object) -> bool:
        dbg.dassert_isinstance(other, Symbol)
        other: Symbol
        return self._to_string_tuple() < other._to_string_tuple()

    def is_selected(
        self,
        ticker: Optional[str],
        exchange: Optional[str],
        asset_class: Optional[icdtyp.AssetClass],
        contract_type: Optional[icdtyp.ContractType],
        currency: Optional[str],
    ) -> bool:
        """
        Return `True` if symbol matches requirements.
        """
        matched = True
        if ticker is not None and self.ticker != ticker:
            matched = False
        if exchange is not None and self.exchange != exchange:
            matched = False
        if asset_class is not None and self.asset_class != asset_class:
            matched = False
        if contract_type is not None and self.contract_type != contract_type:
            matched = False
        if currency is not None and self.currency != currency:
            matched = False
        return matched

    def _to_string_tuple(
        self,
    ) -> Tuple[str, str, str, Optional[str], str]:
        return (
            self.ticker,
            self.exchange,
            self.asset_class.value,
            self.contract_type.value if self.contract_type is not None else None,
            self.currency,
        )


class SymbolUniverse(abc.ABC):
    """
    Store available symbols.
    """

    @abc.abstractmethod
    def get_all_symbols(self) -> List[Symbol]:
        """
        Return all the available symbols.
        """

    def get(
        self,
        ticker: Optional[str],
        exchange: Optional[str],
        asset_class: Optional[icdtyp.AssetClass],
        contract_type: Optional[icdtyp.ContractType],
        currency: Optional[str],
        is_downloaded: Optional[bool] = None,
        frequency: Optional[icdtyp.Frequency] = None,
        path_generator: Optional[icdlfi.FilePathGenerator] = None,
    ) -> List[Symbol]:
        """
        Return all the available symbols based on different selection criteria.

        `frequency` and `path_generator` are used only if `is_downloaded` is True.

        E.g. `get(exchange="GLOBEX", is_downloaded=True)` will return
        all available on S3 symbols for GLOBEX exchange.

        :param ticker: symbol ticker
        :param exchange: trading exchange code
        :param asset_class: symbol asset class
        :param contract_type: symbol contract type
        :param currency: symbol currency
        :param is_downloaded: is symbol available on S3
        :param frequency: downloaded frequency
        :param path_generator: generate path based on symbol
        :return: list of matched symbols
        :raises ValueError: if parameters are not valid
        """
        matched_symbols = []
        for symbol in self.get_all_symbols():
            if not symbol.is_selected(
                ticker=ticker,
                exchange=exchange,
                asset_class=asset_class,
                contract_type=contract_type,
                currency=currency,
            ):
                # Symbol doesn't satisfy parameters.
                continue
            if is_downloaded:
                if frequency is None or path_generator is None:
                    raise ValueError(
                        "`frequency` and `path_generator` params are not specified"
                    )
                # Check path exists.
                if not hs3.exists(
                    path_generator.generate_file_path(
                        symbol=symbol.ticker,
                        frequency=frequency,
                        asset_class=symbol.asset_class,
                        contract_type=symbol.contract_type,
                        ext=icdtyp.Extension.CSV,
                    )
                ):
                    continue
            # Symbol should be returned.
            matched_symbols.append(symbol)
        return matched_symbols
