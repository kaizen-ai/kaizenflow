"""
Import as:

import instrument_master.common.metadata.symbols as icmsym
"""
import abc
import dataclasses
from typing import List, Optional, Tuple

import helpers.dbg as dbg
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
        return self.to_tuple() == other.to_tuple()

    def __hash__(self) -> int:
        return hash(self.to_tuple())

    def __str__(self) -> str:
        string = "(%s)" % ", ".join([str(part) for part in self.to_tuple()])
        return string

    def __lt__(self, other: object) -> bool:
        dbg.dassert_isinstance(other, Symbol)
        other: Symbol
        return self.to_tuple() < other.to_tuple()

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

    def to_tuple(
        self,
    ) -> Tuple[str, str, icdtyp.AssetClass, Optional[icdtyp.ContractType], str]:
        return (
            self.ticker,
            self.exchange,
            self.asset_class.value,
            "" if self.contract_type is None else self.contract_type.value,
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
    ) -> List[Symbol]:
        """
        Return all the available symbols based on different selection criteria.

        E.g. `get(exchange="GLOBEX")` will return all symbols on GLOBEX.
        """
        matched_symbols = [
            symbol
            for symbol in self.get_all_symbols()
            if symbol.is_selected(
                ticker=ticker,
                exchange=exchange,
                asset_class=asset_class,
                contract_type=contract_type,
                currency=currency,
            )
        ]
        return matched_symbols
