"""
Import as:

import instrument_master.common.metadata.symbols as icmsym
"""
import abc
import dataclasses
from typing import List, Optional

import instrument_master.common.data.types as icdtyp


@dataclasses.dataclass
class Symbol:
    ticker: str
    exchange: str
    asset_class: icdtyp.AssetClass
    contract_type: Optional[icdtyp.ContractType]

    def validate(
        self,
        ticker: Optional[str],
        exchange: Optional[str],
        asset_class: Optional[icdtyp.AssetClass],
        contract_type: Optional[icdtyp.ContractType],
    ) -> bool:
        """
        Return `true` if symbol matches requirements.
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
        return matched


class SymbolList(abc.ABC):
    """
    Store available symbols.
    """

    @property
    @abc.abstractmethod
    def symbol_list(self) -> List[Symbol]:
        """
        Return available symbol list.
        """

    def get(
        self,
        ticker: Optional[str],
        exchange: Optional[str],
        asset_class: Optional[icdtyp.AssetClass],
        contract_type: Optional[icdtyp.ContractType],
    ) -> List[Symbol]:
        """
        Return available symbols based on parameters.

        E.g. `get(exchange="GLOBEX")` will return all symbols on GLOBEX.
        """
        matched_symbols = [
            symbol
            for symbol in self.symbol_list
            if symbol.validate(
                ticker=ticker,
                exchange=exchange,
                asset_class=asset_class,
                contract_type=contract_type,
            )
        ]
        return matched_symbols
