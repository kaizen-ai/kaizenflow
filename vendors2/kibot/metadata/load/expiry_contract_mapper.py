import functools
import re
from typing import Iterable, Tuple

import helpers.dbg as dbg


class ExpiryContractMapper:
    """Implement functions to handle expiry contracts, e.g., "ESH19"."""

    _MONTH_TO_EXPIRY = {
        "January": "F",
        "February": "G",
        "March": "H",
        "April": "J",
        "May": "K",
        "June": "M",
        "July": "N",
        "August": "Q",
        "September": "U",
        "October": "V",
        "November": "X",
        "December": "Z",
    }

    _EXPIRY_TO_MONTH = {v: k for k, v in _MONTH_TO_EXPIRY.items()}

    def month_to_expiry(self, month: str) -> str:
        dbg.dassert_in(month, self._MONTH_TO_EXPIRY)
        return self._MONTH_TO_EXPIRY[month]

    def expiry_to_month(self, expiry: str) -> str:
        dbg.dassert_in(expiry, self._EXPIRY_TO_MONTH)
        return self._EXPIRY_TO_MONTH[expiry]

    @staticmethod
    def parse_expiry_contract(v: str) -> Tuple[str, str, str]:
        """Parse a futures contract name into its components, e.g., in a
        futures contract name like "ESH10":

        - base symbol is ES
        - month is H
        - year is 10 (i.e., 2010)
        """
        m = re.match(r"^(\S+)(\S)(\d{2})$", v)
        dbg.dassert(m, "Invalid '%s'", v)
        base_symbol, month, year = m.groups()
        return base_symbol, month, year

    @staticmethod
    def compare_expiry_contract(v1: str, v2: str) -> int:
        """Compare function for two expiry contracts in terms of month and year
        ( e.g., "U10") according to python `cmp` convention.

        :param: return -1, 0, 1 in case of <, ==, > relationship between v1 and
            v2.
        """
        base_symbol1, month1, year1 = ExpiryContractMapper.parse_expiry_contract(
            v1
        )
        base_symbol2, month2, year2 = ExpiryContractMapper.parse_expiry_contract(
            v2
        )
        dbg.dassert_eq(base_symbol1, base_symbol2)
        # Compare.
        if year1 < year2 or (year1 == year2 and month1 < month2):
            res = -1
        elif year1 == year2 and month1 == month2:
            res = 0
        else:
            res = 1
        return res

    @staticmethod
    def sort_expiry_contract(contracts: Iterable[str]) -> Iterable[str]:
        # python3 removed `cmp` from sort so we need to convert it into a key.
        contracts = sorted(
            contracts,
            key=functools.cmp_to_key(
                ExpiryContractMapper.compare_expiry_contract
            ),
        )
        return contracts
