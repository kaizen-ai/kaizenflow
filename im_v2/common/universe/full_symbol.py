"""
Import as:

import im_v2.common.universe.full_symbol as imvcufusy
"""

import logging
import re
from typing import List, Tuple, Union

import helpers.hdbg as hdbg

_LOG = logging.getLogger(__name__)

# Store information about an exchange and a symbol (e.g., `binance::BTC_USDT`).
# Note that information about the vendor is carried in the `ImClient` itself,
# i.e. using `CcxtImClient` serves data from CCXT.
# Full symbols are transformed in `asset_ids` encoded by ints, by `ImClient` and
# used by `MarketData`.
FullSymbol = str


# TODO(gp): -> dassert_valid_full_symbol
def dassert_is_full_symbol_valid(full_symbol: FullSymbol) -> None:
    """
    Check that a full symbol has valid format, i.e. `exchange::symbol`.

    Note: digits and special symbols (except underscore) are not allowed.
    """
    hdbg.dassert_isinstance(full_symbol, str)
    hdbg.dassert_ne(full_symbol, "")
    # Only letters and underscores are allowed.
    # TODO(gp): I think we might need non-leading numbers.
    letter_underscore_pattern = "[a-zA-Z_]"
    # Exchanges and symbols must be separated by `::`.
    regex_pattern = rf"{letter_underscore_pattern}*::{letter_underscore_pattern}*"
    # A valid full symbol must match the pattern.
    full_match = re.fullmatch(regex_pattern, full_symbol, re.IGNORECASE)
    hdbg.dassert(
        full_match,
        "Incorrect full_symbol '%s', it must be `exchange::symbol`",
        full_symbol,
    )


# TODO(Dan): Research if this can be done in a vectorized approach to a `pd.Series`.
def parse_full_symbol(full_symbol: FullSymbol) -> Tuple[str, str]:
    """
    Split a full_symbol into a tuple of exchange and symbol.

    :return: exchange, symbol
    """
    dassert_is_full_symbol_valid(full_symbol)
    exchange, symbol = full_symbol.split("::")
    return exchange, symbol


def build_full_symbol(
    exchange: Union[str, pd.Series], symbol: Union[str, pd.Series]
) -> Union[FullSymbol, pd.Series]:
    """
    Combine exchange and symbol in `FullSymbol`.
    """
    if isinstance(exchange, pd.Series) and isinstance(symbol, pd.Series):
        # TODO(Dan): Think of a more appropriate approach.
        full_symbol = exchange + "::" + symbol
        # TODO(Dan): Try to find a vectorized approach for asserting column values.
        full_symbol = full_symbol.apply(lambda x: dassert_is_full_symbol_valid(x))
    elif isinstance(exchange, str) and isinstance(symbol, str):
        hdbg.dassert_ne(exchange, "")
        hdbg.dassert_ne(symbol, "")
        #
        full_symbol = f"{exchange}::{symbol}"
        dassert_is_full_symbol_valid(full_symbol)
    else:
        raise TypeError("Both inputs should be either strings or `pd.Series`")
    return full_symbol


def dassert_valid_full_symbols(full_symbols: List[FullSymbol]) -> None:
    """
    Verify that full symbols are passed in a list that has no duplicates.
    """
    hdbg.dassert_container_type(full_symbols, list, FullSymbol)
    hdbg.dassert_no_duplicates(full_symbols)
