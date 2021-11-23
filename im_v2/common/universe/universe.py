"""
Import as:

import im_v2.common.universe.universe as imvcounun
"""

import collections
import os
from typing import Dict, List, Optional

import helpers.dbg as hdbg
import helpers.git as hgit
import helpers.io_ as hio

_LATEST_UNIVERSE_VERSION = "v03"


def get_trade_universe(
    version: str = _LATEST_UNIVERSE_VERSION,
) -> Dict[str, Dict[str, List[str]]]:
    """
    Load trade universe for which we have historical data on S3.

    :param version: release version
    :return: trade universe
    """
    file_name = "".join(["universe_", version, ".json"])
    file_path = os.path.join(
        hgit.get_amp_abs_path(), "im_v2/common/universe", file_name
    )
    hdbg.dassert_exists(file_path)
    universe = hio.from_json(file_path)
    return universe  # type: ignore[no-any-return]


# #############################################################################


ExchangeCurrencyTuple = collections.namedtuple(
    "ExchangeCurrencyTuple", "exchange_id currency_pair"
)


def get_vendor_universe_as_tuples(
    version: str = _LATEST_UNIVERSE_VERSION,
    vendor: str = "CCXT",
) -> List[ExchangeCurrencyTuple]:
    """
    Load vendor universe in a list of named tuples format.

    :param version: release version
    :param vendor: vendor to load data for
    :return: vendor universe as a list of tuples
    """
    # Get vendor universe.
    vendor_universe = get_trade_universe(version)[vendor]
    # Convert vendor universe dict to a sorted list of named tuples.
    res_list = [
        ExchangeCurrencyTuple(exchange_id, currency_pair)
        for exchange_id, currency_pairs in vendor_universe.items()
        for currency_pair in currency_pairs
    ]
    res_list = sorted(res_list)
    return res_list


def filter_vendor_universe_as_tuples(
    vendor_universe: List[ExchangeCurrencyTuple],
    exchange_ids: Optional[List[str]] = None,
    currency_pairs: Optional[List[str]] = None,
) -> List[ExchangeCurrencyTuple]:
    """
    Filter vendor universe by provided exchange ids and currencies.

    :param vendor_universe: vendor universe to filter
    :param exchange_ids: list of exchange ids to filter by
    :param currency_pairs: list of currency pairs to filter by
    :return: filtered vendor universe
    """
    if exchange_ids:
        hdbg.dassert_isinstance(exchange_ids, List)
        vendor_universe = [
            e for e in vendor_universe if e.exchange_id in exchange_ids
        ]
    if currency_pairs:
        hdbg.dassert_isinstance(currency_pairs, List)
        vendor_universe = [
            e for e in vendor_universe if e.currency_pair in currency_pairs
        ]
    # Verify that output is not empty.
    hdbg.dassert(
        vendor_universe,
        "No specified exchange ids and currency pairs in the provided universe.",
    )
    return vendor_universe
