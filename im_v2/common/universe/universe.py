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
import im_v2.common.data.client as imvcdcadlo

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


def get_vendor_universe(
    version: str = _LATEST_UNIVERSE_VERSION,
    vendor: str = "CCXT",
) -> List[imvcdcadlo.FullSymbol]:
    """
    Load vendor universe as full symbols.

    :param version: release version
    :param vendor: vendor to load data for
    :return: vendor universe as full symbols
    """
    # Get vendor universe.
    vendor_universe = get_trade_universe(version)[vendor]
    # Convert vendor universe dict to a sorted list of full symbols.
    full_symbols = [
        # TODO(Grisha): use "_" as currencies separator #579.
        imvcdcadlo.construct_full_symbol(exchange_id, currency_pair.replace("/", "_"))
        for exchange_id, currency_pairs in vendor_universe.items()
        for currency_pair in currency_pairs
    ]
    sorted_full_symbols = sorted(full_symbols)
    return sorted_full_symbols

