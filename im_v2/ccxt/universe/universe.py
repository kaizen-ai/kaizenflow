"""
Import as:

import im_v2.ccxt.universe.universe as imvccunun
"""
import os
from typing import Dict, List

import helpers.dbg as hdbg
import helpers.git as hgit
import helpers.io_ as hio
import im_v2.common.data.client as imvcdcadlo

_LATEST_UNIVERSE_VERSION = "v03"
_EXCHANGE_IDS_MAPPING_PATH = os.path.join(
    hgit.get_amp_abs_path(), "im_v2/ccxt/universe/exchange_ids_mapping.json"
)
_CURRENCY_PAIR_IDS_MAPPING_PATH = os.path.join(
    hgit.get_amp_abs_path(), "im_v2/ccxt/universe/currency_pair_ids_mapping.json"
)


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
        hgit.get_amp_abs_path(), "im_v2/ccxt/universe", file_name
    )
    hdbg.dassert_exists(file_path)
    universe = hio.from_json(file_path)
    return universe  # type: ignore[no-any-return]


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
        imvcdcadlo.construct_full_symbol(exchange_id, currency_pair)
        for exchange_id, currency_pairs in vendor_universe.items()
        for currency_pair in currency_pairs
    ]
    sorted_full_symbols = sorted(full_symbols)
    return sorted_full_symbols


def get_full_symbol_id(full_symbol: imvcdcadlo.FullSymbol) -> int:
    """
    Get full symbol numeric id via exchange and currency pair ids mappings.

    :param full_symbol: full symbol
    :return: full symbol numeric id
    """
    # Extract numeric id mappings.
    exchange_ids_mapping = hio.from_json(_EXCHANGE_IDS_MAPPING_PATH)
    currency_pair_ids_mapping = hio.from_json(_CURRENCY_PAIR_IDS_MAPPING_PATH)
    # Get exchange id and currency pair from full symbol.
    exchange_id, currency_pair = imvcdcadlo.parse_full_symbol(full_symbol)
    # Verify that exchange id and currency pair are present in mappings.
    hdbg.dassert_in(exchange_id, exchange_ids_mapping)
    hdbg.dassert_in(currency_pair, currency_pair_ids_mapping)
    # Construct unique full symbol's numeric id by joining exchange id and
    # currency pair numeric ids with "0".
    full_symbol_id = int(
        "0".join(
            [
                str(exchange_ids_mapping[exchange_id]),
                str(currency_pair_ids_mapping[currency_pair]),
            ]
        )
    )
    return full_symbol_id


def get_vendor_universe_ids(
    version: str = _LATEST_UNIVERSE_VERSION,
    vendor: str = "CCXT",
) -> List[int]:
    """
    Load vendor universe as numeric ids of full symbols.

    :param version: release version
    :param vendor: vendor to load data for
    :return: numeric ids of full symbols
    """
    full_symbols = get_vendor_universe(version, vendor)
    full_symbol_ids = [get_full_symbol_id(symbol) for symbol in full_symbols]
    return full_symbol_ids
