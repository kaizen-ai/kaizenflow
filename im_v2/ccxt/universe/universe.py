"""
Import as:

import im_v2.ccxt.universe.universe as imvccunun
"""
import hashlib
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


def string_to_num_id(string_id: str) -> int:
    """
    Convert string id to a numeric one.

    :param string_id: string id to convert
    :return: numeric id
    """
    # Initialize MD5 algorithm converter and update it with string id.
    converter = hashlib.md5()
    converter.update(string_id.encode("utf-8"))
    # Convert string id to integer and take first 10 elements for numeric id.
    num_id = int(str(int(converter.hexdigest(), 16))[:10])
    return num_id


def get_vendor_universe_num_ids(
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
    full_symbol_ids = [string_to_num_id(symbol) for symbol in full_symbols]
    return full_symbol_ids


def build_num_to_string_id_mapping(universe: List[str]) -> Dict[int, str]:
    """
    Build a mapping from numeric ids to string ones.

    :param universe: universe of numeric ids to convert
    :return: numeric to string ids mapping
    """
    mapping = {}
    # Convert each numeric id to a string one and add the pair to the mapping.
    for string_id in universe:
        num_id = string_to_num_id(string_id)
        mapping[num_id] = string_id
    return mapping
