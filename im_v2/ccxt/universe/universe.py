"""
Import as:

import im_v2.ccxt.universe.universe as imvccunun
"""
import functools
import hashlib
import os
from typing import Dict, List, Tuple, Union

import helpers.hdbg as hdbg
import helpers.hgit as hgit
import helpers.hio as hio
import im_v2.common.data.client as icdc

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
        hgit.get_amp_abs_path(), "im_v2/ccxt/universe", file_name
    )
    hdbg.dassert_exists(file_path)
    universe = hio.from_json(file_path)
    return universe  # type: ignore[no-any-return]


# TODO(Dan): remove default values for `vendor` and `as_ids` param #832.
def get_vendor_universe(
    version: str = _LATEST_UNIVERSE_VERSION,
    vendor: str = "CCXT",
    as_ids: bool = False,
) -> Union[List[icdc.FullSymbol], List[int]]:
    """
    Load vendor universe as full symbols or numeric ids.

    :param version: release version
    :param vendor: vendor to load data for (e.g., CCXT, CDD)
    :param as_ids: if True return universe as numeric ids, otherwise universe as full symbols
    :return: vendor universe as full symbols (e.g., gateio::XRP_USDT)
    """
    # Get vendor universe.
    vendor_universe = get_trade_universe(version)[vendor]
    # Convert vendor universe dict to a sorted list of full symbols.
    universe = [
        icdc.construct_full_symbol(exchange_id, currency_pair)
        for exchange_id, currency_pairs in vendor_universe.items()
        for currency_pair in currency_pairs
    ]
    if as_ids:
        # Convert universe symbols to numeric ids.
        universe_tuple = tuple(universe)
        universe = list(build_num_to_string_id_mapping(universe_tuple).keys())
    # Sort list of symbols in the universe.
    universe = sorted(universe)
    return universe


def string_to_num_id(string_id: str) -> int:
    """
    Convert string id into a numeric one.

    :param string_id: string id to convert
    :return: numeric id
    """
    # Initialize MD5 algorithm converter and update it with string id.
    converter = hashlib.md5()
    converter.update(string_id.encode("utf-8"))
    # Get hexadecimal numeric id.
    num_id = converter.hexdigest()
    # Convert hexadecimal id to decimal one.
    num_id = int(num_id, 16)
    # Shorten full numeric id to 10 symbols.
    num_id = int(str(num_id)[:10])
    return num_id


@functools.lru_cache()
def build_num_to_string_id_mapping(universe: Tuple[str, ...]) -> Dict[int, str]:
    """
    Build a mapping from numeric ids to string ones.

    :param universe: universe of string ids to convert
    :return: numeric to string ids mapping
    """
    mapping: Dict[int, str] = {}
    for string_id in universe:
        # Convert string id to a numeric one.
        num_id = string_to_num_id(string_id)
        hdbg.dassert_not_in(
            num_id,
            mapping,
            "Collision: id %s for string `%s` already exists",
            num_id,
            string_id,
        )
        mapping[num_id] = string_id
    return mapping
