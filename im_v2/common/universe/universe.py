"""
Import as:

import im_v2.common.universe.universe as imvcounun
"""
import glob
import os
from typing import Dict, List, Optional, Union

import helpers.hdbg as hdbg
import helpers.hgit as hgit
import helpers.hio as hio
import helpers.hstring as hstring
import im_v2.common.universe.full_symbol as imvcufusy


def _get_universe_file_path(
    vendor: str, mode: str, *, version: Optional[str] = None
) -> str:
    """
    Get universe file path based on version.

    :param vendor: vendor to load data for (e.g., CCXT, Talos)
    :param mode: download or trade universe
    :param version: universe version (e.g. "v01"). If None it uses
      the latest version available
    :return: file path to the universe file corresponding to the specified version
    """
    hdbg.dassert_in(mode, ["download", "trade"])
    vendor = vendor.lower()
    # Get path to vendor universe dir.
    vendor_dir = os.path.join(
        hgit.get_amp_abs_path(), f"im_v2/{vendor}/universe/{mode}"
    )
    hdbg.dassert_dir_exists(vendor_dir)
    if version is None:
        # Find all universe files.
        vendor_universe_pattern = os.path.join(vendor_dir, f"universe_v*.json")
        universe_files = list(glob.glob(vendor_universe_pattern))
        hdbg.dassert_ne(len(universe_files), 0)
        file_path = max(
            universe_files, key=hstring.extract_version_from_file_name
        )
    else:
        # TODO(Juraj): #1487 Assert version format (include 'small').
        file_name = "".join(["universe_", version, ".json"])
        file_path = os.path.join(vendor_dir, file_name)
    hdbg.dassert_path_exists(file_path)
    return file_path


def _get_vedor_universe(
    vendor: str,
    mode: str,
    *,
    version: Optional[str] = None,
) -> Dict[str, List[str]]:
    """
    Load universe specific of a vendor.

    :param vendor: vendor to load data for (e.g., CCXT/Talos)
    :return: trade universe as a nested dictionary of
      exchange name (e.g., binance) to list of symbols e.g.,
        {
            "CCXT": {
                "binance": [
                "ADA_USDT",
                "AVAX_USDT",
                "BNB_USDT",
                "BTC_USDT",
                "DOGE_USDT",
                "EOS_USDT",
                "ETH_USDT",
                "LINK_USDT",
                "SOL_USDT"
                ],
                ...
            }
        }
    """
    # TODO(Grisha): consider always converting a vendor to lowercase.
    file_path = _get_universe_file_path(vendor, mode, version=version)
    hdbg.dassert_path_exists(file_path)
    universe = hio.from_json(file_path)
    hdbg.dassert_in(vendor, universe, "Invalid vendor=`%s`", vendor)
    vendor_universe = universe[vendor]
    return vendor_universe  # type: ignore[no-any-return]


def get_vendor_universe(
    vendor: str,
    mode: str,
    *,
    version: Optional[str] = None,
    as_full_symbol: bool = False,
) -> Union[List[imvcufusy.FullSymbol], Dict[str, Dict[str, List[str]]]]:
    """
    Load vendor universe either as a list of currency pairs per each vendor or
    list of full symbols.

    :param as_full_symbol: if True transform the universe into list of
        full symbols e.g. gateio::XRP_USDT
    :return: vendor universe as a list of symbol or list of full symbols e.g.:
        {
            "talos": {
                "binance": [
                "ADA_USDT",
                "AVAX_USDT",
                "BNB_USDT",
                "BTC_USDT",
                "DOGE_USDT",
                "EOS_USDT",
                "ETH_USDT",
                "LINK_USDT",
                "SOL_USDT"
                ],
                ...
        }
        or ["gateio::XRP_USDT", "kucoin::SOL_USDT"]
    """
    vendor_universe = _get_vedor_universe(vendor, mode, version=version)
    if as_full_symbol:
        # Convert vendor universe dict to a sorted list of full symbols.
        vendor_universe = [
            imvcufusy.build_full_symbol(exchange_id, currency_pair)
            for exchange_id, currency_pairs in vendor_universe.items()
            for currency_pair in currency_pairs
        ]
        # Sort list of symbols in the universe.
        vendor_universe = sorted(vendor_universe)
    return vendor_universe
