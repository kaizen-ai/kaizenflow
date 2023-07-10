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


def _get_universe_dir(vendor: str, mode: str) -> str:
    """
    Get the dir containing universe files for the specified vendor and mode.

    :param vendor: vendor to load data for (e.g., CCXT, Talos)
    :param mode: download or trade universe
    :return: universe dir
    """
    hdbg.dassert_in(mode, ["download", "trade"])
    vendor = vendor.lower()
    universe_dir = os.path.join(
        hgit.get_amp_abs_path(), f"im_v2/{vendor}/universe/{mode}"
    )
    hdbg.dassert_dir_exists(universe_dir)
    return universe_dir


def _get_universe_files(universe_dir: str) -> List[str]:
    """
    Get full paths to all the files in a universe dir.

    :param universe_dir: dir containing universe files
    :return: paths to all the universe files
    """
    universe_files_pattern = os.path.join(universe_dir, "universe_v*.json")
    universe_files = sorted(list(glob.glob(universe_files_pattern)))
    hdbg.dassert_ne(len(universe_files), 0)
    return universe_files


def _get_universe_file_path(
    vendor: str, mode: str, *, version: Optional[str] = None
) -> str:
    """
    Get universe file path.

    :param vendor: vendor to load data for (e.g., CCXT, Talos)
    :param mode: download or trade universe
    :param version: universe version (e.g., "v01")
        - If `None`, it uses the latest version available
    :return: file path to the universe file
    """
    universe_dir = _get_universe_dir(vendor, mode)
    if version is None:
        # Get all the universe files from the dir.
        universe_files = _get_universe_files(universe_dir)
        # Select the latest universe version available.
        file_path = max(
            universe_files, key=hstring.extract_version_from_file_name
        )
    else:
        file_name = "".join(["universe_", version, ".json"])
        file_path = os.path.join(universe_dir, file_name)
    hdbg.dassert_path_exists(file_path)
    return file_path


def get_universe_versions(vendor: str, mode: str) -> List[str]:
    """
    Get all the universe versions for the specified vendor and mode.

    :param vendor: vendor to load data for (e.g., CCXT, Talos)
    :param mode: download or trade universe
    :return: universe versions (e.g., `["v1", "v2", "v2.1"]`)
    """
    universe_dir = _get_universe_dir(vendor, mode)
    universe_files = _get_universe_files(universe_dir)
    # Get universe versions from the file names.
    universe_versions_tuples = [
        hstring.extract_version_from_file_name(file_) for file_ in universe_files
    ]
    # Put universe versions in a readable format.
    # E.g., from `[(1, 0), (2, 0), (2, 1)]` to `["v1", "v2", "v2.1"]`.
    universe_versions = [
        "v" + ".".join(map(str, tuple_)).rstrip(".0")
        for tuple_ in universe_versions_tuples
    ]
    universe_versions = sorted(universe_versions)
    return universe_versions


# #############################################################################


def _get_vendor_universe(
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
    file_path = _get_universe_file_path(vendor, mode, version=version)
    hdbg.dassert_path_exists(file_path)
    universe = hio.from_json(file_path)
    # Convert vendor name to lowercase.
    vendor = vendor.lower()
    universe = {k.lower(): v for k, v in universe.items()}
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
    vendor_universe = _get_vendor_universe(vendor, mode, version=version)
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
