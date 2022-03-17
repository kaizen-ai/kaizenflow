"""
Import as:

import im_v2.common.universe.universe as imvcounun
"""
import os
import glob
from typing import Dict, List, Union, Optional

import helpers.hdbg as hdbg
import helpers.hgit as hgit
import helpers.hio as hio
import im_v2.common.data.client as icdc

def get_trade_universe(vendor: str, *,
    version: Optional[str] = None,
) -> Dict[str, Dict[str, List[str]]]:
    """
    Load trade universe for which we have historical data on S3.

    :param vendor: vendor to load data for (e.g., CCXT/Talos)
    :param version: release version
    :return: trade universe
    """
    file_path = get_universe_file_path(vendor, version)
    hdbg.dassert_exists(file_path)
    universe = hio.from_json(file_path)
    return universe  # type: ignore[no-any-return]


# TODO(Dan): remove default values for `vendor` param #832.
def get_vendor_universe(*,
    version: Optional[str] = None, vendor: str = "CCXT"
) -> Union[List[icdc.FullSymbol], List[int]]:
    """
    Load vendor universe as full symbols.

    :param version: release version
    :param vendor: vendor to load data for (e.g., CCXT, Talos)
    :return: vendor universe as full symbols (e.g., gateio::XRP_USDT)
    """
    # Get vendor universe.
    vendor_universe = get_trade_universe(vendor, version)
    # Convert vendor universe dict to a sorted list of full symbols.
    universe = [
        icdc.build_full_symbol(exchange_id, currency_pair)
        for exchange_id, currency_pairs in vendor_universe.items()
        for currency_pair in currency_pairs
    ]
    # Sort list of symbols in the universe.
    universe = sorted(universe)
    return universe

def get_universe_file_path(vendor: str, version: Optional[str]) -> str:
    """
    Get universe file based on version. If version is not provided
     assumes the latest version
    
    :param vendor: vendor to load data for (e.g., CCXT, Talos)
    :param version: universe release version
    :return: file path to the universe file of specified version
    """
    vendor = vendor.lower()
    if version is None:
        # Get path to vendor dir.
        vendor_dir = os.path.join(hgit.get_amp_abs_path(), f"im_v2/{vendor}/universe/")
        hdbg.dassert_dir_exists(vendor_dir)
        vendor_universe_pattern = os.path.join(vendor_dir, "universe_v*.json")
        # Find all universe files.
        universe_files = list(glob.glob(vendor_universe_pattern))
        hdbg.dassert_ne(len(universe_files), 0)
        # Assuming 0 < version <= 99.
        get_vers_part = lambda x: int(x.rstrip(".json")[-2])
        file_path = max(universe_files, key=get_vers_part)
    else:
        file_name = "".join(["universe_", version, ".json"])
        file_path = os.path.join(
            hgit.get_amp_abs_path(), f"im_v2/{vendor}/universe", file_name
        )
    return file_path