"""
Import as:

import im.data.universe as imdauni
"""

import os
from typing import Dict, List

import helpers.git as hgit
import helpers.io_ as hio

_LATEST_UNIVERSE_VERSION = "02"


def get_trade_universe(
    version: str = _LATEST_UNIVERSE_VERSION,
) -> Dict[str, Dict[str, List[str]]]:
    """
    Load trade universe for which we have historical data on S3.

    :param version: release version
    :return: trade universe
    """
    file_name = "".join(["universe_", version, ".json"])
    file_path = os.path.join(hgit.get_amp_abs_path(), "im/data", file_name)
    universe = hio.from_json(file_path)
    # TODO(Grisha): remove CDD from the universe, file a bug for it.
    universe.pop("CDD")
    return universe
