"""
This file contains various utilities for interacting with Talos
API.

Import as:

import im_v2.talos.utils as imvtut
"""

import pandas as pd
import helpers.hdatetime as hdateti

def timestamp_to_talos_iso_8601(timestamp: pd.Timestamp) -> str:
    """
    Transform Timestamp in UTC into a string in the format accepted by Talos API.

    Example:
    2019-10-20T15:00:00+00:00 -> 2019-10-20T15:00:00.000000Z

    :param timestampt: specify if this instance should call the 'sandbox'
          or 'prod' API
    """
    # Talos operates strictly with UTC timestamps.
    hdateti.dassert_has_UTC_tz(timestamp)
    timestamp_iso_8601 = timestamp.strftime("%Y-%m-%dT%H:%M:%S.000000Z")
    return timestamp_iso_8601  # type: ignore


