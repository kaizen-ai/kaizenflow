"""
Import as:

import im_v2.talos.utils as imv2tauti
"""
import datetime
import logging

import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg

_LOG = logging.getLogger(__name__)
_TALOS_HOST = "talostrading.com"


def get_endpoint(environment: str) -> str:
    """
    Get entrypoint to Talos. The only environment we currently support is
    `sandbox`.

    :param environment: i.e., `sandbox`
    :return:
    """
    if environment == "sandbox":
        endpoint = f"sandbox.{_TALOS_HOST}"
    else:
        hdbg.dfatal("Incorrect account type. Supported environments: 'sandbox'.")
    return endpoint


def timestamp_to_talos_iso_8601(timestamp: pd.Timestamp) -> str:
    """
    Transform Timestamp into a string in the format accepted by Talos API.

    Example:
    2019-10-20T15:00:00.000000Z

    Note: microseconds must be included.
    """
    # Timezone check.
    hdateti.dassert_has_UTC_tz(timestamp)
    # Timestamp converter.
    timestamp_iso_8601 = timestamp.strftime("%Y-%m-%dT%H:%M:%S.000000Z")
    return timestamp_iso_8601  # type: ignore


def get_talos_current_utc_timestamp() -> str:
    """
    Return the current UTC timestamp in the format acceptable by Talos.

    Example: 2019-10-20T15:00:00.000000Z
    """
    utc_datetime = datetime.datetime.utcnow().strftime(
        "%Y-%m-%dT%H:%M:%S.000000Z"
    )
    return utc_datetime
