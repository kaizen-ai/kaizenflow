"""
Import as:

import im_v2.talos.utils as imv2tauti
"""
import datetime
import logging

import helpers.hdbg as hdbg

_LOG = logging.getLogger(__name__)


def get_endpoint(account: str) -> str:
    """
    Get entrypoint to Talos API.
    """
    if account == "talos_sandbox":
        endpoint = "tal-87.sandbox.talostrading.com"
    else:
        hdbg.dfatal(
            "Incorrect account type. Supported account types: 'talos_sandbox'."
        )
    return endpoint


def get_talos_current_utc_timestamp() -> str:
    """
    Return the current UTC timestamp in Talos-acceptable format.

    Example: 2019-10-20T15:00:00.000000Z
    """
    utc_datetime = datetime.datetime.utcnow().strftime(
        "%Y-%m-%dT%H:%M:%S.000000Z"
    )
    return utc_datetime


# timestamp.strftime(
#    "%Y-%m-%dT%H:%M:%S.000000Z"
# )
