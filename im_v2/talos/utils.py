"""
Import as:

import im_v2.talos.utils as imv2tauti
"""
import datetime
import logging

import helpers.hsecrets as hsecret
import helpers.hdbg as hdbg

_LOG = logging.getLogger(__name__)
_TALOS_HOST = "talostrading.com"


def get_endpoint(environment: str) -> str:
    """
    Get entrypoint to Talos.
    """
    hdbg.dassert_in(environment, ["sandbox", "prod"])
    if environment == "sandbox":
        endpoint = f"sandbox.{_TALOS_HOST}"
    else:
        hdbg.dfatal(
            "Incorrect account type. Supported environment: 'sandbox'."
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
