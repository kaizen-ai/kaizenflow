"""
Import as:

import im_v2.talos.utils as imv2tauti
"""
import datetime
import logging

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


def get_talos_current_utc_timestamp() -> str:
    """
    Return the current UTC timestamp in the format acceptable by Talos.

    Example: 2019-10-20T15:00:00.000000Z
    """
    # Timestamp converter.
    utc_datetime = datetime.datetime.utcnow().strftime(
        "%Y-%m-%dT%H:%M:%S.000000Z"
    )
    # Timezone check.
    hdateti.dassert_has_UTC_tz(utc_datetime)
    return utc_datetime
