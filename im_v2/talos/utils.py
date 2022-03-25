"""
Import as:

import im_v2.talos.utils as imv2tauti
"""
import abc
import base64
import datetime
import hashlib
import hmac
import logging
from typing import Any, Dict, List

import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hsecrets as hsecret

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


class TalosApiBase(abc.ABC):

    def __init__(self, account: str):
        self._account = account
        self._api_keys = hsecret.get_secret(self._account)
        # Talos request endpoint.
        self._endpoint = get_endpoint(self._account)

    def build_parts(
            self, wall_clock_timestamp: str, path: str, request_type: str
    ) -> List[str]:
        """
        
        """
        hdbg.dassert_in(
            request_type, ["GET", "POST"], msg="Incorrect request type"
        )
        parts = [request_type, wall_clock_timestamp, self._endpoint, path]
        return parts

    def build_headers(
            self, parts: List[str], wall_clock_timestamp: str
    ) -> Dict[str, str]:
        """
        
        """
        signature = self.calculate_signature(self._api_keys["secretKey"], parts)
        headers = {
            "TALOS-KEY": self._api_keys["apiKey"],
            "TALOS-SIGN": signature,
            "TALOS-TS": wall_clock_timestamp,
        }
        return headers

    @abc.abstractmethod
    def build_url(self, path: str, **kwargs: Any) -> str:
        """
        
        """
        ...

    def calculate_signature(self, secret_key: str, parts: List[str]) -> str:
        """
        Encode the request using secret key.

        Require parts of the API request provided as a list, e.g.:

        ```
        [
            "POST",
            str(utc_datetime),
            "tal-87.sandbox.talostrading.com",
            "/v1/orders",
        ]
        ```

        :param secret_key: secret key used for encoding
        :param parts: parts of the GET or POST request
        :return: an encoded string
        """
        payload = "\n".join(parts)
        hash = hmac.new(
            secret_key.encode("ascii"),
            payload.encode("ascii"),
            hashlib.sha256,
        )
        hash.hexdigest()
        signature = base64.urlsafe_b64encode(hash.digest()).decode()
        return signature