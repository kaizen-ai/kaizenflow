"""
Import as:

import im_v2.talos.utils as imv2tauti
"""
import base64
import datetime
import hashlib
import hmac
import logging
from typing import Dict, List, Optional

import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hsecrets as hsecret

_LOG = logging.getLogger(__name__)
_TALOS_HOST = "talostrading.com"


def timestamp_to_talos_iso_8601(timestamp: pd.Timestamp) -> str:
    """
    Transform Timestamp into a string in the format accepted by Talos API.

    Example:
    2019-10-20T15:00:00.000000Z

    Note: microseconds must be included.
    """

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


class TalosApiBuilder:
    """
    Base class containing the methods for Talos API access.
    """

    def __init__(self, account: str):
        self._account = account
        self._api_keys = hsecret.get_secret(f"talos_{self._account}")
        # Talos request endpoint.
        self._endpoint = self.get_endpoint()

    @staticmethod
    def calculate_signature(secret_key: str, parts: List[str]) -> str:
        """
        Encode the request using secret key.

        Requires parts of the API request provided as a list, e.g.:

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

    def build_parts(
        self, request_type: str, wall_clock_timestamp: str, path: str
    ) -> List[str]:
        """
        Combine initial parts of a GET or POST request.

        The parts include a timestamp, endpoint and path, e.g.:

        ```
        [
        "GET",
        "2019-10-20T15:00:00.000000Z",
        "sandbox.talostrading.com",
        "/v1/orders"
        ]
        ```

        :param request_type: GET or POST
        :param wall_clock_timestamp: time of request creation
        :param path: part of url after endpoint, e.g. "/v1/orders"
        :return parts: parts of request
        """
        hdbg.dassert_in(
            request_type, ["GET", "POST"], msg="Incorrect request type"
        )
        parts = [request_type, wall_clock_timestamp, self._endpoint, path]
        return parts

    def build_headers(
        self, parts: Optional[List[str]], wall_clock_timestamp: Optional[str]
    ) -> Dict[str, str]:
        """
        Build parts of the request metadata.

        This includes providing public key and encoding request
        with secret key for Talos authorization.

        :param parts: parts of request
        :param wall_clock_timestamp: time of request submission
        :return: headers for Talos request
        """
        headers = {"TALOS-KEY": self._api_keys["apiKey"]}
        if parts:
            signature = self.calculate_signature(
                self._api_keys["secretKey"], parts
            )
            headers["TALOS-SIGN"] = signature
        if wall_clock_timestamp:
            headers["TALOS-TS"] = wall_clock_timestamp
        return headers

    def get_endpoint(self) -> str:
        """
        Get entrypoint to Talos. The only environment we currently support is
        `sandbox`.

        :return: Talos endpoint, e.g. "sandbox.talostrading.com"
        """
        if self._account == "sandbox":
            endpoint = f"sandbox.{_TALOS_HOST}"
        else:
            hdbg.dfatal(
                "Incorrect account type. Supported environments: 'sandbox'."
            )
        return endpoint
