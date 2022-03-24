"""
Import as:

import oms.oms_talos_utils as oomtauti
"""
import base64
import datetime
import hashlib
import hmac
import logging
import uuid
from typing import List

import helpers.hdbg as hdbg

_LOG = logging.getLogger(__name__)


def calculate_signature(secret_key: str, parts: List[str]) -> str:
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


def get_order_id() -> str:
    """
    Get an order ID in UUID4 format.
    """
    return str(uuid.uuid4())
