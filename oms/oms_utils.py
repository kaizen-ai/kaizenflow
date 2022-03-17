"""
Import as:

import oms.oms_utils as oomsutil
"""
import base64
import collections
import datetime
import hashlib
import hmac
import logging
import uuid
from typing import Dict, List

import numpy as np
import pandas as pd

import helpers.hdbg as hdbg

_LOG = logging.getLogger(__name__)


def _timestamp_to_str(timestamp: pd.Timestamp) -> str:
    """
    Print timestamp as string only in terms of time.

    This is useful to simplify the debug output for intraday trading.
    """
    val = "'%s'" % str(timestamp.time())
    return val


def _get_col_name(col_name: str, prefix: str) -> str:
    if prefix != "":
        col_name = prefix + "." + col_name
    return col_name


# #############################################################################
# Accounting functions.
# #############################################################################

# Represent a set of DataFrame columns that is built incrementally.
Accounting = Dict[str, List[float]]


def _create_accounting_stats(columns: List[str]) -> Accounting:
    """
    Create incrementally built dataframe with the given columns.
    """
    accounting = collections.OrderedDict()
    for column in columns:
        accounting[column] = []
    return accounting


def _append_accounting_df(
    df: pd.DataFrame,
    accounting: Accounting,
    prefix: str,
) -> pd.DataFrame:
    """
    Update `df` with the intermediate results stored in `accounting`.
    """
    dfs = []
    for key, value in accounting.items():
        _LOG.debug("key=%s", key)
        # Pad the data so that it has the same length as `df`.
        num_vals = len(accounting[key])
        num_pad = df.shape[0] - num_vals
        hdbg.dassert_lte(0, num_pad)
        buffer = [np.nan] * num_pad
        # Get the target column name.
        col_name = _get_col_name(key, prefix)
        # Create the column of the data frame.
        df_out = pd.DataFrame(value + buffer, index=df.index, columns=[col_name])
        hdbg.dassert_eq(df_out.shape[0], df.shape[0])
        dfs.append(df_out)
    # Concat all the data together with the input.
    df_out = pd.concat([df] + dfs, axis=1)
    return df_out


# #############################################################################
# Accounting functions.
# #############################################################################


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


def get_order_id() -> str:
    """
    Get an order ID in UUID4 format.
    """
    return str(uuid.uuid4())


def get_talos_current_utc_timestamp() -> str:
    """
    Return the current UTC timestamp in Talos-acceptable format.

    Example: 2019-10-20T15:00:00.000000Z
    """
    utc_datetime = datetime.datetime.utcnow().strftime(
        "%Y-%m-%dT%H:%M:%S.000000Z"
    )
    return utc_datetime
