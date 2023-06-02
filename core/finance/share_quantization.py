"""
Import as:

import core.finance.share_quantization as cfinquant
"""
import logging
from typing import Dict, Optional, Union

import numpy as np
import pandas as pd

import helpers.hdbg as hdbg
import helpers.hpandas as hpandas

_LOG = logging.getLogger(__name__)


def quantize_shares(
    shares: Union[pd.Series, pd.DataFrame],
    quantization: str,
    asset_id_to_decimals: Optional[Dict[int, int]] = None,
) -> pd.DataFrame:
    """
    Keep factional shares or round to the nearest share or lot.

    In vectorized flows, `shares` is a time-indexed dataframe.
    In incremental flows, `shares` is a series.

    quantization: modes are:
      - "no_quantization"
      - "nearest_share"
      - "nearest_lot"
      _ "asset_specific"
    """
    # Perform sanity-checks.
    hdbg.dassert_type_in(shares, [pd.Series, pd.DataFrame])
    #
    if quantization == "no_quantization":
        hdbg.dassert(
            asset_id_to_decimals is None,
            "asset_id_to_decimals must be `None` when `quantization`=%s",
            quantization,
        )
        quantized_shares = shares
    elif quantization == "nearest_share":
        hdbg.dassert(
            asset_id_to_decimals is None,
            "asset_id_to_decimals must be `None` when `quantization`=%s",
            quantization,
        )
        quantized_shares = np.rint(shares)
    elif quantization == "nearest_lot":
        hdbg.dassert(
            asset_id_to_decimals is None,
            "asset_id_to_decimals must be `None` when `quantization`=%s",
            quantization,
        )
        # It is not clear whether it is best to use
        #   - 100 * np.rint(shares / 100)
        #   - np.around(shares, -2)
        #   - round(shares, -2)
        #   - shares.round(-2)
        #
        # E.g., from https://numpy.org/doc/stable/reference/generated/numpy.around.html:
        #   - > np.round(56294995342131.5, 3)
        #     56294995342131.51
        #     (due to IEEE floating point standard intricacies)
        #   - "...Python’s builtin round function uses a more accurate but
        #      slower algorithm for 64-bit floating point values"
        quantized_shares = shares.round(-2)
    elif quantization == "asset_specific":
        hdbg.dassert_isinstance(
            asset_id_to_decimals,
            dict,
            "asset_id_to_decimals must be a `dict` when `quantization`=%s",
            quantization,
        )
        quantized_shares = shares
        # Convert series to a dataframe so that we can use `pd.DataFrame.round()`.
        if isinstance(quantized_shares, pd.Series):
            quantized_shares = quantized_shares.to_frame().T
        # Ensure `asset_id_to_decimals` has an asset_id for every asset.
        hdbg.dassert_is_subset(
            quantized_shares.columns, asset_id_to_decimals.keys()
        )
        # Perform the rounding.
        quantized_shares = quantized_shares.round(asset_id_to_decimals)
        # If series-to-dataframe conversion was performed, undo it here.
        if isinstance(shares, pd.Series):
            quantized_shares = quantized_shares.squeeze()
            quantized_shares.name = shares.name
    else:
        raise ValueError(f"Invalid quantization strategy `{quantization}`")
    _LOG.debug("`quantized_shares`=\n%s", hpandas.df_to_str(quantized_shares))
    return quantized_shares
