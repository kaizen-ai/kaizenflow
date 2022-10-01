"""
Import as:

import core.finance.share_quantization as cfinquant
"""
import logging
from typing import Union

import numpy as np
import pandas as pd

import helpers.hdbg as hdbg
import helpers.hpandas as hpandas

_LOG = logging.getLogger(__name__)


def quantize_shares(
    shares: Union[pd.Series, pd.DataFrame],
    quantization: str,
) -> pd.DataFrame:
    """
    Keep factional shares or round to the nearest share or lot.
    """
    # Perform sanity-checks.
    hdbg.dassert_type_in(shares, [pd.Series, pd.DataFrame])
    #
    if quantization == "no_quantization":
        quantized_shares = shares
    elif quantization == "nearest_share":
        quantized_shares = np.rint(shares)
    elif quantization == "nearest_lot":
        quantized_shares = 100 * np.rint(shares / 100)
    else:
        raise ValueError(f"Invalid quantization strategy `{quantization}`")
    _LOG.debug("`quantized_shares`=\n%s", hpandas.df_to_str(quantized_shares))
    return quantized_shares
