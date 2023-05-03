"""
Transformation data for Reddit.

Import as:

import sorrentum_sandbox.examples.reddit.transform as ssexretr
"""
import logging
import re
from typing import List, Optional, Tuple

import pandas as pd

_LOG = logging.getLogger(__name__)

def get_nvt_ratio(market_cap, transaction_volume):
    """
    NVT Ratio = Market Cap / Transaction Volume

    :param: Market Cap & Transaction Volume 24h
    :return: NVT Ratio
    """
    if transaction_volume == 0:
        return 0
    return market_cap / transaction_volume


def get_mc_fdmc_ratio(market_cap, fully_diluted_market_cap):
    """
    :param: Market Cap & Fully Diluted Market Cap
    :return: Market Cap / Fully Diluted Market Cap
    """
    if fully_diluted_market_cap == 0:
        return 0
    return market_cap / fully_diluted_market_cap


def extract_features(data: pd.DataFrame) -> pd.DataFrame:
    """
    Extract features from list of posts.

    :param data: list of bitcoin data
    :return: List of features
    """
    output = []
    for _, bitcoin_data in data.iterrows():
        nvt_ratio = get_nvt_ratio(
            bitcoin_data["market_cap"], 
            bitcoin_data["transaction_volume"]
        )
        mc_fdmc_ratio = get_mc_fdmc_ratio( 
            bitcoin_data["market_cap"],
            bitcoin_data["fully_diluted_market_cap"],
        )

        features = {
            "name": bitcoin_data["name"],
            "price": bitcoin_data["price"],
            "nvt_ratio": nvt_ratio,
            "mc_fdmc_ratio": mc_fdmc_ratio,
            "market_cap_dominance": bitcoin_data["market_cap_dominance"],
            "last_updated": bitcoin_data["last_updated"],
        }
        output += [features]
    return pd.DataFrame(output)