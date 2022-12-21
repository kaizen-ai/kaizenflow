"""
Import as:

import research_amp.cc.algotrading as ramccalg
"""

from typing import List, Optional

import pandas as pd

import core.config as cconfig
import dataflow.universe as dtfuniver
import helpers.hdbg as hdbg
import im_v2.common.data.client as icdc
import im_v2.crypto_chassis.data.client as iccdc
import market_data as mdata

# #############################################################################
# Notebook Config examples
# #############################################################################


def get_default_config(
    *,
    start_ts: Optional[pd.Timestamp] = pd.Timestamp("2022-12-14 00:00:00+00:00"),
    end_ts: Optional[pd.Timestamp] = pd.Timestamp("2022-12-15 00:00:00+00:00"),
) -> cconfig.Config:
    """
    Get a config for a notebook with algorithmic trading experiments.

    - Latest universe (v3)
    - Resampled to 1sec
    - For 1 asset and 1 day
    """
    dict_ = {
        "client_config": {
            "resample_1min": False,
            "tag": "downloaded_1sec",
            "contract_type": "futures",
            "universe": {
                "full_symbols": ["binance::ADA_USDT"],
                "universe_version": "v3",
            },
        },
        "market_data_config": {"start_ts": start_ts, "end_ts": end_ts},
    }
    config = cconfig.Config.from_dict(dict_)
    return config


# #############################################################################
# Client and MarketData initialization
# #############################################################################


def get_bid_ask_ImClient(config: cconfig.Config) -> icdc.ImClient:
    """
    Get a historical client for bid/ask data.
    """
    # Set up the parameters for initialization of the IM Client.
    universe_version = config.get_and_mark_as_used(
        ("client_config", "universe", "universe_version")
    )
    resample_1min = config.get_and_mark_as_used(
        ("client_config", "resample_1min")
    )
    contract_type = config.get_and_mark_as_used(
        ("client_config", "contract_type")
    )
    tag = config.get_and_mark_as_used(("client_config", "tag"))
    client = iccdc.get_CryptoChassisHistoricalPqByTileClient_example2(
        universe_version, resample_1min, contract_type, tag
    )
    config["client_config"]["client"] = client
    return client


def get_universe(config: cconfig.Config) -> List[int]:
    """
    Load asset IDs based on config universe and symbols.
    """
    universe_version = config.get_and_mark_as_used(
        ("client_config", "universe", "universe_version")
    )
    client = config.get_and_mark_as_used(("client_config", "client"))
    # Verify that provided symbols are present in the client.
    universe_string = f"crypto_chassis_{universe_version}-all"
    universe_full_symbols = dtfuniver.get_universe(universe_string)
    config_full_symbols = config.get_and_mark_as_used(
        ("client_config", "universe", "full_symbols")
    )
    hdbg.dassert_is_subset(config_full_symbols, universe_full_symbols)
    # Convert to asset ids.
    config["client_config"]["universe"][
        "asset_ids"
    ] = client.get_asset_ids_from_full_symbols(config_full_symbols)
    asset_ids = config.get_and_mark_as_used(
        ("client_config", "universe", "asset_ids")
    )
    return asset_ids


def get_market_data(config: cconfig.Config) -> mdata.MarketData:
    """
    Get historical market data to connect to data source node.
    """
    im_client = config.get_and_mark_as_used(("client_config", "client"))
    asset_ids = config.get_and_mark_as_used(
        ("client_config", "universe", "asset_ids")
    )
    columns = None
    columns_remap = None
    wall_clock_time = pd.Timestamp("2100-01-01T00:00:00+00:00")
    market_data = mdata.get_HistoricalImClientMarketData_example1(
        im_client,
        asset_ids,
        columns,
        columns_remap,
        wall_clock_time=wall_clock_time,
    )
    return market_data
