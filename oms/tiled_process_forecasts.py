"""
Import as:

import oms.tiled_process_forecasts as otiprfor
"""

import datetime
import logging

import pandas as pd
from tqdm.autonotebook import tqdm

import core.config as cconfig
import helpers.hpandas as hpandas
import helpers.hparquet as hparque
import market_data as mdata
import oms.portfolio as omportfo
import oms.portfolio_example as oporexam
import oms.process_forecasts as oprofore

_LOG = logging.getLogger(__name__)


# TODO(Paul): Move this or make it an example.
def get_portfolio(market_data: mdata.MarketData) -> omportfo.AbstractPortfolio:
    strategy_id = "strategy"
    account = "account"
    timestamp_col = "end_datetime"
    mark_to_market_col = "close"
    pricing_method = "twap.5T"
    initial_holdings = pd.Series([0], [-1])
    column_remap = {
        "bid": "bid",
        "ask": "ask",
        "price": "close",
        "midpoint": "midpoint",
    }
    portfolio = oporexam.get_DataFramePortfolio_example2(
        strategy_id,
        account,
        market_data,
        timestamp_col,
        mark_to_market_col,
        pricing_method,
        initial_holdings,
        column_remap=column_remap,
    )
    return portfolio


# TODO(Paul): Move this and make it an example.
def get_process_forecasts_config() -> cconfig.Config:
    dict_ = {
        "order_config": {
            "order_type": "price@twap",
            "order_duration": 5,
        },
        "optimizer_config": {
            "backend": "batch_optimizer",
            "dollar_neutrality_penalty": 0.1,
            "volatility_penalty": 0.5,
            "turnover_penalty": 0.0,
            "target_gmv": 1e5,
            "target_gmv_upper_bound_multiple": 1.0,
        },
        "execution_mode": "batch",
        "ath_start_time": datetime.time(9, 30),
        "trading_start_time": datetime.time(9, 35),
        "ath_end_time": datetime.time(16, 0),
        "trading_end_time": datetime.time(15, 55),
    }
    config = cconfig.get_config_from_nested_dict(dict_)
    return config


async def run_tiled_process_forecasts(
    file_name: str,
    start_date: datetime.date,
    end_date: datetime.date,
    asset_id_col: str,
    returns_col: str,
    prediction_col: str,
    volatility_col: str,
    portfolio: omportfo.AbstractPortfolio,
    process_forecasts_config: cconfig.Config,
) -> None:
    columns = [asset_id_col, returns_col, volatility_col, prediction_col]
    tiles = hparque.yield_parquet_tiles_by_year(
        file_name,
        start_date,
        end_date,
        columns,
    )
    num_years = end_date.year - start_date.year + 1
    for tile in tqdm(tiles, total=num_years):
        # Parquet reads asset_ids as categoricals; convert to ints.
        tile = hpandas.convert_col_to_int(tile, asset_id_col)
        # Convert any dataframe columns to ints if possible.
        tile = tile.rename(columns=hparque.maybe_cast_to_int)
        # Extract the prediction and volatility data as dataframes with columns
        # equal to asset ids.
        prediction_df = tile[[prediction_col, asset_id_col]].pivot(
            columns=asset_id_col,
            values=prediction_col,
        )
        volatility_df = tile[[volatility_col, asset_id_col]].pivot(
            columns=asset_id_col,
            values=volatility_col,
        )
        await oprofore.process_forecasts(
            prediction_df,
            volatility_df,
            portfolio,
            process_forecasts_config,
        )
    return portfolio
