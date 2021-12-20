"""
Import as:

import market_data.market_data_interface_example as mdmdinex
"""

import asyncio
import logging
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

import core.real_time as creatime
import helpers.datetime_ as hdateti
import helpers.dbg as hdbg
import helpers.hnumpy as hnumpy
import helpers.printing as hprint
import market_data.market_data_interface as mdmadain

_LOG = logging.getLogger(__name__)


def generate_random_price_data(
    start_datetime: pd.Timestamp,
    end_datetime: pd.Timestamp,
    columns: List[str],
    asset_ids: List[int],
    *,
    freq: str = "1T",
    initial_price: float = 1000,
    seed: int = 42,
) -> pd.DataFrame:
    """
    Generate synthetic data used to mimic real-time price data.

    The data:
        - is a random walk with a bias of 1000 and increments ~ iid U[-0.5, 0.5]
        - looks like:
        ```
        TODO(gp):
        ```
    """
    _LOG.debug(
        hprint.to_str("start_datetime end_datetime columns asset_ids freq seed")
    )
    hdateti.dassert_tz_compatible(start_datetime, end_datetime)
    hdbg.dassert_lte(start_datetime, end_datetime)
    hdbg.dassert_isinstance(asset_ids, list)
    #
    start_dates = pd.date_range(start_datetime, end_datetime, freq=freq)
    dfs = []
    for asset_id in asset_ids:
        df = pd.DataFrame()
        df["start_datetime"] = start_dates
        df["end_datetime"] = start_dates + pd.Timedelta(minutes=1)
        # TODO(gp): We can add 1 sec here to make it more interesting.
        df["timestamp_db"] = df["end_datetime"]
        # TODO(gp): Filter by ATH, if needed.
        # Random walk with increments independent and uniform in [-0.5, 0.5].
        for column in columns:
            with hnumpy.random_seed_context(seed):
                data = np.random.rand(len(start_dates), 1) - 0.5  # type: ignore[var-annotated]
            df[column] = initial_price + data.cumsum()
        df["asset_id"] = asset_id
        dfs.append(df)
    df = pd.concat(dfs, axis=0)
    return df


def get_replayed_time_market_data_interface_example1(
    event_loop: asyncio.AbstractEventLoop,
    initial_replayed_delay: int,
    df: pd.DataFrame,
    *,
    delay_in_secs: int = 0,
    sleep_in_secs: float = 1.0,
    time_out_in_secs: int = 60 * 2,
) -> Tuple[mdmadain.ReplayedTimeMarketDataInterface, hdateti.GetWallClockTime]:
    """
    Build a `ReplayedTimeMarketDataInterface` backed by synthetic data.

    :param start_datetime: start time for the generation of the synthetic data
    :param end_datetime: end time for the generation of the synthetic data
    :param initial_replayed_delay: how many minutes after the beginning of the data
        the replayed time starts. This is useful to simulate the beginning / end of
        the trading day
    :param asset_ids: asset ids to generate data for. `None` defaults to all the
        available asset ids in the data frame
    """
    # Build the `ReplayedTimeMarketDataInterface` backed by the df with
    # `initial_replayed_delay` after the first timestamp of the data.
    knowledge_datetime_col_name = "timestamp_db"
    asset_id_col_name = "asset_id"
    # If the asset ids were not specified, then infer it from the dataframe.
    asset_ids = df[asset_id_col_name].unique()
    start_time_col_name = "start_datetime"
    end_time_col_name = "end_datetime"
    columns = None
    # Build the wall clock.
    tz = "ET"
    initial_replayed_dt = df[start_time_col_name].min() + pd.Timedelta(
        minutes=initial_replayed_delay
    )
    speed_up_factor = 1.0
    get_wall_clock_time = creatime.get_replayed_wall_clock_time(
        tz,
        initial_replayed_dt,
        event_loop=event_loop,
        speed_up_factor=speed_up_factor,
    )
    # Build a `ReplayedTimeMarketDataInterface`.
    market_data_interface = mdmadain.ReplayedTimeMarketDataInterface(
        df,
        knowledge_datetime_col_name,
        delay_in_secs,
        #
        asset_id_col_name,
        asset_ids,
        start_time_col_name,
        end_time_col_name,
        columns,
        get_wall_clock_time,
        sleep_in_secs=sleep_in_secs,
        time_out_in_secs=time_out_in_secs,
    )
    return market_data_interface, get_wall_clock_time


# TODO(gp): initial_replayed_delay -> initial_delay_in_mins (or in secs).
def get_replayed_time_market_data_interface_example2(
    event_loop: asyncio.AbstractEventLoop,
    start_datetime: pd.Timestamp,
    end_datetime: pd.Timestamp,
    initial_replayed_delay: int,
    asset_ids: List[int],
    *,
    delay_in_secs: int = 0,
    columns: Optional[List[str]] = None,
    sleep_in_secs: float = 1.0,
    time_out_in_secs: int = 60 * 2,
) -> Tuple[mdmadain.ReplayedTimeMarketDataInterface, hdateti.GetWallClockTime]:
    """
    Build a `ReplayedTimeMarketDataInterface` backed by synthetic data.

    :param start_datetime: start time for the generation of the synthetic data
    :param end_datetime: end time for the generation of the synthetic data
    :param initial_replayed_delay: how many minutes after the beginning of the data
        the replayed time starts. This is useful to simulate the beginning / end of
        the trading day
    :param asset_ids: asset ids to generate data for. `None` defaults to all the
        available asset ids in the data frame
    """
    # Build the df with the data.
    if columns is None:
        columns = ["last_price"]
    hdbg.dassert_is_not(asset_ids, None)
    df = generate_random_price_data(
        start_datetime, end_datetime, columns, asset_ids
    )
    market_data_interface, get_wall_clock_time = (
        get_replayed_time_market_data_interface_example1(
            event_loop,
            initial_replayed_delay,
            df,
            delay_in_secs=delay_in_secs,
            sleep_in_secs=sleep_in_secs,
            time_out_in_secs=time_out_in_secs
    ))
    return market_data_interface, get_wall_clock_time


def get_replayed_time_market_data_interface_example3(
    event_loop: asyncio.AbstractEventLoop,
) -> Tuple[mdmadain.ReplayedTimeMarketDataInterface, hdateti.GetWallClockTime]:
    """
    Build a ReplayedTimeMarketDataInterface:

    - with synthetic data between `2000-01-01 9:30` and `10:30`
    - for two assets
    """
    # Generate random price data.
    start_datetime = pd.Timestamp(
        "2000-01-01 09:30:00-05:00", tz="America/New_York"
    )
    end_datetime = pd.Timestamp(
        "2000-01-01 10:30:00-05:00", tz="America/New_York"
    )
    columns_ = ["price"]
    asset_ids = [101, 202]
    df = generate_random_price_data(
        start_datetime, end_datetime, columns_, asset_ids
    )
    _LOG.debug("df=%s", hprint.dataframe_to_str(df))
    # Build a `ReplayedTimeMarketDataInterface`.
    initial_replayed_delay = 5
    delay_in_secs = 0
    sleep_in_secs = 30
    time_out_in_secs = 60 * 5
    (
        market_data_interface,
        get_wall_clock_time,
    ) = get_replayed_time_market_data_interface_example1(
        event_loop,
        initial_replayed_delay,
        df=df,
        delay_in_secs=delay_in_secs,
        sleep_in_secs=sleep_in_secs,
        time_out_in_secs=time_out_in_secs,
    )
    return market_data_interface, get_wall_clock_time
