"""
Import as:

import market_data.market_data_example as mdmadaex
"""

import asyncio
import datetime
import logging
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

import core.artificial_signal_generators as carsigen
import core.real_time as creatime
import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hnumpy as hnumpy
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import im_v2.ccxt.data.client.test.ccxt_clients_example as ivcdctcce
import market_data.market_data_im_client as mdmdimcl
import market_data.replayed_market_data as mdremada

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
    time_delta = pd.Timedelta(freq)
    for asset_id in asset_ids:
        df = pd.DataFrame()
        df["start_datetime"] = start_dates
        df["end_datetime"] = start_dates + time_delta
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


def generate_random_bars(
    start_datetime: pd.Timestamp,
    end_datetime: pd.Timestamp,
    asset_ids: List[int],
    *,
    bar_duration: str = "1T",
    bar_volatility_in_bps: int = 10,
    bar_expected_count: int = 1000,
    last_price: float = 1000,
    start_time: datetime.time = datetime.time(9, 31),
    end_time: datetime.time = datetime.time(16, 00),
    seed: int = 10,
) -> pd.DataFrame:
    """
    Wraps `generate_random_bars_for_asset()` for multiple instruments.

    :return: dataframe as in `generate_random_bars_for_asset()`, concatenated
        along the index, sorted by timestamp then by asset it
    """
    asset_dfs = []
    for asset_id in asset_ids:
        df = generate_random_bars_for_asset(
            start_datetime,
            end_datetime,
            asset_id,
            bar_duration=bar_duration,
            bar_volatility_in_bps=bar_volatility_in_bps,
            bar_expected_count=bar_expected_count,
            last_price=last_price,
            start_time=start_time,
            end_time=end_time,
            seed=seed,
        )
        asset_dfs.append(df)
        seed += 1
    df = pd.concat(asset_dfs, axis=0).sort_values(["end_datetime", "asset_id"])
    return df


def generate_random_bars_for_asset(
    start_datetime: pd.Timestamp,
    end_datetime: pd.Timestamp,
    asset_id: int,
    *,
    bar_duration: str = "1T",
    bar_volatility_in_bps: int = 10,
    bar_expected_count: int = 1000,
    last_price: float = 1000,
    start_time: datetime.time = datetime.time(9, 31),
    end_time: datetime.time = datetime.time(16, 00),
    seed: int = 10,
) -> pd.DataFrame:
    """
    Return a dataframe of random bars for a single instrument.

    :param start_datetime: initial timestamp
    :param end_datetime: final timestamp
    :param asset_id: asset id for labeling
    :param bar_duration: length of bar in time
    :param bar_volatility_in_bps: expected bar volatility
    :param bar_expected_count: expected volume per bar
    :param last_price: "last price" before start of series
    :param start_time: e.g., start of active trading hours
    :param end_time: e.g., end of active trading hours
    :param seed: seed for numpy `Generator`
    :return: dataframe like
      - index is an integer index
      - columns include timestamps, asset id, price, volume, and fake features
    """
    price_process = carsigen.PriceProcess(seed)
    close = price_process.generate_price_series_from_normal_log_returns(
        start_datetime,
        end_datetime,
        asset_id,
        bar_duration=bar_duration,
        bar_volatility_in_bps=bar_volatility_in_bps,
        last_price=last_price,
        start_time=start_time,
        end_time=end_time,
    ).rename("close")
    volume = price_process.generate_volume_series_from_poisson_process(
        start_datetime,
        end_datetime,
        asset_id,
        bar_duration=bar_duration,
        bar_expected_count=bar_expected_count,
        start_time=start_time,
        end_time=end_time,
    ).rename("volume")
    f1 = price_process.generate_volume_series_from_poisson_process(
        start_datetime,
        end_datetime,
        asset_id,
        bar_duration=bar_duration,
        bar_expected_count=2 * bar_expected_count,
        start_time=start_time,
        end_time=end_time,
    ).rename("f1")
    f2 = price_process.generate_volume_series_from_poisson_process(
        start_datetime,
        end_datetime,
        asset_id,
        bar_duration=bar_duration,
        bar_expected_count=2 * bar_expected_count,
        start_time=start_time,
        end_time=end_time,
    ).rename("f2")
    s1 = 0.01 * price_process.generate_volume_series_from_poisson_process(
        start_datetime,
        end_datetime,
        asset_id,
        bar_duration=bar_duration,
        bar_expected_count=2,
        start_time=start_time,
        end_time=end_time,
    )
    s2 = price_process.generate_volume_series_from_poisson_process(
        start_datetime,
        end_datetime,
        asset_id,
        bar_duration=bar_duration,
        bar_expected_count=0.1 * bar_expected_count,
        start_time=start_time,
        end_time=end_time,
    ).rename("s2")
    s1 = (s1 * s2).groupby(lambda x: x.date).cumsum().rename("s1")
    # TODO(Paul): Expose the bar delay.
    bar_delay = "10s"
    df = build_timestamp_df(
        close.index,
        bar_duration,
        bar_delay,
    )
    df = pd.concat(
        [df, close, volume, f1, f2, s1, s2],
        axis=1,
    )
    df["asset_id"] = asset_id
    return df.reset_index(drop=True)


def build_timestamp_df(
    index: pd.DatetimeIndex,
    bar_duration: str,
    bar_delay: str,
) -> pd.DataFrame:
    hdbg.dassert_isinstance(index, pd.DatetimeIndex)
    bar_time_delta = pd.Timedelta(bar_duration)
    start_datetime = pd.Series(
        data=index - bar_time_delta, index=index, name="start_datetime"
    )
    end_datetime = pd.Series(data=index, index=index, name="end_datetime")
    bar_time_delay = pd.Timedelta(bar_delay)
    timestamp = pd.Series(
        data=index + bar_time_delay, index=index, name="timestamp_db"
    )
    df = pd.concat(
        [start_datetime, end_datetime, timestamp],
        axis=1,
    )
    return df


# #############################################################################


# TODO(gp): Return only MarketData since the wall clock is inside it.
def get_ReplayedTimeMarketData_from_df(
    event_loop: asyncio.AbstractEventLoop,
    initial_replayed_delay: int,
    df: pd.DataFrame,
    *,
    delay_in_secs: int = 0,
    sleep_in_secs: float = 1.0,
    time_out_in_secs: int = 60 * 2,
) -> Tuple[mdremada.ReplayedMarketData, hdateti.GetWallClockTime]:
    """
    Build a `ReplayedMarketData` backed by synthetic data stored in a
    dataframe.

    :param df: dataframe including the columns
        ["timestamp_db", "asset_id", "start_datetime", "end_datetime"]
    :param initial_replayed_delay: how many minutes after the beginning of the data
        the replayed time starts. This is useful to simulate the beginning / end of
        the trading day
    """
    # Build the `ReplayedMarketData` backed by the df with
    # `initial_replayed_delay` after the first timestamp of the data.
    knowledge_datetime_col_name = "timestamp_db"
    hdbg.dassert_in(knowledge_datetime_col_name, df.columns)
    asset_id_col_name = "asset_id"
    hdbg.dassert_in(asset_id_col_name, df.columns)
    # If the asset ids were not specified, then infer it from the dataframe.
    asset_ids = df[asset_id_col_name].unique()
    start_time_col_name = "start_datetime"
    hdbg.dassert_in(start_time_col_name, df.columns)
    end_time_col_name = "end_datetime"
    hdbg.dassert_in(end_time_col_name, df.columns)
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
    # Build a `ReplayedMarketData`.
    market_data = mdremada.ReplayedMarketData(
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
    return market_data, get_wall_clock_time


# TODO(gp): initial_replayed_delay -> initial_delay_in_mins (or in secs).
def get_ReplayedTimeMarketData_example2(
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
) -> Tuple[mdremada.ReplayedMarketData, hdateti.GetWallClockTime]:
    """
    Build a `ReplayedMarketData` backed by synthetic data.

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
    (market_data, get_wall_clock_time,) = get_ReplayedTimeMarketData_from_df(
        event_loop,
        initial_replayed_delay,
        df,
        delay_in_secs=delay_in_secs,
        sleep_in_secs=sleep_in_secs,
        time_out_in_secs=time_out_in_secs,
    )
    return market_data, get_wall_clock_time


def get_ReplayedTimeMarketData_example3(
    event_loop: asyncio.AbstractEventLoop,
) -> Tuple[mdremada.ReplayedMarketData, hdateti.GetWallClockTime]:
    """
    Build a `ReplayedMarketData`:

    - with synthetic price data between `2000-01-01 9:30` and `10:30`
    - for two assets 101 and 202
    - starting 5 minutes after the data
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
    _LOG.debug("df=%s", hpandas.df_to_str(df))
    # Build a `ReplayedMarketData`.
    initial_replayed_delay = 5
    delay_in_secs = 0
    sleep_in_secs = 30
    time_out_in_secs = 60 * 5
    (market_data, get_wall_clock_time,) = get_ReplayedTimeMarketData_from_df(
        event_loop,
        initial_replayed_delay,
        df=df,
        delay_in_secs=delay_in_secs,
        sleep_in_secs=sleep_in_secs,
        time_out_in_secs=time_out_in_secs,
    )
    return market_data, get_wall_clock_time


def get_ReplayedTimeMarketData_example4(
    event_loop: asyncio.AbstractEventLoop,
    start_datetime: pd.Timestamp,
    end_datetime: pd.Timestamp,
    asset_ids: List[int],
    *,
    initial_replayed_delay: int = 0,
) -> Tuple[mdremada.ReplayedMarketData, hdateti.GetWallClockTime]:
    """
    Build a `ReplayedMarketData` with synthetic bar data for the given interval
    of time and assets.
    """
    # Generate random price data.
    df = generate_random_bars(start_datetime, end_datetime, asset_ids)
    _LOG.debug("df=%s", hpandas.df_to_str(df))
    # Build a `ReplayedMarketData`.
    delay_in_secs = 0
    sleep_in_secs = 30
    time_out_in_secs = 60 * 5
    market_data, get_wall_clock_time = get_ReplayedTimeMarketData_from_df(
        event_loop,
        initial_replayed_delay,
        df,
        delay_in_secs=delay_in_secs,
        sleep_in_secs=sleep_in_secs,
        time_out_in_secs=time_out_in_secs,
    )
    return market_data, get_wall_clock_time


# #############################################################################


def get_ImClientMarketData_example1(
    asset_ids: List[int],
    columns: List[str],
    column_remap: Optional[Dict[str, str]],
) -> mdmdimcl.ImClientMarketData:
    """
    Build a `ImClientMarketData` backed with loaded test data.
    """
    ccxt_client = ivcdctcce.get_CcxtCsvClient_example1()
    #
    asset_id_col = "asset_id"
    start_time_col_name = "start_ts"
    end_time_col_name = "end_ts"
    get_wall_clock_time = get_ImClientMarketData_wall_clock_time
    market_data_client = mdmdimcl.ImClientMarketData(
        asset_id_col,
        asset_ids,
        start_time_col_name,
        end_time_col_name,
        columns,
        get_wall_clock_time,
        im_client=ccxt_client,
        column_remap=column_remap,
    )
    return market_data_client


# TODO(gp): We can also use a real wall clock.
def get_ImClientMarketData_wall_clock_time() -> pd.Timestamp:
    """
    Get a wall clock time to build `ImClientMarketData` for tests.
    """
    return pd.Timestamp("2018-08-17T01:30:00+00:00")
