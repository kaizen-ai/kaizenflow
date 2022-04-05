"""
Import as:

import market_data.market_data_example as mdmadaex
"""

import asyncio
import logging
from typing import Dict, List, Optional, Tuple

import pandas as pd

import core.finance as cofinanc
import core.real_time as creatime
import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import market_data.market_data_im_client as mdmdimcl
import market_data.replayed_market_data as mdremada

_LOG = logging.getLogger(__name__)


# #############################################################################
# ReplayedTimeMarketData examples
# #############################################################################


# TODO(gp): Return only MarketData since the wall clock is inside it.
def get_ReplayedTimeMarketData_from_df(
    event_loop: asyncio.AbstractEventLoop,
    initial_replayed_delay: int,
    df: pd.DataFrame,
    *,
    knowledge_datetime_col_name: str = "timestamp_db",
    asset_id_col_name: str = "asset_id",
    start_time_col_name: str = "start_datetime",
    end_time_col_name: str = "end_datetime",
    delay_in_secs: int = 0,
    sleep_in_secs: float = 1.0,
    time_out_in_secs: int = 60 * 2,
) -> Tuple[mdremada.ReplayedMarketData, hdateti.GetWallClockTime]:
    """
    Build a `ReplayedMarketData` backed by data stored in a dataframe.

    :param df: dataframe including the columns
        ["timestamp_db", "asset_id", "start_datetime", "end_datetime"]
    :param initial_replayed_delay: how many minutes after the beginning of the
        data the replayed time starts. This is useful to simulate the beginning
        / end of the trading day.
    """
    hdbg.dassert_in(knowledge_datetime_col_name, df.columns)
    hdbg.dassert_in(asset_id_col_name, df.columns)
    # Infer the asset ids from the dataframe.
    asset_ids = list(df[asset_id_col_name].unique())
    hdbg.dassert_in(start_time_col_name, df.columns)
    hdbg.dassert_in(end_time_col_name, df.columns)
    columns = None
    # Build the wall clock.
    tz = "ET"
    # Find the initial timestamp of the data and shift by
    # `initial_replayed_delay`.
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
    df = cofinanc.generate_random_price_data(
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
    df = cofinanc.generate_random_price_data(
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
    Build a `ReplayedMarketData` with synthetic bar data.
    """
    # Generate random price data.
    df = cofinanc.generate_random_bars(start_datetime, end_datetime, asset_ids)
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


def get_ReplayedTimeMarketData_example5(
    event_loop: asyncio.AbstractEventLoop,
    start_datetime: pd.Timestamp,
    end_datetime: pd.Timestamp,
    asset_ids: List[int],
    *,
    initial_replayed_delay: int = 0,
) -> Tuple[mdremada.ReplayedMarketData, hdateti.GetWallClockTime]:
    """
    Build a `ReplayedMarketData` with synthetic top-of-the-book data.
    """
    # Generate random price data.
    df = cofinanc.generate_random_top_of_book_bars(
        start_datetime, end_datetime, asset_ids
    )
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
# ImClientMarketData examples
# #############################################################################


def get_ImClientMarketData_example1(
    asset_ids: List[int],
    columns: List[str],
    column_remap: Optional[Dict[str, str]],
) -> mdmdimcl.ImClientMarketData:
    """
    Build a `ImClientMarketData` backed with loaded test data.
    """
    import im_v2.ccxt.data.client.ccxt_clients_example as imvcdcccex

    resample_1min = True
    ccxt_client = imvcdcccex.get_CcxtCsvClient_example1(resample_1min)
    #
    asset_id_col = "asset_id"
    start_time_col_name = "start_ts"
    end_time_col_name = "end_ts"
    get_wall_clock_time = get_ImClientMarketData_wall_clock_time1
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


def get_ImClientMarketData_example2(
    asset_ids: List[int],
    columns: List[str],
    column_remap: Optional[Dict[str, str]],
) -> mdmdimcl.ImClientMarketData:
    """
    Build a `ImClientMarketData` using `DataFrameImClient`.
    """
    import im_v2.common.data.client.data_frame_im_clients_example as imvcdcdfimce

    data_frame_client = imvcdcdfimce.get_DataFrameImClient_example1()
    #
    asset_id_col = "asset_id"
    start_time_col_name = "start_ts"
    end_time_col_name = "end_ts"
    get_wall_clock_time = get_ImClientMarketData_wall_clock_time2
    market_data_client = mdmdimcl.ImClientMarketData(
        asset_id_col,
        asset_ids,
        start_time_col_name,
        end_time_col_name,
        columns,
        get_wall_clock_time,
        im_client=data_frame_client,
        column_remap=column_remap,
    )
    return market_data_client


# TODO(gp): We can also use a real wall clock.
def get_ImClientMarketData_wall_clock_time1() -> pd.Timestamp:
    """
    Get a wall clock time to build `ImClientMarketData` for tests.
    """
    return pd.Timestamp("2018-08-17T01:30:00+00:00")


def get_ImClientMarketData_wall_clock_time2() -> pd.Timestamp:
    """
    Get a wall clock time to build `ImClientMarketData` using
    `DataFrameImClient`.
    """
    return pd.Timestamp("2000-01-01T10:10:00-00:00")
