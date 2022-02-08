"""
Import as:

import dataflow.model.experiment_config as dtfmoexcon
"""
import datetime
import logging
from typing import Any, Callable, Iterable, List, Optional, Tuple

import pandas as pd

import core.config as cconfig
import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hprint as hprint

_LOG = logging.getLogger(__name__)


# #############################################################################
# Asset universe.
# #############################################################################


def parse_universe_str(universe_str: str) -> Tuple[str, Optional[int]]:
    """
    Parse a string representing an universe

    E.g., "kibot_v1_0-top100", "kibot_v2_0-all".
    """
    data = universe_str.split("-")
    hdbg.dassert_eq(len(data), 2)
    universe_version, top_n = data
    if top_n == "all":
        top_n = None
    else:
        prefix = "top"
        hdbg.dassert(top_n.startswith(prefix), "Invalid top_n='%s'", top_n)
        top_n = int(top_n[len(prefix) :])
    return universe_version, top_n


def get_universe_top_n(universe: List[Any], n: Optional[int]) -> List[Any]:
    if n is None:
        # No filtering.
        pass
    else:
        hdbg.dassert_lte(1, n, "Invalid n='%s'", n)
        hdbg.dassert_lte(n, len(universe))
        universe = universe[:n]
    universe = sorted(universe)
    return universe


# #############################################################################
# Period.
# #############################################################################


# TODO(gp): -> get_time_interval
def get_period(period: str) -> Tuple[datetime.datetime, datetime.datetime]:
    if period == "2days":
        start_datetime = datetime.datetime(2020, 1, 6)
        end_datetime = datetime.datetime(2020, 1, 7)
    elif period == "Jan2020":
        # Jan in 2020.
        start_datetime = datetime.datetime(2020, 1, 1)
        end_datetime = datetime.datetime(2020, 2, 1)
    elif period == "2018":
        # 2018.
        start_datetime = datetime.datetime(2018, 1, 1)
        end_datetime = datetime.datetime(2019, 1, 1)
    elif period == "2009_2019":
        # Entire 2009-2019 period.
        start_datetime = datetime.datetime(2009, 1, 1)
        end_datetime = datetime.datetime(2019, 1, 1)
    elif period == "2015_2022":
        start_datetime = datetime.datetime(2015, 1, 1)
        end_datetime = datetime.datetime(2022, 1, 1)
    elif period == "2012_2022":
        start_datetime = datetime.datetime(2012, 1, 1)
        end_datetime = datetime.datetime(2022, 1, 1)
    else:
        hdbg.dfatal("Invalid period='%s'" % period)
    _LOG.info("start_datetime=%s end_datetime=%s", start_datetime, end_datetime)
    hdbg.dassert_lte(start_datetime, end_datetime)
    return start_datetime, end_datetime


# #############################################################################
# Experiment config.
# #############################################################################

# TODO(gp): bm_config -> experiment_config
# TODO(gp): build_model_config -> build_experiment_config


def parse_experiment_config(experiment_config: str) -> Tuple[str, str, str]:
    """
    Parse a string representing an experiment in the format:
    `<universe>.<date_period>.<time_interval>`, e.g., "top100.15T.all".

    Each token can be composed of multiple chunks separated by `-`. E.g.,
    `universe_str = "eg_v1_0-top100"`

    :return: universe_str, date_period_str, time_interval_str
    """
    _LOG.info(hprint.to_str("experiment_config"))
    #
    data = experiment_config.split(".")
    hdbg.dassert_eq(len(data), 3)
    universe_str, date_period_str, time_interval_str = data
    #
    _LOG.info(hprint.to_str("universe_str date_period_str time_interval_str"))
    return universe_str, date_period_str, time_interval_str


# #############################################################################
# Experiment config processing.
# #############################################################################


def set_asset_id(
    config: cconfig.Config,
    asset_id_key: cconfig.Config.Key,
    asset_id: int,
    *,
    allow_new_key: bool = True,
    assume_dummy: bool = True,
) -> None:
    """
    Assign an `asset_id` to a config.

    :param asset_id_key: the key to assign
        - E.g., `("DAG", "rets/read_data", "asset_id")`
    :param assume_dummy: assume that the value in the config corresponding to
        `asset_id_key` is `DUMMY`, i.e., it's coming from a template. This is
        used to enforce a use patter like "create a template config and then
        overwrite DUMMY values only once".
    """
    hdbg.dassert_isinstance(config, cconfig.Config)
    config = config.copy()
    _LOG.debug("Creating config for egid=`%s`", asset_id)
    if not allow_new_key:
        hdbg.dassert_in(asset_id_key, config)
        if assume_dummy:
            hdbg.dassert_eq(config.get(asset_id_key), cconfig.DUMMY)
    config[asset_id_key] = asset_id


def build_configs_varying_asset_id(
    config: cconfig.Config,
    asset_id_key: cconfig.Config.Key,
    asset_ids: List[int],
) -> List[cconfig.Config]:
    """
    Create a list of `Config`s based on `config` using different `asset_ids`.
    """
    hdbg.dassert_isinstance(config, cconfig.Config)
    _LOG.debug("Universe has %d asset_ids", len(asset_ids))
    configs = []
    for asset_id in asset_ids:
        config_tmp = config.copy()
        set_asset_id(config_tmp, asset_id_key, asset_id)
        _LOG.debug("config_tmp=%s\n", config_tmp)
        #
        configs.append(config_tmp)
    return configs


def build_configs_varying_universe_tiles(
    config: cconfig.Config,
    universe_tile_id: cconfig.Config.Key,
    universe_tiles: Iterable[List[int]],
) -> List[cconfig.Config]:
    """
    Create a list of `Config`s based on `config` using different `asset_ids`.
    """
    hdbg.dassert_isinstance(config, cconfig.Config)
    _LOG.debug("Universe has %d tiles", len(universe_tiles))
    configs = []
    for universe_tile in universe_tiles:
        config_tmp = config.copy()
        set_asset_id(config_tmp, universe_tile_id, universe_tile)
        _LOG.debug("config_tmp=%s\n", config_tmp)
        #
        configs.append(config_tmp)
    return configs


def build_configs_with_tiled_periods(
    config: cconfig.Config,
    start_timestamp: hdateti.Datetime,
    end_timestamp: hdateti.Datetime,
    freq_as_pd_str: str,
    lookback_as_pd_str: str,
) -> List[cconfig.Config]:
    """
    Create a list of `Config`s based on `config` using different a partition
    of the interval of time [`start_timestamp`, `end_timestamp`] using intervals like
    `(a, b]`

    :param start_timestamp, end_timestamp: the interval of time to partition
    :param freq_as_pd_str: the frequency of partitioning (e.g., `M`, `W`)
    :param lookback_as_pd_str: the extra period of time (e.g., `10D`) before the
        start of the interval, needed to warm up the period (e.g., compute
        features)
    """
    hdbg.dassert_isinstance(config, cconfig.Config)
    start_timestamp = hdateti.to_timestamp(start_timestamp)
    end_timestamp = hdateti.to_timestamp(end_timestamp)
    hdbg.dassert_lte(start_timestamp, end_timestamp)
    # TODO(gp): Check that the lookback is > 0.
    lookback = pd.Timedelta(lookback_as_pd_str)
    #
    configs = []
    dates = pd.date_range(
        start_timestamp.date(), end_timestamp.date(), freq=freq_as_pd_str
    )
    hdbg.dassert_lte(1, len(dates))
    date_tuples = zip(dates[:-1], dates[1:])
    for start_ts, end_ts in date_tuples:
        # Add one day to make the interval `(a, b]`.
        start_ts += pd.Timedelta("1D")
        #
        config_tmp = config.copy()
        config_tmp[("meta", "start_timestamp_with_lookback")] = (
            start_ts - lookback
        )
        config_tmp[("meta", "start_timestamp")] = start_ts
        config_tmp[("meta", "end_timestamp")] = end_ts
        #
        _LOG.debug("config_tmp=%s\n", config_tmp)
        #
        configs.append(config_tmp)
    return configs


# TODO(gp): This is probably equivalent to some iterchain.reduce() standard function.
def apply_build_configs(
    func: Callable, configs_in: List[cconfig.Config]
) -> List[cconfig.Config]:
    """
    Apply a `build_configs_*()` to each Config in `configs` and return the
    accumulated list of all the configs.
    """
    configs_out = []
    for config in configs_in:
        configs_tmp = func(config)
        hdbg.dassert_container_type(configs_tmp, list, cconfig.Config)
        configs_out.extend(configs_tmp)
    return configs_out
