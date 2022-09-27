# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.8
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
# %load_ext autoreload
# %autoreload 2
# %matplotlib inline

# %% [markdown]
# # Imports

# %%
import itertools
import logging
import os
from typing import List, Tuple

import pandas as pd

import core.config as cconfig
import core.plotting as coplotti
import dataflow.model as dtfmod
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import helpers.hwall_clock_time as hwacltim
import market_data as mdata
import oms as oms

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # System configs

# %%
prod_system_config_output = load_config_as_list(
    prod_dir + "/system_config.output.txt"
)
sim_system_config_output = load_config_as_list(
    sim_dir + "/system_config.output.txt"
)
prod_system_config_input = load_config_as_list(
    prod_dir + "/system_config.input.txt"
)
sim_system_config_input = load_config_as_list(
    sim_dir + "/system_config.input.txt"
)

# %%
prod_output_only, sim_output_only = diff_lines(
    prod_system_config_output, sim_system_config_output
)

# %%
# prod_output_only

# %%
# sim_output_only

# %% [markdown]
# # DAG compare

# %%
prod_dag_dir = os.path.join(prod_dir, "dag/node_io/node_io.data")
hdbg.dassert_dir_exists(prod_dag_dir)
print(prod_dag_dir)
sim_dag_dir = os.path.join(sim_dir, "dag/node_io/node_io.data")
hdbg.dassert_dir_exists(sim_dag_dir)
print(sim_dag_dir)

# %%
# Read CSV which is multi-index
# stage = "0.read_data"
# stage = "2.zscore"
# target_cols = ['ask', 'bid', 'close', 'day_num_spread', 'day_spread', 'high', 'low', 'notional', 'open', 'sided_ask_count', 'sided_bid_count', 'start_time', 'volume']

stage = "7.process_forecasts"
target_cols = [
    "close",
    "close_vwap",
    "day_num_spread",
    "day_spread",
    "garman_klass_vol",
    "high",
    "low",
    "notional",
    "open",
    "prediction",
    "twap",
    "volume",
]
# timestamp = "20220915_154500"
timestamp = "20220915_100000"
current_timestamp = hwacltim.get_machine_wall_clock_time(as_str=True)

file_name = f"predict.{stage}.df_out.{timestamp}.{current_timestamp}.csv"
file_name = os.path.join(prod_dag_dir, file_name)
print("prod_file_name=", file_name)
prod_dag_df = pd.read_csv(file_name, parse_dates=True, index_col=0, header=[0, 1])

prod_dag_df = prod_dag_df[start_timestamp:end_timestamp]

file_name = f"predict.{stage}.df_out.{timestamp}.csv"
file_name = os.path.join(sim_dag_dir, file_name)
print("sim_file_name=", file_name)
sim_dag_df = pd.read_csv(file_name, parse_dates=True, index_col=0, header=[0, 1])
sim_dag_df = sim_dag_df[start_timestamp:end_timestamp]

asset_ids = prod_dag_df.columns.levels[1].tolist()

columns = list(itertools.product(target_cols, asset_ids))
prod_dag_df = prod_dag_df[pd.MultiIndex.from_tuples(columns)].copy()
hpandas.df_to_str(prod_dag_df, log_level=logging.INFO)
prod_dag_df.to_csv("prod_tmp.csv")
prod_dag_df = pd.read_csv("prod_tmp.csv", index_col=0, header=[0, 1])

#
# sim_dag_df = sim_dag_df.drop(labels="end_time.1 timestamp_db index".split(), axis=1, level=0)
sim_dag_df = sim_dag_df[pd.MultiIndex.from_tuples(columns)].copy()
hpandas.df_to_str(sim_dag_df, log_level=logging.INFO)
#
sim_dag_df.to_csv("sim_tmp.csv")
sim_dag_df = pd.read_csv("sim_tmp.csv", index_col=0, header=[0, 1])
#
print(list(prod_dag_df.columns.levels[0]))
print(list(sim_dag_df.columns.levels[0]))


# %%
# Compare output
dag_corrs = dtfmod.compute_correlations(prod_dag_df, sim_dag_df.shift(0))
# hpandas.df_to_str(dag_corrs, precision=3, log_level=logging.INFO)

# sort_col = "close"
sort_col = "prediction"
# sort_col = "price"
# sort_col = "volatility"
hpandas.df_to_str(
    dag_corrs.sort_values(sort_col, ascending=False),
    num_rows=None,
    precision=3,
    log_level=logging.INFO,
)


# %% [markdown]
# # Functions

# %%
def get_replayed_delay_in_mins(
    min_timestamp_from_file: pd.Timestamp,
    min_timestamp_from_prod: pd.Timestamp,
) -> int:
    """
    Compute replayed delay in minutes from minimal time in market data from
    file and prod system start time.
    """
    time_diff_in_secs = (
        min_timestamp_from_prod - min_timestamp_from_file
    ).total_seconds()
    replayed_delay_in_mins = int(time_diff_in_secs / 60)
    return replayed_delay_in_mins


def load_portfolio(
    portfolio_dir, start_timestamp, end_timestamp, freq
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load portfolio and related stats.
    """
    # Make sure the directory exists.
    hdbg.dassert_dir_exists(portfolio_dir)
    # Sanity-check timestamps.
    hdbg.dassert_isinstance(start_timestamp, pd.Timestamp)
    hdbg.dassert_isinstance(end_timestamp, pd.Timestamp)
    hdbg.dassert_lt(start_timestamp, end_timestamp)
    # Load the portfolio and stats dataframes.
    portfolio_df, portfolio_stats_df = oms.Portfolio.read_state(
        portfolio_dir,
    )
    # Sanity-check the dataframes.
    hpandas.dassert_time_indexed_df(
        portfolio_df, allow_empty=False, strictly_increasing=True
    )
    hpandas.dassert_time_indexed_df(
        portfolio_stats_df, allow_empty=False, strictly_increasing=True
    )
    # Sanity-check the date ranges of the dataframes against the start and end timestamps.
    first_timestamp = portfolio_df.index[0]
    hdbg.dassert_lte(first_timestamp.round(freq), start_timestamp)
    last_timestamp = portfolio_df.index[-1]
    hdbg.dassert_lte(end_timestamp, last_timestamp.round(freq))
    #
    portfolio_df = portfolio_df.loc[start_timestamp:end_timestamp]
    portfolio_stats_df = portfolio_stats_df.loc[start_timestamp:end_timestamp]
    #
    return portfolio_df, portfolio_stats_df


def compute_delay(df: pd.DataFrame, freq: str) -> pd.Series:
    """
    Compute forecast delays from bar timestamps.
    """
    bar_index = df.index.round(freq)
    delay_vals = df.index - bar_index
    delay = pd.Series(delay_vals, bar_index, name="delay")
    return delay


def check_for_missing_bars(df: pd.DataFrame, freq: str) -> None:
    """
    Check that no data bars are missed.
    """
    _LOG.info("Actual index=%s", df.index)
    hpandas.dassert_monotonic_index(df)
    actual_index = df.index.round(freq)
    min_ts = df.index.min()
    max_ts = df.index.max()
    expected_index = pd.date_range(start=min_ts, end=max_ts, freq=freq).round(
        freq
    )
    hdbg.dassert_set_eq(actual_index, expected_index)


# TODO(Grisha): @Dan Provide correct stats descriptions.
def print_stats(df: pd.DataFrame) -> None:
    """
    Print basic stats and sanity checks before doing heavy computations.

    Stats include:
    - minimal index timestamp
    - maximum index timestamp
    - fraction of assets with no difference in num shares
    - fraction of assets with empty num shares
    """
    _LOG.info("min timestamp=%s", df.index.min())
    _LOG.info("max timestamp=%s", df.index.max())
    n_zeros = sum(df["diff_num_shares"].sum(axis=1) == 0)
    _LOG.info(
        "fraction of diff_num_shares=0 is %s",
        hprint.perc(n_zeros, df["diff_num_shares"].shape[0]),
    )
    n_nans = df["diff_num_shares"].sum(axis=1).isna().sum()
    _LOG.info(
        "fraction of diff_num_shares=0 is %s",
        hprint.perc(n_nans, df["diff_num_shares"].shape[0]),
    )


# TODO(Paul): Clean up the system config handling.
def load_config_as_list(path: str) -> List[str]:
    """
    Load config as a list of string lines.
    """
    with open(path) as f:
        lines = f.readlines()
    _LOG.debug("Lines read=%d", len(lines))
    return lines


def diff_lines(list1: List[str], list2: List[str]) -> Tuple[List[str], List[str]]:
    """
    Get output lines that differ.
    """
    list1_only = list(set(list1) - set(list2))
    list2_only = list(set(list2) - set(list1))
    return list1_only, list2_only


# %% [markdown]
# # Set system parameters

# %%
aws_profile = "ck"
file_path = (
    "/shared_data/prod_reconciliation/20220915/simulation/test_data.csv.gz"
)
# file_path = "s3://cryptokaizen-data/unit_test/outcomes/Test_C1b_Time_ForecastSystem_with_DataFramePortfolio_ProdReconciliation/input/test_data.csv.gz"
column_remap = {
    "start_timestamp": "start_datetime",
    "end_timestamp": "end_datetime",
}
timestamp_db_column = "end_datetime"
datetime_columns = ["start_datetime", "end_datetime", "timestamp_db"]
market_data_df = mdata.load_market_data(
    file_path,
    # aws_profile=aws_profile,
    column_remap=column_remap,
    timestamp_db_column=timestamp_db_column,
    datetime_columns=datetime_columns,
)
market_data_df.head()

# %%
min_market_data_end_time = (
    market_data_df["end_datetime"].min().tz_convert(tz="America/New_York")
)
min_market_data_end_time

# %%
max_market_data_end_time = (
    market_data_df["end_datetime"].max().tz_convert(tz="America/New_York")
)
max_market_data_end_time

# %%
date = "2022-09-15"
start_timestamp = pd.Timestamp(date + " 09:10:00", tz="America/New_York")
_LOG.info("start_timestamp=%s", start_timestamp)
end_timestamp = pd.Timestamp(date + " 11:15:00", tz="America/New_York")
_LOG.info("end_timestamp=%s", end_timestamp)

# %%
replayed_delay_in_mins_or_timestamp = get_replayed_delay_in_mins(
    min_market_data_end_time,
    start_timestamp,
)
replayed_delay_in_mins_or_timestamp

# %%
prod_dir = "/shared_data/prod_reconciliation/20220915/prod/system_log_dir_20220915_2hours"
sim_dir = "/shared_data/prod_reconciliation/20220915/simulation/system_log_dir"
prod_portfolio_dir = os.path.join(prod_dir, "process_forecasts/portfolio")
prod_forecast_dir = os.path.join(prod_dir, "process_forecasts")
sim_portfolio_dir = os.path.join(sim_dir, "process_forecasts/portfolio")
sim_forecast_dir = os.path.join(sim_dir, "process_forecasts")

# %%
dict_ = {
    "prod_forecast_dir": prod_forecast_dir,
    "sim_forecast_dir": sim_forecast_dir,
    "prod_portfolio_dir": prod_portfolio_dir,
    "sim_portfolio_dir": sim_portfolio_dir,
    "freq": "5T",
    "start_timestamp": start_timestamp,
    "end_timestamp": end_timestamp,
}
#
config = cconfig.Config.from_dict(dict_)
display(config)

# %% [markdown]
# # Forecasts

# %% [markdown]
# ## Load prod and sim forecasts

# %%
prod_forecast_df = oms.ForecastProcessor.read_logged_target_positions(
    config["prod_forecast_dir"]
)
check_for_missing_bars(prod_forecast_df, config["freq"])
print_stats(prod_forecast_df)
hpandas.df_to_str(prod_forecast_df, log_level=logging.INFO)

# %%
sim_forecast_df = oms.ForecastProcessor.read_logged_target_positions(
    config["sim_forecast_dir"]
)
check_for_missing_bars(sim_forecast_df, config["freq"])
print_stats(sim_forecast_df)
hpandas.df_to_str(sim_forecast_df, log_level=logging.INFO)

# %% [markdown]
# ## Compute forecast prod delay

# %%
prod_forecast_delay = compute_delay(prod_forecast_df, config["freq"])
hpandas.df_to_str(prod_forecast_delay, log_level=logging.INFO)

# %%
# Plot delay in seconds.
prod_forecast_delay.dt.total_seconds().plot(title="delay in seconds")

# %%
prod_forecast_df.index = prod_forecast_df.index.round(config["freq"])
sim_forecast_df.index = sim_forecast_df.index.round(config["freq"])
prod_forecast_df = prod_forecast_df.loc[start_timestamp:end_timestamp]
sim_forecast_df = sim_forecast_df.loc[start_timestamp:end_timestamp]

# %% [markdown]
# ## Compare forecast dataframes

# %%
forecast_corrs = dtfmod.compute_correlations(prod_forecast_df, sim_forecast_df)
hpandas.df_to_str(forecast_corrs, precision=3, log_level=logging.INFO)

# %%
sort_col = "prediction"
hpandas.df_to_str(
    forecast_corrs.sort_values(sort_col, ascending=False),
    num_rows=10,
    precision=3,
    log_level=logging.INFO,
)

# %% [markdown]
# # Orders

# %% [markdown]
# ## Load prod and sim orders

# %%
prod_order_df = oms.ForecastProcessor.read_logged_orders(
    config["prod_forecast_dir"]
)
hpandas.df_to_str(prod_order_df, log_level=logging.INFO)

# %%
sim_order_df = oms.ForecastProcessor.read_logged_orders(
    config["sim_forecast_dir"]
)
hpandas.df_to_str(sim_order_df, log_level=logging.INFO)

# %% [markdown]
# # Portfolios

# %% [markdown]
# ## Load prod portfolio

# %%
prod_portfolio_df, prod_portfolio_stats_df = load_portfolio(
    config["prod_portfolio_dir"],
    config["start_timestamp"],
    config["end_timestamp"],
    config["freq"],
)

# %%
hpandas.df_to_str(prod_portfolio_df, log_level=logging.INFO)

# %%
hpandas.df_to_str(prod_portfolio_stats_df, log_level=logging.INFO)

# %% [markdown]
# ## Load sim portfolio

# %%
sim_portfolio_df, sim_portfolio_stats_df = load_portfolio(
    config["sim_portfolio_dir"],
    config["start_timestamp"],
    config["end_timestamp"],
    config["freq"],
)

# %%
hpandas.df_to_str(sim_portfolio_df, log_level=logging.INFO)

# %%
hpandas.df_to_str(sim_portfolio_stats_df, log_level=logging.INFO)

# %% [markdown]
# ## Compute prod portfolio delay

# %%
prod_portfolio_delay = compute_delay(prod_portfolio_df, config["freq"])
hpandas.df_to_str(prod_portfolio_delay, log_level=logging.INFO)

# %%
prod_portfolio_delay.dt.total_seconds().plot(title="delay in seconds")

# %%
_LOG.info("prod portfolio delay mean=%s", prod_portfolio_delay.mean())
_LOG.info("prod portfolio delay std=%s", prod_portfolio_delay.std())

# %% [markdown]
# ## Normalize bar times

# %%
dfs = [
    prod_portfolio_df,
    prod_portfolio_stats_df,
    sim_portfolio_df,
    sim_portfolio_stats_df,
]

# %%
for df in dfs:
    df.index = df.index.round(config["freq"])

# %% [markdown]
# ## Compare portfolio stats

# %%
portfolio_stats_dfs = {
    "prod": prod_portfolio_stats_df,
    "sim": sim_portfolio_stats_df,
}
portfolio_stats_dfs = pd.concat(portfolio_stats_dfs, axis=1)

# %%
prod_portfolio_stats_df["pnl"].plot()

# %%
sim_portfolio_stats_df["pnl"].plot()

# %%
hpandas.df_to_str(portfolio_stats_dfs, log_level=logging.INFO)

# %%
coplotti.plot_portfolio_stats(portfolio_stats_dfs)

# %%
portfolio_stats_corrs = dtfmod.compute_correlations(
    prod_portfolio_stats_df, sim_portfolio_stats_df
)
display(portfolio_stats_corrs.round(3))

# %%
stats_computer = dtfmod.StatsComputer()
stats_sxs, _ = stats_computer.compute_portfolio_stats(
    portfolio_stats_dfs, config["freq"]
)
display(stats_sxs)

# %% [markdown]
# ## Compare portfolios at the instrument level

# %%
portfolio_corrs = dtfmod.compute_correlations(prod_portfolio_df, sim_portfolio_df)
hpandas.df_to_str(portfolio_corrs, precision=3, log_level=logging.INFO)

# %%
sort_col = "pnl"
hpandas.df_to_str(
    portfolio_corrs.sort_values(sort_col, ascending=False),
    num_rows=10,
    precision=3,
    log_level=logging.INFO,
)
