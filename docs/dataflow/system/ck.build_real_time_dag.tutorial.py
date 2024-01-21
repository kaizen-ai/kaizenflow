# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.15.2
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
import logging

import pandas as pd

import core.finance as cofinanc
import core.real_time_example as cretiexa
import dataflow.core as dtfcore
import dataflow.system.real_time_dag_runner as dtfsrtdaru
import dataflow.system.source_nodes as dtfsysonod
import dataflow_amp.pipelines.realtime_etl_data_observer as dtfapredo
import helpers.hasyncio as hasynci
import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint
import market_data.market_data_example as mdmadaex

# %%
hdbg.init_logger(verbosity=logging.DEBUG)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # Generate synthetic OHLCV data

# %%
tz = "America/New_York"
start_datetime = pd.Timestamp("2023-01-03 09:30:00", tz=tz)
end_datetime = pd.Timestamp("2023-01-03 11:30:00", tz=tz)
asset_ids = [101, 201, 301]
bar_duration = "1S"
# TODO(gp): random_ohlcv_bars -> market_data
random_ohlcv_bars = cofinanc.generate_random_ohlcv_bars(
    start_datetime,
    end_datetime,
    asset_ids,
    bar_duration=bar_duration,
)
# random_ohlcv_bars.set_index("end_datetime", inplace=True, drop=True)
# random_ohlcv_bars.index.name = "end_datetime"
# _LOG.debug(hpandas.df_to_str(random_ohlcv_bars))
random_ohlcv_bars.head(10)

# %% [markdown]
# # Build a data source node

# %%
market_data = random_ohlcv_bars
# # Put the synthetic data into "DataFlow format".
# market_data = random_ohlcv_bars.drop(
#     columns=["start_datetime", "timestamp_db"]
# ).pivot(index="end_datetime", columns="asset_id")
# _LOG.debug(hpandas.df_to_str(market_data)

# %%
knowledge_datetime_col_name = "timestamp_db"
# market_data[knowledge_datetime_col_name] = market_data.index
event_loop = None
# event_loop = hasynci._EventLoop()
# asyncio.set_event_loop(event_loop)
replayed_delay_in_mins_or_timestamp = 0
(
    replayed_market_data,
    get_wall_clock_time,
) = mdmadaex.get_ReplayedTimeMarketData_from_df(
    event_loop, replayed_delay_in_mins_or_timestamp, market_data
)

# %%
timedelta = pd.Timedelta("1T")
# timedelta = pd.Timedelta("1S")
multiindex_output = True
replayed_market_data_node = dtfsysonod.RealTimeDataSource(
    "replayed_market_data",
    replayed_market_data,
    timedelta,
    knowledge_datetime_col_name,
    multiindex_output,
)

# %% [markdown]
# # Build a dag builder

# %%
dag_builder = dtfapredo.Realtime_etl_DataObserver_DagBuilder()
dag_config = dag_builder.get_config_template()
dag = dag_builder.get_dag(dag_config)
dag.insert_at_head(replayed_market_data_node)

# %%
dtfcore.draw(dag)

# %%
# # An example DAG execution.
# dag_df_out = dag.run_leq_node("compute_ret_0", "fit")["df_out"]
# _LOG.debug(hpandas.df_to_str(dag_df_out))

# %% [markdown]
# # Run DAG

# %%
# Set up the event loop.
bar_duration_in_secs = 60
execute_rt_loop_kwargs = cretiexa.get_replayed_time_execute_rt_loop_kwargs(
    bar_duration_in_secs, event_loop=event_loop
)
# Align on a second boundary.
get_wall_clock_time = lambda: hdateti.get_current_time(
    tz="ET", event_loop=event_loop
)
# creatime.align_on_time_grid(
#     get_wall_clock_time, bar_duration_in_secs, event_loop=event_loop
# )
dag_runner_kwargs = {
    "dag": dag,
    "fit_state": None,
    "execute_rt_loop_kwargs": execute_rt_loop_kwargs,
    "dst_dir": None,
    "get_wall_clock_time": get_wall_clock_time,
    "bar_duration_in_secs": bar_duration_in_secs,
    # We don't want to set the current bar in this test.
    # "set_current_bar_timestamp": False,
    "set_current_bar_timestamp": True,
}
# Run.
dag_runner = dtfsrtdaru.RealTimeDagRunner(**dag_runner_kwargs)
result_bundles = hasynci.run(dag_runner.predict(), event_loop=event_loop)
events = dag_runner.events
