# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
# ## Imports

# %%
# %load_ext autoreload
# %autoreload 2

import logging
import os

import pandas as pd

import helpers.dbg as dbg
import helpers.printing as prnt

# %%
prnt.config_notebook()

# dbg.init_logger(verbosity=logging.DEBUG)
dbg.init_logger(verbosity=logging.INFO)
# dbg.test_logger()
_LOG = logging.getLogger(__name__)

# %% [markdown]
# # Real-time node
#
# import core.dataflow.real_time as cdrt

# %%
import helpers.datetime_ as hdatetime

# %%
rrt = cdrt.ReplayRealTime(pd.Timestamp("2021-07-27 9:30:00-04:00"))

rct = rrt.get_replayed_current_time()
print("rct=%s" % rct)

# %%
rct = rrt.get_replayed_current_time()
print("rct=%s" % rct)

# %%
rct = rrt.get_replayed_current_time()
print("rct=%s" % rct)

# %%
current_time = hdatetime.get_current_time(tz="ET")
print(current_time)
current_time = pd.Timestamp(current_time)
print(current_time.round("2S"))
# num_seconds
# aligned_current_time = current_time
# print(current_time)

# %%
rrt = cdrt.ReplayRealTime(
    pd.Timestamp("2021-07-27 9:30:00-04:00"), speed_up_factor=60
)

sleep_interval_in_secs = 1.0
num_iterations = 10
# get_current_time = datetime.datetime.now
# get_current_time = lambda : hdatetime.get_current_time(tz="ET")
get_current_time = rrt.get_replayed_current_time
need_to_execute = cdrt.execute_every_5_minutes

cdrt.execute_dag_with_real_time_loop(
    sleep_interval_in_secs, num_iterations, get_current_time, need_to_execute
)

# %% [markdown]
# ## Test real-time node

# %%
start_datetime = pd.Timestamp("2010-01-04 09:30:00")
end_datetime = pd.Timestamp("2010-01-05 09:30:00")
columns = ["close", "volume"]
cdrt.generate_synthetic_data(columns, start_datetime, end_datetime)



# %%
import core.dataflow.nodes.sources as cdtfns

nid = "rtds"

delay_in_secs = 0.0

start_datetime = pd.Timestamp("2010-01-04 09:30:00", tz=hdatetime.get_ET_tz())
end_datetime = pd.Timestamp("2010-01-05 09:30:00", tz=hdatetime.get_ET_tz())

# Use a replayed real-time starting at the same time as the data.
rrt = cdrt.ReplayRealTime(
    start_datetime
)
get_current_time = rrt.get_replayed_current_time

data_builder = cdrt.generate_synthetic_data
data_builder_kwargs = {
    "columns": ["close", "volume"],
    "start_datetime": start_datetime,
    "end_datetime": end_datetime,
    "seed": 42,
}

columns = ["close", "volume"]
rtds = cdtfns.RealTimeDataSource("rtds", delay_in_secs, get_current_time, data_builder, data_builder_kwargs)

#current_time = pd.Timestamp("2010-01-04 09:35:00")
#rtds.set_current_time(current_time)

rtds.fit()

# %%
rtds.fit()

# %% [markdown]
# ## Build pipeline
#

# %%
import core.config as cconfig
import core.dataflow as cdataf

import core.dataflow.real_time as cdrt
import dataflow_amp.returns.pipeline as darp

dag_builder = darp.ReturnsPipeline()
config = dag_builder.get_config_template()

# # Add the source node.
# source_config = cconfig.get_config_from_nested_dict(
#     {
#         "func": cldns.load_single_instrument_data,
#         "func_kwargs": {
#             "start_date": datetime.date(2010, 6, 29),
#             "end_date": datetime.date(2010, 7, 13),
#         },
#     }
# )
# config["load_prices"] = source_config
# config["resample_prices_to_1min", "func_kwargs", "volume_cols"] = ["volume"]
# config["compute_vwap", "func_kwargs", "rule"] = "15T"
# config["compute_vwap", "func_kwargs", "volume_col"] = "volume"

if False:
    from im.kibot.data.config import S3_PREFIX

    ticker = "AAPL"
    file_path = os.path.join(S3_PREFIX, "pq/sp_500_1min", ticker + ".pq")
    source_node_kwargs = {
        "func": cdataf.load_data_from_disk,
        "func_kwargs": {
            "file_path": file_path,
            "start_date": pd.to_datetime("2010-01-04 9:30:00"),
            "end_date": pd.to_datetime("2010-01-04 16:05:00"),
        },
    }
    config["load_prices"] = cconfig.get_config_from_nested_dict(
        source_node_kwargs
    )

else:
    start_date = pd.Timestamp("2010-01-04 09:30:00")
    end_date = pd.Timestamp("2010-01-04 11:30:00")

    source_node_kwargs = {
        "columns": ["close", "vol"],
        "start_date": start_date,
        "end_date": end_date,
    }
    config["load_prices"] = cconfig.get_config_from_nested_dict(
        {
            "source_node_name": "real_time_synthetic",
            "source_node_kwargs": source_node_kwargs,
        }
    )

print(config)

# %%
dag = dag_builder.get_dag(config)

# %%
if False:
    # nid = "compute_ret_0"
    nid = "load_prices"
    node = dag.get_node("load_prices")
    node.reset_current_time()
    node.set_current_time(pd.to_datetime("2010-01-06 9:30:00"))

    dict_ = dag.run_leq_node(nid, "fit")

    print(dict_)

# %%
node = dag.get_node("load_prices")
node.reset_current_time()

for now in cdrt.get_now_time(start_date, end_date):
    print("now=", now)
    execute = cdrt.is_dag_to_execute(now)
    if execute:
        print("Time to execute the DAG")
        node = dag.get_node("load_prices")
        node.set_current_time(now)
        #
        sink = dag.get_unique_sink()
        dict_ = dag.run_leq_node(sink, "fit")
        print(dict_["df_out"].tail(3))

# %%
import dataflow_amp.real_time.real_time_return_pipeline as dtfart

dag_builder = dtfart.RealTimeReturnPipeline()

config = dag_builder.get_config_template()
print("\n# config=\n%s" % config)

dag_builder.validate_config(config)

dag = dag_builder.get_dag(config)

# %%
#print(dag)
cdataf.draw(dag)

# %%
# Align on a even second.
cdrt.align_on_even_second()
#
sleep_interval_in_secs = 1.0
num_iterations = 3
get_current_time = rrt.get_replayed_current_time
need_to_execute = cdrt.execute_every_2_seconds
#
execution_trace, results = cdrt.execute_dag_with_real_time_loop(
    sleep_interval_in_secs,
    num_iterations,
    get_current_time,
    need_to_execute,
    dag,
)

# %%
results[0][1]["df_out"]
