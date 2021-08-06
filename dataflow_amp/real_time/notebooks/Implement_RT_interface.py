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

# %% [markdown]
# ## Use real_time_return_pipeline

# %%
import dataflow_amp.real_time.real_time_return_pipeline as dtfart

dag_builder = dtfart.RealTimeReturnPipeline()

config = dag_builder.get_config_template()
print("\n# config=\n%s" % config)

dag_builder.validate_config(config)

dag = dag_builder.get_dag(config)

# %%
# print(dag)
cdataf.draw(dag)

# %%
# # Align on a even second.
# cdrt.align_on_even_second()
# #
# sleep_interval_in_secs = 1.0
# num_iterations = 3
# get_current_time = rrt.get_replayed_current_time
# need_to_execute = cdrt.execute_every_2_seconds
# #
# events, results = cdrt.execute_dag_with_real_time_loop(
#     sleep_interval_in_secs,
#     num_iterations,
#     get_current_time,
#     need_to_execute,
#     dag,
# )

# %%
results[0][1]["df_out"]

# %%
##

import core.dataflow.real_time as cdrt

# %%
import helpers.datetime_ as hdatetime

start_datetime = pd.Timestamp("2010-01-04 09:30:00", tz=hdatetime.get_ET_tz())
end_datetime = pd.Timestamp("2010-01-05 09:30:00", tz=hdatetime.get_ET_tz())

# Use a replayed real-time starting at the same time as the data.
rrt = cdrt.ReplayRealTime(start_datetime)
get_current_time = rrt.get_replayed_current_time

# %%
import core.dataflow as cdtf

execute_rt_loop_kwargs = {
    "sleep_interval_in_secs": 1.0,
    "num_iterations": 3,
    "get_current_time": rrt.get_replayed_current_time,
    "need_to_execute": cdrt.execute_every_2_seconds,
}

#
kwargs = {
    "config": config,
    "dag_builder": dag_builder,
    "fit_state": None,
    #
    "execute_rt_loop_kwargs": execute_rt_loop_kwargs,
    #
    "dst_dir": None,
}

dag_runner = cdtf.RealTimeDagRunner(**kwargs)

# Align on a even second.
cdrt.align_on_even_second()
result = dag_runner.predict()

# %%
result[0]

# %%
len(result[1])

# %%
import pandas as pd

import helpers.unit_test as hut

num_cols = 2
seed = 42
date_range_kwargs = {
    "start": pd.Timestamp("2010-01-01"),
    "end": pd.Timestamp("2010-02-01"),
    "freq": "1B",
}
# pd.date_range(**date_range_kwargs)
data = hut.get_random_df(num_cols, seed=seed, date_range_kwargs=date_range_kwargs)
print(data)


config = {
    "rule": "1B",
    "agg_func": "last",
    "resample_kwargs": None,
    "agg_func_kwargs": None,
}
node = cdnt.Reample("resample", **config)
df_out = node.fit(data)["df_out"]

# %%
import core.dataflow.nodes.transformers as cdtfnt

nid = "nop"


def func(df_in):
    return df_in


func_kwargs = {}

node = cdtfnt.FunctionWrapper(nid, func, func_kwargs)

node.fit(data)

# %%
import core.dataflow.test.test_builders as test_builders

dag_builder = test_builders._NaivePipeline()

config = dag_builder.get_config_template()

dag_builder.get_dag(config)
