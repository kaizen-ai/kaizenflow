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

import datetime
import logging
import os

import pandas as pd
from pyarrow import parquet
import s3fs

import helpers.dbg as dbg
import helpers.env as env
import helpers.printing as prnt

# %%
prnt.config_notebook()

# dbg.init_logger(verbosity=logging.DEBUG)
dbg.init_logger(verbosity=logging.INFO)
# dbg.test_logger()
_LOG = logging.getLogger(__name__)

# %%
# def print_columns(df: pd.DataFrame) -> None:
#     print("# Columns")
#     print("num_cols=%s" % len(df.columns))
#     print(", ".join(df.columns.tolist()))

# %% [markdown]
# ## Real-time node

# %%
import time

# %% [markdown]
# ## Test real-time node

# %%
nid = "rtds"
start_date = pd.Timestamp("2010-01-04 09:30:00")
end_date = pd.Timestamp("2010-01-10 09:30:00")

columns = ["close", "volume"]
rtds = dartu.RealTimeDataSource("rtds", columns start_date, end_date)

now = pd.Timestamp("2010-01-04 09:35:00")
rtds.set_now_time(now)
    
rtds.fit()

# %% [markdown]
# ## Build pipeline

# %%
import dataflow_amp.real_time.utils as dartu
import dataflow_amp.returns.pipeline as darp
import core.dataflow as cdataf
import core.config as cconfig

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
    end_date = pd.Timestamp("2010-01-05 09:30:00")
    
    source_node_kwargs = {
        "columns": ["close", "vol"],
        "start_date": start_date,
        "end_date": end_date,
    }
    config["load_prices"] = cconfig.get_config_from_nested_dict({
        "source_node_name": "real_time_synthetic",
        "source_node_kwargs": source_node_kwargs
    })

print(config)

# %%
dag = dag_builder.get_dag(config)

# %%
if False:
    #nid = "compute_ret_0"
    nid = "load_prices"
    node = dag.get_node("load_prices")
    node.set_now_time(pd.to_datetime("2010-01-06 9:30:00"))

    dict_ = dag.run_leq_node(nid, "fit")

    print(dict_)

# %%
# import helpers.printing as hprint
# df = cldns.load_db_example_data()
# print("end_time=[%s, %s]" % (min(df["end_time"]), max(df["end_time"])))
# print(df.shape)


# datetime_ = pd.Timestamp("2021-07-22 20:01:00-00:00")
# print(datetime_)
# df = cldns.get_db_data(datetime_)
# print("end_time=[%s, %s]" % (min(df["end_time"]), max(df["end_time"])))
# print(df.shape)

# df.head()

# %%
#df[["start_time", "end_time", "timestamp_db"]]

# %%
node = dag.get_node("load_prices")
node.reset_now_time()
    
for now in dartu.get_now_time(start_date, end_date):
    print("now=", now)
    execute = dartu.is_dag_to_execute(now)
    if execute:
        print("Time to execute the DAG")
        node = dag.get_node("load_prices")
        node.set_now_time(now)
        #
        sink = dag.get_unique_sink()
        dict_ = dag.run_leq_node(sink, "fit")
        print(dict_["df_out"].tail(3))

