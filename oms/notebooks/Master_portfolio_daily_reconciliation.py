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

import logging

import pandas as pd

import core.config as cconfig
import core.plotting as coplotti
import dataflow.model as dtfmod
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint
import oms as oms

# %% run_control={"marked": true}
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %%
import oms

log_dir = "/app/process_forecasts/2022-07-20/portfolio"
portfolio_df, stats_df = oms.Portfolio.read_state(log_dir)

# %%
#portfolio_df.columns

stats_df

# %%
log_dir = "/cache/production/process_forecasts.20220718/2022-07-18/portfolio"
portfolio_df2, stats_df2 = oms.Portfolio.read_state(log_dir)

# %%
stats_df2

# %%
stats_df["pnl"].plot()
stats_df2["pnl"].plot()

# %%
sta

# %%
time = "121500"
sim_dir = "/app/process_forecasts/2022-07-20/"
sim_file = f"/app/process_forecasts/2022-07-20/evaluate_forecasts/prediction/20220714_{time}.csv"
    
prod_dir = "/app/prod_execution/20220714/"

# prod_execution/20220714/process_forecasts/2022-07-14/evaluate_forecasts/prediction/20220714_113000.csv
prod_file = f"/app/prod_execution/20220714/process_forecasts/2022-07-14/evaluate_forecasts/prediction/20220714_{time}.csv"

# %%
sim_df = pd.read_csv(sim_file, index_col=0)
sim_df.index = pd.to_datetime(sim_df.index)
display(sim_df.head())

prod_df = pd.read_csv(prod_file, index_col=0)
prod_df.index = pd.to_datetime(prod_df.index)
display(prod_df.head())

# %%
date = "2022-07-14"
start_timestamp = pd.Timestamp(date + " 09:30:00", tz="America/New_York")
end_timestamp = pd.Timestamp(date + " 16:00:00", tz="America/New_York")

# %%
sim_df = sim_df[start_timestamp:end_timestamp]
prod_df = prod_df[start_timestamp:end_timestamp]
#prod_df.dropna()

# %%
sim_df.tail()

# %%
prod_df.tail()

# %%
sim_df == prod_df
