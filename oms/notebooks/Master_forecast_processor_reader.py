# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.3
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
import os

import pandas as pd

import core.config as cconfig
import core.plotting as coplotti
import dataflow.model as dtfmod
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint
import oms as oms

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %%
log_dir = ""

# %%
paper_df, paper_stats_df = oms.AbstractPortfolio.read_state(
    os.path.join(log_dir, "portfolio")
)

# %%
coplotti.plot_portfolio_stats(paper_stats_df, freq="B")

# %%
stats_computer = dtfmod.StatsComputer()
summary_stats, _ = stats_computer.compute_portfolio_stats(
    paper_stats_df, "B"
)
display(summary_stats)

# %%
target_positions = oms.ForecastProcessor.read_logged_target_positions(
    log_dir
)
