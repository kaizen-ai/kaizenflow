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

# %% [markdown]
# # Description

# %% [markdown]
# This notebook compares the portfolio statistics for historical backtest and shadow trading.

# %% [markdown]
# # Imports

# %%
# %load_ext autoreload
# %autoreload 2
# %matplotlib inline

# %%
import logging

import pandas as pd

import core.plotting as coplotti
import dataflow.model as dtfmod
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint
import reconciliation as reconcil

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # Load data

# %% [markdown]
# ## Historical

# %%
research_portfolio_path = "/shared_data/backtest.C11a.config3/build_tile_configs.C11a.ccxt_v8_1-all.120T.2023-08-01_2024-06-10.ins.run0/portfolio_dfs/20240611_130516/forecast_evaluator_kwargs:optimizer_config_dict:target_gmv=100000"
(
    research_portfolio_df,
    research_portfolio_stats,
) = dtfmod.AbstractForecastEvaluator.load_portfolio_and_stats(
    research_portfolio_path
)
# Select index corresponding to shadow trading period.
# bar_metrics = bar_metrics.loc[paper_trading_portfolio_stats.index]

# %% [markdown]
# ## Paper trading

# %%
paths = [
    "/shared_data2/ecs_tokyo/preprod/prod_reconciliation/C11a.config3/paper_trading/20240609_131000.20240610_130500",
    "/shared_data2/ecs_tokyo/preprod/prod_reconciliation/C11a.config3/paper_trading/20240608_131000.20240609_130500",
    "/shared_data2/ecs_tokyo/preprod/prod_reconciliation/C11a.config3/paper_trading/20240607_131000.20240608_130500",
    "/shared_data2/ecs_tokyo/preprod/prod_reconciliation/C11a.config3/paper_trading/20240606_131000.20240607_130500",
    "/shared_data2/ecs_tokyo/preprod/prod_reconciliation/C11a.config3/paper_trading/20240605_131000.20240606_130500",
    "/shared_data2/ecs_tokyo/preprod/prod_reconciliation/C11a.config3/paper_trading/20240604_131000.20240605_130500",
    "/shared_data2/ecs_tokyo/preprod/prod_reconciliation/C11a.config3/paper_trading/20240603_131000.20240604_130500",
]

# %%
burn_in_bars = 1
data_type = "portfolio"
bar_duration = "120T"

# %%
research_vs_shadow_dfs = []
shadow_portfolio_dfs = []
for path in paths:
    print("***")
    print(path)
    system_log_path_dict = reconcil.get_system_log_dir_paths(path, "scheduled")
    portfolio_path_dict = reconcil.get_system_log_paths(
        system_log_path_dict, data_type
    )
    portfolio_dfs, portfolio_stats_dfs = reconcil.load_portfolio_dfs(
        portfolio_path_dict, bar_duration
    )
    shadow_portfolio_stats = portfolio_stats_dfs["prod"].iloc[burn_in_bars:]
    shadow_portfolio_dfs.append(shadow_portfolio_stats)
    #
    research_vs_shadow = pd.concat(
        [
            research_portfolio_stats.shift(-1).loc[shadow_portfolio_stats.index],
            shadow_portfolio_stats,
        ],
        keys=["research", "shadow_trading"],
        axis=1,
    )
    research_vs_shadow_dfs.append(research_vs_shadow)

# %% [markdown]
# # Plot research vs shadow one by one

# %%
# 20240609_131000.20240610_130500
display(research_vs_shadow_dfs[0].head(5))
coplotti.plot_portfolio_stats(research_vs_shadow_dfs[0])

# %%
# 20240608_131000.20240609_130500
display(research_vs_shadow_dfs[1].head(5))
coplotti.plot_portfolio_stats(research_vs_shadow_dfs[1])

# %%
# 20240607_131000.20240608_130500
display(research_vs_shadow_dfs[2].head(5))
coplotti.plot_portfolio_stats(research_vs_shadow_dfs[2])

# %%
# 20240606_131000.20240607_130500
display(research_vs_shadow_dfs[3].head(5))
coplotti.plot_portfolio_stats(research_vs_shadow_dfs[3])

# %%
# 20240605_131000.20240606_130500
display(research_vs_shadow_dfs[4].head(5))
coplotti.plot_portfolio_stats(research_vs_shadow_dfs[4])

# %%
# 20240604_131000.20240605_130500
display(research_vs_shadow_dfs[5].head(5))
coplotti.plot_portfolio_stats(research_vs_shadow_dfs[5])

# %%
# 20240603_131000.20240604_130500
display(research_vs_shadow_dfs[6].head(5))
coplotti.plot_portfolio_stats(research_vs_shadow_dfs[6])

# %% [markdown]
# # Stitching  DFs together

# %%
burn_in_bars = 1
stitched_shadow_dfs = [df.iloc[burn_in_bars:] for df in shadow_portfolio_dfs]
stitched_shadow_dfs = pd.concat(stitched_shadow_dfs)
research_vs_shadow = pd.concat(
    [
        research_portfolio_stats.shift(-1).loc[stitched_shadow_dfs.index],
        stitched_shadow_dfs,
    ],
    keys=["research", "shadow_trading"],
    axis=1,
)


# %%
display(research_vs_shadow.head(5))

# %%
coplotti.plot_portfolio_stats(research_vs_shadow.sort_index())
