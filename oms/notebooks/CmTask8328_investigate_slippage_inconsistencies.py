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
# # Imports

# %%
# %load_ext autoreload
# %autoreload 2

import logging

import core.finance.target_position_df_processing as cftpdp
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
# # Load prod and sim portfolio and prod target positions

# %%
portfolio_path_dict = {
    "prod": "/shared_data/ecs/preprod/prod_reconciliation/C11a.config3/prod/20240516_220000.20240517_100000/prod/system_log_dir.manual/process_forecasts/portfolio",
    "sim": "/shared_data/ecs/preprod/prod_reconciliation/C11a.config3/prod/20240516_220000.20240517_100000/simulation/system_log_dir.manual/process_forecasts/portfolio",
}
bar_duration = "120T"
portfolio_dfs, portfolio_stats_dfs = reconcil.load_portfolio_dfs(
    portfolio_path_dict,
    bar_duration,
)

# %%
prod_target_position_df = reconcil.load_target_positions(
    portfolio_path_dict["prod"].strip("portfolio"),
    bar_duration,
)
prod_target_position_df.head(3)

# %% [markdown]
# # Compute Pnl difference between sim and prod

# %%
# Exclude the last 2 bars as they might be "special" because we use them
# to liquidate holdings.
prod_total_pnl = portfolio_stats_dfs["prod"]["pnl"].iloc[:-2].sum()
prod_total_pnl

# %%
sim_total_pnl = portfolio_stats_dfs["sim"]["pnl"].iloc[:-2].sum()
sim_total_pnl

# %%
total_pnl_diff = sim_total_pnl - prod_total_pnl
total_pnl_diff

# %% [markdown]
# # Compute slippage from prod portfolio notional executed trades

# %%
# To compute slippage from `executed_trades_notional` we must make sure that
# there were no underfills by comparing executed shares.
per_bar_executed_trades_diff = (
    portfolio_dfs["prod"]["executed_trades_shares"]
    - portfolio_dfs["sim"]["executed_trades_shares"]
).sum(axis=1)
per_bar_executed_trades_diff

# %%
# Compute notional slippage for "buy" trades.
prod_buy_df = portfolio_dfs["prod"]["executed_trades_notional"][
    portfolio_dfs["prod"]["executed_trades_notional"] > 0
]
sim_buy_df = portfolio_dfs["sim"]["executed_trades_notional"][
    portfolio_dfs["sim"]["executed_trades_notional"] > 0
]
buy_slippage_notional = prod_buy_df.sub(sim_buy_df, fill_value=0)
buy_slippage_notional.iloc[:-2].sum().sum()

# %%
# Compute notional slippage for "sell" trades.
prod_sell_df = portfolio_dfs["prod"]["executed_trades_notional"][
    portfolio_dfs["prod"]["executed_trades_notional"] < 0
]
sim_sell_df = portfolio_dfs["sim"]["executed_trades_notional"][
    portfolio_dfs["sim"]["executed_trades_notional"] < 0
]
sell_slippage_notional = sim_sell_df.sub(prod_sell_df, fill_value=0)
sell_slippage_notional.iloc[:-2].sum().sum()

# %%
# Compute total notional slippage. The number should match the pnl difference above.
total_slippage_notional = buy_slippage_notional.sub(
    sell_slippage_notional, fill_value=0
)
total_slippage_notional.iloc[:-2].sum().sum()

# %%
# Per-bar notional slippage.
total_slippage_notional.iloc[:-2].sum(axis=1)

# %% [markdown]
# # Compute slippage using the `compute_execution_quality_df` function

# %%
(
    execution_quality_df,
    execution_quality_stats_df,
) = cftpdp.compute_execution_quality_df(
    portfolio_dfs["prod"],
    prod_target_position_df,
)
execution_quality_stats_df.head(3)

# %%
# Total notional slippage.
execution_quality_df["slippage_notional"].iloc[:-2].sum().sum()

# %%
# Per-bar notional slippage.
execution_quality_df["slippage_notional"].iloc[:-2].sum(axis=1)

# %% [markdown]
# # Compare

# %%
# Compare slippage computed from portfolio trades directly to the one
# computed using `compute_execution_quality_df()`. Both versions of slippage
# should match on all levels (per-bar, per-instrument).

# %%
# Per-bar slippage comparison.
slippage_diff_per_bar = total_slippage_notional.iloc[:-2].sum(
    axis=1
) - execution_quality_df["slippage_notional"].iloc[:-2].sum(axis=1)
slippage_diff_per_bar

# %%
# Per-asset slippage comparison.
slippage_diff_per_asset = total_slippage_notional.iloc[:-2].sum(
    axis=0
) - execution_quality_df["slippage_notional"].iloc[:-2].sum(axis=0)
slippage_diff_per_asset
