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
# The notebook monitors a scheduled trading run, i.e. displays performance metrics (e.g., pnl), broker info (e.g., current balance, open positions).
#
# Note: this is a copy of `oms/notebooks/Master_PnL_real_time_observer.old.py` adapted for scheduled trading runs.

# %%
# TODO(Nina): consider renaming to "System_observer".

# %%
# %load_ext autoreload
# %autoreload 2
# %matplotlib inline

# %% [markdown]
# # Imports

# %%

import logging
import os

import numpy as np
import pandas as pd

import core.config as cconfig
import core.plotting as coplotti
import dataflow.model as dtfmod
import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import helpers.hsystem as hsystem
import oms.broker.ccxt.ccxt_broker_instances as obccbrin
import oms.hsecrets as homssec
import reconciliation as reconcil

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # Build the reconciliation config

# %%
# When running manually, specify the path to the config to load config from file,
# for e.g., `.../reconciliation_notebook/fast/result_0/config.pkl`.
config_file_name = None
# Set 'replace_ecs_tokyo = True' if running the notebook manually.
replace_ecs_tokyo = False
config = cconfig.get_notebook_config(
    config_file_path=config_file_name, replace_ecs_tokyo=replace_ecs_tokyo
)
if config is None:
    _LOG.info("Using hardwired config")
    # Specify the config directly when running the notebook manually.
    # Below is just an example.
    prod_data_root_dir = "/shared_data/CmTask7933/system_reconciliation"
    dag_builder_ctor_as_str = (
        "dataflow_orange.pipelines.C3.C3a_pipeline_tmp.C3a_DagBuilder_tmp"
    )
    run_mode = "paper_trading"
    start_timestamp_as_str = "20240415_131000"
    end_timestamp_as_str = "20240416_130500"
    mode = "scheduled"
    save_plots_for_investors = True
    tag = ""
    html_bucket_path = henv.execute_repo_config_code("get_html_bucket_path()")
    s3_dst_dir = os.path.join(html_bucket_path, "pnl_for_investors")
    config_list = reconcil.build_system_observer_configs(
        prod_data_root_dir,
        dag_builder_ctor_as_str,
        run_mode,
        start_timestamp_as_str,
        end_timestamp_as_str,
        mode,
        save_plots_for_investors,
        s3_dst_dir=s3_dst_dir,
        tag=tag,
    )
    config = config_list[0]
print(config)

# %% [markdown]
# # System config

# %%
# Load the system config.
config_file_name = "system_config.output.values_as_strings.pkl"
system_config_path = os.path.join(config["system_log_dir"], config_file_name)
system_config = cconfig.load_config_from_pickle(system_config_path)
print(system_config)

# %% [markdown]
# # Last update time

# %%
# TODO(Grisha): tz should go to notebook's config.
tz = "ET"
current_time = hdateti.get_current_time(tz)
print(current_time)

# %%
portfolio_stats_dir = os.path.join(
    config["system_log_dir"], "process_forecasts", "portfolio", "statistics"
)
_LOG.info("Portfolio stats directory: %s", portfolio_stats_dir)
# Some flows below require a few bars to be already computed, we use the number of CSV Portfolio
# files in the portoflio dir as a proxy of the number of bars computed.
if os.path.exists(portfolio_stats_dir):
    cmd = f"ls {portfolio_stats_dir}"
    _, portfolio_files = hsystem.system_to_string(cmd)
    if portfolio_files == "":
        # 0 bars if dir is empty or does not exist.
        n_bars_passed = 0
    else:
        n_bars_passed = len(portfolio_files.split("\n"))
else:
    n_bars_passed = 0
_LOG.info("Bars passed: %s", n_bars_passed)

# %% [markdown]
# # Current balance, open positions

# %%
# Get Broker.
universe_version = system_config["market_data_config"]["universe_version"]
# TODO(Grisha): store `exchange, preprod, account_type, secret_id` as separate
# fields in SystemConfig.
exchange, preprod, account_type, secret_id = system_config[
    "secret_identifier_config"
].split(".")
secret_identifier = homssec.SecretIdentifier(
    exchange, preprod, account_type, secret_id
)
# Use temporary local dir in order not to override related production results
# for this run.
# TODO(Juraj): hardcoded contract type "futures".
broker = obccbrin.get_CcxtBroker_exchange_only_instance1(
    universe_version, secret_identifier, "/app/tmp.log_dir", "futures"
)

# %%
broker.get_open_positions()

# %%
total_balance = broker.get_total_balance()
_LOG.info(total_balance)

# %% [markdown]
# # Specify data to load

# %% [markdown]
# # Portfolio

# %% [markdown]
# ## Load logged portfolios (prod)

# %%
# We need at least 2 bars, because the first one is NaN.
portfolio_min_bars = 2
# StatsComputer requires at least a couple of non-empty bars.
stats_computer_min_bars = 3

# %%
if n_bars_passed < portfolio_min_bars:
    _LOG.warning(
        "Not enough data to load portfolio stats, the number of computed bars is %s, required %s, skipping.",
        n_bars_passed,
        portfolio_min_bars,
    )
else:
    # Points to `system_log_dir/process_forecasts/portfolio`.
    data_type = "portfolio"
    portfolio_path = reconcil.get_data_type_system_log_path(
        config["system_log_dir"], data_type
    )
    _LOG.info("portfolio_path=%s", portfolio_path)
    portfolio_dfs, portfolio_stats_dfs = reconcil.load_portfolio_dfs(
        {"prod": portfolio_path},
        config["meta"]["bar_duration"],
    )
    hpandas.df_to_str(portfolio_dfs["prod"], num_rows=5, log_level=logging.INFO)

# %%
if n_bars_passed < portfolio_min_bars:
    _LOG.warning(
        "Not enough data to compute portfolio stats, the number of computed bars is %s, required %s, skipping.",
        n_bars_passed,
        portfolio_min_bars,
    )
else:
    portfolio_stats_df = pd.concat(portfolio_stats_dfs, axis=1)
    #
    hpandas.df_to_str(portfolio_stats_df, num_rows=5, log_level=logging.INFO)

# %% [markdown]
# ## Compute Portfolio statistics (prod)

# %%
# There's nothing to plot if we have 2 bars only. We burn
# the first bar since it contain NaN.
if n_bars_passed < portfolio_min_bars:
    _LOG.warning(
        "Not enough data to compute portfolio stats, the number of computed bars is %s, required %s, skipping.",
        n_bars_passed,
        portfolio_min_bars,
    )
else:
    bars_to_burn = 1
    coplotti.plot_portfolio_stats(portfolio_stats_df.iloc[bars_to_burn:])

# %%
# We need at least 3 bars to calculate stats because we cut the first
# bar with NaNs, and stats could be calculated minimum with 2 bars.
if n_bars_passed < stats_computer_min_bars:
    _LOG.warning(
        "Not enough data to compute portfolio stats, the number of computed bars is %s, required %s, skipping.",
        n_bars_passed,
        stats_computer_min_bars,
    )
else:
    stats_computer = dtfmod.StatsComputer()
    stats_sxs, _ = stats_computer.compute_portfolio_stats(
        portfolio_stats_df.iloc[bars_to_burn:], config["meta"]["bar_duration"]
    )
    display(stats_sxs)


# %%
# Check the balance. Assert if it's below the threshold.
# TODO(Nina): pass via notebook's config.
balance_threshold = -1000
usdt_balance = total_balance["USDT"]
_LOG.info("Current USDT balance is %s $", np.round(usdt_balance, 2))
#
msg = f"USDT balance is below the threshold: {usdt_balance} < {balance_threshold} USDT"
hdbg.dassert_lt(balance_threshold, usdt_balance, msg=msg)

# %%
if n_bars_passed < portfolio_min_bars:
    _LOG.warning(
        "Not enough data to compute portfolio stats, the number of computed bars is %s, required %s, skipping.",
        n_bars_passed,
        portfolio_min_bars,
    )
else:
    pnl = portfolio_stats_df.T.xs("pnl", level=1).T
    cum_pnl = pnl.cumsum()
    # Assert if PnL below the threshold.
    # TODO(Nina): pass via notebook's config.
    pnl_threshold = -100
    # Check the latest row, i.e. for current timestamp.
    pnl = cum_pnl["prod"].iloc[-1]
    _LOG.info("Current notional cumulative PnL is %s $", np.round(pnl, 2))
    #
    msg = f"Current notional cumulative PnL is below the threshold: {pnl} < {pnl_threshold}$"
    hdbg.dassert_lt(pnl_threshold, pnl, msg=msg)

# %%
if n_bars_passed < portfolio_min_bars:
    _LOG.warning(
        "Not enough data to compute portfolio stats, the number of computed bars is %s, required %s, skipping.",
        n_bars_passed,
        portfolio_min_bars,
    )
else:
    # Check Current notional cumulative PnL in relative terms.
    # TODO(Nina): pass via notebook's config.
    fraction_threshold = -0.1
    #
    gmv = portfolio_stats_df.T.xs("gmv", level=1).T
    gmv = gmv.replace(0, np.nan)
    rolling_gmv = gmv.expanding().mean()
    # To compute average GMV use GMV values available up to the current point in time.
    cum_pnl_gmv = cum_pnl.divide(rolling_gmv)["prod"].iloc[-1]
    _LOG.info(
        "Current notional cumulative PnL as fraction of GMV is %s",
        np.round(cum_pnl_gmv, 5),
    )
    #
    msg = f"Current notional cumulative PnL as fraction of GMV is below the threshold {cum_pnl_gmv} < {fraction_threshold}"
    hdbg.dassert_lt(fraction_threshold, cum_pnl_gmv, msg=msg)

# %%
if n_bars_passed < portfolio_min_bars:
    _LOG.warning(
        "Not enough data to compute portfolio stats, the number of computed bars is %s, required %s, skipping.",
        n_bars_passed,
        portfolio_min_bars,
    )
else:
    gross_volume = portfolio_stats_df.T.xs("gross_volume", level=1).T
    gross_volume = gross_volume.replace(0, np.nan)
    cum_gross_volume = gross_volume.cumsum().ffill()
    #
    cum_pnl_gross_vol_bps = 1e4 * cum_pnl.iloc[-1] / cum_gross_volume.iloc[-1]
    _LOG.info(
        "Current notional cumulative PnL as fraction of cumulative gross volume in bps %s",
        np.round(cum_pnl_gross_vol_bps.iloc[0], 4),
    )
