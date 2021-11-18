# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.4
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

import core.config as cconfig
import core.dataflow_model.incremental_single_name_model_evaluator as cdtfmisnmev
import core.dataflow_model.model_evaluator as cdtfmomoev
import core.dataflow_model.model_plotter as cdtfmomopl
import core.dataflow_model.stats_computer as cdtfmostco
import core.plotting as coplotti
import helpers.dbg as hdbg
import helpers.printing as hprint

# %%
hdbg.init_logger(verbosity=logging.INFO)
# hdbg.init_logger(verbosity=logging.DEBUG)

_LOG = logging.getLogger(__name__)

# _LOG.info("%s", env.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # Notebook config

# %%
# Read from env var.
eval_config = cconfig.Config.from_env_var("AM_CONFIG_CODE")

# Override config.
if eval_config is None:
    src_dir = ""
    file_name = "result_bundle.v2_0.pkl"
    prediction_col = ""
    target_col = ""
    aws_profile = None

    eval_config = cconfig.get_config_from_nested_dict(
        {
            "compute_stats_kwargs": {
                "src_dir": src_dir,
                "file_name": file_name,
                "prediction_col": prediction_col,
                "target_col": target_col,
                "start": None,
                "end": None,
                "selected_idxs": None,
                "aws_profile": aws_profile,
            },
            "aggregate_single_name_models": {
                "src_dir": src_dir,
                "file_name": file_name,
                "position_intent_1_col": "",
                "ret_0_col": "",
                "spread_0_col": "",
                "prediction_col": prediction_col,
                "target_col": target_col,
                "start": None,
                "end": None,
                "selected_idxs": None,
                "aws_profile": aws_profile,
            },
            "bh_adj_threshold": 0.1,
        }
    )

print(str(eval_config))

# %% [markdown]
# # Compute stats

# %%
stats = cdtfmisnmev.compute_stats_for_single_name_artifacts(
    **eval_config["compute_stats_kwargs"].to_dict()
)

# %%
# TODO(gp): Move this chunk of code into a function.
col_mask = (
    stats.loc["signal_quality"].loc["sr.adj_pval"]
    < eval_config["bh_adj_threshold"]
)
selected = stats.loc[:, col_mask].columns.to_list()
not_selected = stats.loc[:, ~col_mask].columns.to_list()

print("num model selected=%s" % hprint.perc(len(selected), stats.shape[1]))
print("model selected=%s" % selected)
print("model not selected=%s" % not_selected)

# Use `selected = None` to show all of the models.

# TODO(Paul): call `multipletests_plot()`

# %% [markdown]
# # Build portfolio

# %%
portfolio, daily_dfs = cdtfmisnmev.aggregate_single_name_models(
    **eval_config["aggregate_single_name_models"].to_dict()
)

# %%
portfolio.dropna().head()

# %%
stats_computer = cdtfmostco.StatsComputer()

# %% [markdown]
# # TODO
# - Compute stats of `portfolio`
# - Create daily rets and daily pnl dfs
#   - `plot_effective_correlation_rank()`
#   - `plot_correlation_matrix()`
#   - `plot_pnl()`
# - Plot `portfolio` performance
#   - `plot_cumulative_returns()`
#   - `plot_monthly_heatmap()`
#   - `plot_yearly_barplot()`
#   - `plot_sharpe_ratio_panel()`
#   - `plot_qq()`
#   - ...

# %%
