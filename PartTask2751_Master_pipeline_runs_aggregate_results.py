# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.4.2
#   kernelspec:
#     display_name: Python [conda env:.conda-p1_develop] *
#     language: python
#     name: conda-env-.conda-p1_develop-py
# ---

# %% [markdown]
# # Description

# %% [markdown]
# - Evaluate multiple runs from DAG modeling pipelines
# - Calculate top-level stats and adjust for multiple hypothesis testing

# %% [markdown]
# # Imports

# %%
# %load_ext autoreload
# %autoreload 2


import pandas as pd

import automl.notebooks.utils as amlnut
import core.config as cfg
import core.config_builders as cfgb
import core.plotting as plot
import core.statistics as stats
import core.timeseries_study as tss

# %% [markdown]
# # Load config

# %%
eval_config = cfgb.get_config_from_env()

# %%
# if eval_config is None:
if True:
    eval_config = cfgb.get_config_from_nested_dict(
        {
            "results_directory": "/data/automl/experiments/basf/RH_1",
            "metrics": {
                "perf_": stats.compute_annualized_sharpe_ratio,
                "ttest_": stats.ttest_1samp,
                "moments_": stats.compute_moments,
            },
            "perform_global_adj": {"pval_col": "ttest_pval",},
            "global_adj_pval_threshold": 0.2,
            "perform_family_adj": {"pval_col": "ttest_pval", "tag_col": "tag",},
            "perform_family_selection": {
                "tag_col": "tag",
                "sr_col": "perf_ann_sharpe",
                "adj_pval_threshold": 0.2,
                "ann_sharpe_threshold": 0.7,
                "num": 10,
            },
            "tags": (
                "rh1",
                "rh2",
                "rh3",
                "rh4",
                "rh5",
                "rh6",
                "rh7",
                "rh8",
                "rh9",
                "rh10",
                "rh11",  # Selected (Propylene)
                "rh12",
                "rh13",  # Selected (Butadiene)
                "rh14",
                "rh15",  # Selected (Butadiene)
                "butadiene",
            ),
            "pipeline_stages_to_plot": {
                "sig/load_data",
                "sig/dropna",
                "rets/clip",
                "model/ml",
            },
        }
    )

# %%
eval_config

# %% [markdown]
# # Load pipeline run configs

# %%
# Load configs before loading (larger) result_bundles.
config_dict = amlnut.load_files(eval_config["results_directory"], "config.pkl")

# %%
# Create config dataframe.
configs = cfg.convert_to_dataframe(config_dict.values())

# %%
configs.head(3)

# %%
# Create dataframe of config diffs.
diffs = cfg.diff_configs(config_dict.values())
config_diffs = cfg.convert_to_dataframe(diffs).dropna(how="all", axis=1)

# %%
config_diffs.head(3)

# %% [markdown]
# # Load pipeline result bundles

# %%
result_bundles = amlnut.load_files(
    eval_config["results_directory"], "result_bundle.pkl"
)

# %%
len(result_bundles)

# %% [markdown]
# # Calculate metrics and performance statistics

# %%
pnls = amlnut.compute_pnl_v2(result_bundles)

# %%
# TODO: Add a progress bar.
pnl_stats = tss.map_dict_to_dataframe(
    pnls, eval_config["metrics"].to_dict()
).transpose()

# %%
pnl_stats.head(3)

# %% [markdown]
# ## Global adjustment

# %%
pnl_stats_with_adj = amlnut.perform_global_adj(
    pnl_stats, **eval_config["perform_global_adj"].to_dict()
)

# %%
pnl_stats_with_adj

# %%
results_df = pd.concat([pnl_stats_with_adj, config_diffs], axis=1)

# %%
results_df.head()

# %%
#Using get_multiple_plots does not allow us to use multipletests_plot with series or dataframe with one column.
plot.multipletests_plot(
    results_df["global_adj_pval"].dropna(),
    threshold=eval_config["global_adj_pval_threshold"],
)

# %% [markdown]
# ## Family-wise adjustment

# %%
results_df = amlnut.perform_family_adj(
    results_df, **eval_config["perform_family_adj"].to_dict()
)

# %%
results_df.head(3)

# %%
tags = results_df["tag"].unique()
adj_cols = tags + "_adj_pval"

# %%
plot.multipletests_plot(
    results_df[adj_cols],
    threshold=eval_config["global_adj_pval_threshold"],
    num_cols=2
)

# %% [markdown]
# ## Plot selected PnL

# %%
selected = amlnut.perform_family_selection(
    results_df, pnls, **eval_config["perform_family_selection"].to_dict()
)

# %%
selected.keys()

# %%
# TODO(2752): Make these fancy.
#
# for tag in selected.keys():
#    selected[tag].cumsum().plot()
