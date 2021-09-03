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
# # Description
#
# - Initialize with returns, predictions, target volatility, and oos start date
# - Evaluate portfolios generated from the predictions
#
# - TODO(gp): This should be called `Master_model_evaluator` like the class

# %% [markdown]
# # Imports

# %%
# %load_ext autoreload
# %autoreload 2

import logging

import core.config as cconfig
import core.dataflow_model.model_evaluator as modeval
import core.dataflow_model.model_plotter as modplot
import helpers.dbg as dbg
import helpers.printing as hprint

# %%
dbg.init_logger(verbosity=logging.INFO)
# dbg.init_logger(verbosity=logging.DEBUG)

_LOG = logging.getLogger(__name__)

# _LOG.info("%s", env.get_system_signature()[0])

hprint.config_notebook()

# %%
# file_name = "/app/oos_experiment.RH2Eg.v2_0-top10.5T.run1_test/result_1/result_bundle.v2_0.pkl"

# import helpers.pickle_ as hpickle

# obj = hpickle.from_pickle(file_name)

# %%
# obj.keys()

# %%
# import helpers.introspection as hintro

# for k in obj.keys():
#     print(k, hintro.get_size_in_bytes(obj[k]))

# %%
# obj["payload"]["fit_result_bundle"]["result_df"]

# %% [markdown]
# # Notebook config

# %%
# Read from env var.
eval_config = cconfig.Config.from_env_var("AM_CONFIG_CODE")

# Override config.
if eval_config is None:
    # experiment_dir = "s3://eglp-spm-sasm/experiments/experiment.RH2Ef.v1_9-all.5T.20210831-004747.run1.tgz"
    #experiment_dir = "/cache/experiments/oos_experiment.RH1E.v2_0-top100.5T"
    experiment_dir = "/cache/experiments/experiment.RH4E.v2_0-top10.5T"
    aws_profile = None
    selected_idxs = None

    eval_config = cconfig.get_config_from_nested_dict(
        {
            "load_experiment_kwargs": {
                "src_dir": experiment_dir,
                "file_name": "result_bundle.v2_0.pkl",
                "experiment_type": "ins_oos",
                "selected_idxs": selected_idxs,
                "aws_profile": aws_profile,
            },
            "model_evaluator_kwargs": {
                "predictions_col": "mid_ret_0_vol_adj_clipped_2_hat",
                "target_col": "mid_ret_0_vol_adj_clipped_2",
                # "oos_start": "2017-01-01",
                "oos_start": None,
                "abort_on_error": True,
            },
            "bh_adj_threshold": 0.1,
            "resample_rule": "W",
            "mode": "ins",
            "target_volatility": 0.1,
        }
    )

print(str(eval_config))

# %% [markdown]
# # Initialize ModelEvaluator and ModelPlotter

# %%
# Build the ModelEvaluator from the eval config.
evaluator = modeval.ModelEvaluator.from_eval_config(eval_config)

# Build the ModelPlotter.
plotter = modplot.ModelPlotter(evaluator)

# %%
evaluator._data

# %% [markdown]
# # Analysis

# %%
pnl_stats = evaluator.calculate_stats(
    mode=eval_config["mode"], target_volatility=eval_config["target_volatility"]
)
display(pnl_stats)

# %% [markdown]
# ## Model selection

# %%
plotter.plot_multiple_tests_adjustment(
    threshold=eval_config["bh_adj_threshold"], mode=eval_config["mode"]
)

# %%
# TODO(gp): Move this chunk of code in a function.
col_mask = (
    pnl_stats.loc["signal_quality"].loc["sr.adj_pval"]
    < eval_config["bh_adj_threshold"]
)
selected = pnl_stats.loc[:, col_mask].columns.to_list()
not_selected = pnl_stats.loc[:, ~col_mask].columns.to_list()

print("num model selected=%s" % hprint.perc(len(selected), pnl_stats.shape[1]))
print("model selected=%s" % selected)
print("model not selected=%s" % not_selected)

# Use `selected = None` to show all the models.

# %%
# selected = None
plotter.plot_multiple_pnls(
    keys=selected,
    resample_rule=eval_config["resample_rule"],
    mode=eval_config["mode"],
)

# %% [markdown]
# ## Return correlation

# %%
plotter.plot_correlation_matrix(
    series="returns",
    resample_rule=eval_config["resample_rule"],
    mode=eval_config["mode"],
)

# %%
plotter.plot_effective_correlation_rank(
    series="returns",
    resample_rule=eval_config["resample_rule"],
    mode=eval_config["mode"],
)

# %% [markdown]
# ## Model correlation

# %%
plotter.plot_correlation_matrix(
    series="pnl",
    resample_rule=eval_config["resample_rule"],
    mode=eval_config["mode"],
)

# %%
plotter.plot_effective_correlation_rank(
    series="pnl",
    resample_rule=eval_config["resample_rule"],
    mode=eval_config["mode"],
)

# %% [markdown]
# ## Aggregate model

# %%
pnl_srs, pos_srs, aggregate_stats = evaluator.aggregate_models(
    keys=selected,
    mode=eval_config["mode"],
    target_volatility=eval_config["target_volatility"],
)
display(aggregate_stats)

# %%
plotter.plot_sharpe_ratio_panel(keys=selected, mode=eval_config["mode"])

# %%
plotter.plot_rets_signal_analysis(
    keys=selected,
    resample_rule=eval_config["resample_rule"],
    mode=eval_config["mode"],
    target_volatility=eval_config["target_volatility"],
)

# %%
plotter.plot_performance(
    keys=selected,
    resample_rule=eval_config["resample_rule"],
    mode=eval_config["mode"],
    target_volatility=eval_config["target_volatility"],
)

# %%
plotter.plot_rets_and_vol(
    keys=selected,
    resample_rule=eval_config["resample_rule"],
    mode=eval_config["mode"],
    target_volatility=eval_config["target_volatility"],
)

# %%
if False:
    plotter.plot_positions(
        keys=selected,
        mode=eval_config["mode"],
        target_volatility=eval_config["target_volatility"],
    )

# %%
if False:
    # Plot the returns and prediction for one or more models.
    model_key = selected[:1]
    plotter.plot_returns_and_predictions(
        keys=model_key,
        resample_rule=eval_config["resample_rule"],
        mode=eval_config["mode"],
    )
