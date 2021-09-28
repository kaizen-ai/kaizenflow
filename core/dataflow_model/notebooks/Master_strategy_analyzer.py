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

# %% [markdown]
# - Initialize with returns, alpha, and spread
# - Evaluate portfolios generated from the alpha
#
# - TODO(gp): This should be called `Master_strategy_evaluator` like the class

# %% [markdown]
# # Imports

# %%
# %load_ext autoreload
# %autoreload 2

import logging

import core.config as cconfig
import core.dataflow_model.model_evaluator as modeval
import core.dataflow_model.utils as cdmu
import helpers.dbg as dbg
import helpers.printing as hprint

# %%
dbg.init_logger(verbosity=logging.INFO)
# dbg.init_logger(verbosity=logging.DEBUG)

_LOG = logging.getLogger(__name__)

# _LOG.info("%s", env.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # Notebook config

# %%
# Read from env var.
eval_config = cconfig.Config.from_env_var("AM_CONFIG_CODE")

if eval_config is None:
    experiment_dir = ""
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
            "strategy_evaluator_kwargs": {
                "returns_col": "mid_ret_0",
                "position_intent_col": "position_intent_1",
                "spread_col": "spread",
                "abort_on_error": True,
            },
            "bh_adj_threshold": 0.1,
            "resample_rule": "W",
        }
    )

print(str(eval_config))

# %%
# Build the ModelEvaluator from the eval config.
evaluator = modeval.StrategyEvaluator.from_eval_config(eval_config)

# %%
# TODO(gp): Finish this.
keys = None
evaluator.compute_stats(keys)
