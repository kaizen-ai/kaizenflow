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
import core.dataflow_model.incremental_single_name_model_evaluator as ime
import core.dataflow_model.model_evaluator as modeval
import core.dataflow_model.model_plotter as modplot
import core.dataflow_model.stats_computer as csc
import core.dataflow_model.utils as cdmu
import core.plotting as cplot
import core.statistics as cstati
import helpers.dbg as dbg
import helpers.printing as hprint

# %%
dbg.init_logger(verbosity=logging.INFO)
# dbg.init_logger(verbosity=logging.DEBUG)

_LOG = logging.getLogger(__name__)

# _LOG.info("%s", env.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # Load features

# %%
feat_iter = cdmu.yield_experiment_artifacts(
    src_dir="",
    file_name="result_bundle.v2_0.pkl",
    load_rb_kwargs={},
)

# %%
key, artifact = next(feat_iter)
display("key=%s", key)
features = artifact.result_df

# %%
features.head()

# %% [markdown]
# # Cross-sectional feature analysis

# %%
cplot.plot_heatmap(features.corr(), mode="clustermap", figsize=(20, 20))

# %%
cplot.plot_effective_correlation_rank(features)

# %%
cplot.plot_projection(features.resample("B").sum(min_count=1))

# %%
sc = csc.StatsComputer()

# %%
features.apply(sc.compute_summary_stats).round(3)

# %% [markdown]
# # Single feature analysis

# %%
feature = ""

# %%
cplot.plot_qq(features[feature])

# %%
cplot.plot_histograms_and_lagged_scatterplot(
    features[feature], lag=1, figsize=(20, 20)
)

# %%
cplot.plot_time_series_by_period(
    features[feature],
    "hour",
)

# %%
