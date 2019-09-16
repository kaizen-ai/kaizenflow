# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.2'
#       jupytext_version: 1.2.1
#   kernelspec:
#     display_name: Python [conda env:develop] *
#     language: python
#     name: conda-env-develop-py
# ---

# %% [markdown]
# ## Import

# %%
# %load_ext autoreload
# %autoreload 2
# import datetime
import logging

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

import core.explore as exp
import core.features_analyzer as ana
import core.finance as fin
import helpers.config as cfg
import helpers.dbg as dbg
import helpers.env as env
import helpers.printing as pri
import vendors.particle_one.utils as put

# %%
print(env.get_system_signature())

pri.config_notebook()

# TODO(gp): Changing level during the notebook execution doesn't work. Fix it.
# dbg.init_logger(verb=logging.DEBUG)
dbg.init_logger(verb=logging.INFO)
# dbg.test_logger()

_LOG = logging.getLogger(__name__)

# %% [markdown]
# # Config

# %%
config = {
    "feature_file_name": "/Users/saggese/GoogleDrive/alphamatic/Particle/Tech/twitter_dataset_sentiment_07.05.19.csv",
    #
    "feat_zscore_com": 28,
    #
}

if True:
    config.update(
        {
            "agg_interval": "1T",
            # "agg_interval": "5T",
            "agg_function": "mean",
            #
            "rets_file_name": "/Users/saggese/src/lemonade/vendors/particle_one/oil_1min_zrets.csv",
        }
    )
else:
    config.update(
        {
            # TODO(gp): weekday
            "agg_interval": "1B",
            "agg_function": "mean",
            #
            "rets_file_name": "/Users/saggese/src/lemonade/vendors/particle_one/oil_daily_zrets.csv",
        }
    )

_LOG.info("config=\n%s", cfg.config_to_string(config))

# %% [markdown]
# # Read data

# %%
feat_data = put.read_data_from_config(config)

_LOG.info("feat_data=\n%s", feat_data.head(3))

# %% [markdown]
# ## Plot time distribution

# %%
mode = "time_of_the_day"
exp.plot_time_distributions(feat_data.index, mode)
plt.show()
exp.plot_time_distributions(feat_data.index, mode, density=False)

# %%
mode = "minute_of_the_hour"
exp.plot_time_distributions(feat_data.index, mode)

# %%
mode = "weekday"
exp.plot_time_distributions(feat_data.index, mode)

# %%
# mode = "day_of_the_month"
mode = "month_of_the_year"
exp.plot_time_distributions(feat_data.index, mode)

# %%
# mode = "day_of_the_month"
mode = "year"
exp.plot_time_distributions(feat_data.index, mode, density=False)

# %% [markdown]
# ## Plot distribution.

# %%
put.plot_raw_data_pdf(feat_data, put.get_raw_features())

# %%
exp.plot_heatmap(feat_data.corr(), "heatmap", annot=True, vmin=-1, vmax=1.0)

# %% [markdown]
# ## Compute features

# %%
# Resample to 1 minute
# - sum()
# - mean()

# Build signal
# - test each component by itself
# - test difference of each component D+ - D-
# - test (S+ - S-) - (D+ - D-) + (I+ - I-)

# %%
feat_data, new_features = put.compute_features_from_config(config, feat_data)

all_features = put.get_raw_features() + new_features
_LOG.info("all_features=%s", all_features)
_LOG.info("feat_data=\n%s", feat_data.head(3))

# %%
feat_names = "demand inventory supply".split()
exp.plot_heatmap(
    feat_data[feat_names].corr(), "heatmap", annot=True, vmin=-1, vmax=1.0
)

# %%
# Count majority1 and majority2.

# %% [markdown]
# ## Sample

# %%
feat_data_sampled = put.sample_features_from_config(config, feat_data)

feat_data_sampled.head(2)

# %%
# feat_names = put.get_raw_features() + "demand inventory supply".split()
feat_names = "demand inventory supply".split()
put.plot_raw_data_pdf(feat_data_sampled, feat_names)

# %%
feat_data_sampled[["demand"]].resample("1D").sum().plot()
plt.show()

fin.zscore(
    feat_data_sampled["demand"],
    com=config["feat_zscore_com"],
    demean=True,
    standardize=True,
    delay=1,
).resample("1D").sum().plot()

# %% [markdown]
# # Read returns

# %%
_LOG.info("Reading csv '%s'", config["rets_file_name"])
rets = pd.read_csv(config["rets_file_name"], index_col=0, parse_dates=True)

exp.display_df(rets)

# %%
exp.plot_non_na_cols(rets.resample("1D").sum())

# %%
# Resample to 1min.
_LOG.info("## Before resampling")
exp.report_zero_nan_inf_stats(rets)

# %%
rets = fin.resample_1min(rets)

_LOG.info("## After resampling")
exp.report_zero_nan_inf_stats(rets_tmp)

rets.fillna(0.0, inplace=True)

# %%
rets.resample("1B").sum().cumsum().plot()

# %%
annot = True
stocks_corr = rets.corr()

sns.clustermap(stocks_corr, annot=annot)

# %% [markdown]
# # Analysis

# %%
display(rets.dropna().head(3))
display(feat_data_sampled.dropna().head(3))

# %%
all_df = pd.concat([rets, feat_data_sampled], axis=1, sort=True)

if config["feat_zscore_com"] is not None:
    _LOG.info("z-scoring")
    cols = [c for c in all_df.columns if not c.endswith("_ret_0")]
    _LOG.info("cols=%s", cols)
    all_df[cols] = fin.zscore(
        all_df[cols],
        com=config["feat_zscore_com"],
        demean=True,
        standardize=True,
        delay=1,
    )

all_df.dropna().tail()

# %%
y_var = "CL_ret_0"
# y_var = "NG_ret_0"
x_vars = "demand inventory supply".split()
# use_intercept = True
use_intercept = False
nan_mode = "drop"
# x_shifts = [0]
x_shifts = [-5, -3, -2, -1, 0, 1, 2, 3, 5]
# x_shifts = [-5, -3, -2, -1, 0]
res_df = ana.analyze_features(
    all_df, y_var, x_vars, use_intercept, nan_mode=nan_mode, x_shifts=x_shifts
)
display(res_df)

# %%
ar = ana.Reporter(res_df)
ar.plot()

# %%
if False:
    y_var = "CL_ret_0"
    y_var = "NG_ret_0"
    x_vars = "demand"
    # use_intercept = True
    use_intercept = False
    nan_mode = "drop"
    x_shift = 1
    report_stats = True
    res_df = ana._analyze_feature(
        all_df, y_var, x_var, use_intercept, nan_mode, x_shift, report_stats
    )
    display(res_df)
