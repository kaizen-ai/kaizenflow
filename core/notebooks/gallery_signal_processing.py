# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.4.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
# ## Import

# %%
# %load_ext autoreload
# %autoreload 2
# %matplotlib inline

import collections
import logging
import pprint

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import core.artificial_signal_generators as sig_gen
import core.plotting as plot
import core.signal_processing as sigp
import core.statistics as stats
import helpers.dbg as dbg
import helpers.env as env
import helpers.printing as prnt

# %%
dbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", env.get_system_signature()[0])

prnt.config_notebook()

# %% [markdown]
# # Generate signal

# %%
arma00process = sig_gen.ArmaProcess([], [])

# %%
rets = arma00process.generate_sample(
    {"start": "2000-01-01", "periods": 4 * 252, "freq": "B"},
    scale=5,
    burnin=20,
    seed=42,
)

# %%
price = rets.cumsum()

# %%
rets.name += "_rets"
price.name += "_price"

# %% [markdown]
# ## Price

# %%
plot.plot_cols(price)

# %%
price_decomp = sigp.get_trend_residual_decomp(price, tau=16)

# %%
price_decomp.head(3)

# %%
plot.plot_cols(price_decomp)

# %% [markdown]
# ### Price wavelet decomposition

# %%
price_smooth, price_detail = sigp.get_swt(price, wavelet="haar")

# %%
plot.plot_cols(price_detail)

# %%
plot.plot_cols(price_smooth)

# %%
plot.plot_correlation_matrix(price_detail, mode="heatmap")

# %% [markdown]
# ## Returns

# %%
plot.plot_cols(rets)

# %%
stats.apply_normality_test(rets.to_frame())

# %%
plot.plot_autocorrelation(rets)

# %%
plot.plot_spectrum(rets)

# %% [markdown]
# ### Returns wavelet decomposition

# %%
rets_smooth, rets_detail = sigp.get_swt(rets, "haar")

# %%
plot.plot_cols(rets_detail)

# %%
plot.plot_cols(rets_detail, mode="renormalize")

# %%
stats.apply_normality_test(rets_detail)

# %%
plot.plot_autocorrelation(rets_detail, title_prefix="Wavelet level ")

# %%
plot.plot_spectrum(rets_detail, title_prefix="Wavelet level ")

# %%
plot.plot_correlation_matrix(rets_detail, mode="heatmap")

# %% [markdown]
# ### Z-scored returns

# %%
zscored_rets = sigp.get_dyadic_zscored(rets, demean=False)

# %%
plot.plot_cols(zscored_rets)

# %%
stats.apply_normality_test(zscored_rets)

# %%
plot.plot_autocorrelation(zscored_rets, title_prefix="tau exp = ")

# %%
plot.plot_spectrum(zscored_rets, title_prefix="tau exp = ")

# %%

# %% [markdown]
# # EMAs

# %%
impulse = sig_gen.get_impulse(-252, 3 * 252, tick=1)

# %%
impulse.plot()

# %%
for i in range(1, 6):
    sigp.compute_ema(impulse, tau=40, min_periods=20, depth=i).plot()

# %%
for i in range(1, 6):
    sigp.compute_smooth_moving_average(
        impulse, tau=40, min_periods=20, min_depth=1, max_depth=i
    ).plot()

# %%
for i in range(1, 6):
    sigp.compute_smooth_moving_average(
        impulse, tau=40, min_periods=20, min_depth=i, max_depth=5
    ).plot()

# %%
for i in range(1, 6):
    sigp.compute_rolling_norm(
        impulse, tau=40, min_periods=20, min_depth=1, max_depth=i, p_moment=1
    ).plot()

# %%
for i in np.arange(0.5, 4.5, 0.5):
    sigp.compute_rolling_norm(
        impulse, tau=40, min_periods=20, min_depth=1, max_depth=2, p_moment=i
    ).plot()

# %% [markdown]
# # Outliers handling

# %%
np.random.seed(100)
n = 100000
data = np.random.normal(loc=0.0, scale=1.0, size=n)
print(data[:5])

srs = pd.Series(data)
srs.plot(kind="hist")


# %%
def _analyze(srs):
    print(np.isnan(srs).sum())
    srs.plot(kind="hist")
    plt.show()
    pprint.pprint(stats)


# %%
mode = "winsorize"
lower_quantile = 0.01
window = 1000
min_periods = 10
stats = collections.OrderedDict()
srs_out = sigp.process_outliers(
    srs, mode, lower_quantile, window=window, min_periods=min_periods, info=stats
)
#
_analyze(srs_out)

# %%
mode = "winsorize"
lower_quantile = 0.01
upper_quantile = 0.90
window = 1000
min_periods = 10
stats = collections.OrderedDict()
srs_out = sigp.process_outliers(
    srs,
    mode,
    lower_quantile,
    upper_quantile=upper_quantile,
    window=window,
    min_periods=min_periods,
    info=stats,
)
#
_analyze(srs_out)

# %%
mode = "set_to_nan"
lower_quantile = 0.01
window = 1000
min_periods = 10
stats = collections.OrderedDict()
srs_out = sigp.process_outliers(
    srs, mode, lower_quantile, window=window, min_periods=min_periods, info=stats
)
#
_analyze(srs_out)

# %%
mode = "set_to_zero"
lower_quantile = 0.10
window = 1000
min_periods = 10
stats = collections.OrderedDict()
srs_out = sigp.process_outliers(
    srs, mode, lower_quantile, window=window, min_periods=min_periods, info=stats
)
#
_analyze(srs_out)
