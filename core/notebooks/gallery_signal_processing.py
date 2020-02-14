# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.2'
#       jupytext_version: 1.2.4
#   kernelspec:
#     display_name: Python [conda env:.conda-p1_develop] *
#     language: python
#     name: conda-env-.conda-p1_develop-py
# ---

# %% [markdown]
# ## Import

# %%
# %load_ext autoreload
# %autoreload 2
# %matplotlib inline

import collections
import pprint

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import core.artificial_signal_generators as sig_gen
import core.signal_processing as sigp

# %% [markdown]
# # Generate signal

# %%
prices = sig_gen.get_gaussian_walk(0, 0.01, 4 * 252, seed=20)

# %%
prices.plot()

# %%
rets = (np.log(prices) - np.log(prices.shift(1))).dropna()

# %%
rets.plot()

# %%
# Data for example
x = np.linspace(0, 1, num=2048)
chirp_signal = np.sin(250 * np.pi * x ** 2)

# %%
pd.Series(chirp_signal).plot()

# %% [markdown]
# # Time domain tools

# %%
sigp.plot_autocorrelation(chirp_signal)

# %%
sigp.plot_autocorrelation(rets)

# %% [markdown]
# # Frequency domain tools

# %%
sigp.plot_power_spectral_density(chirp_signal)

# %%
sigp.plot_power_spectral_density(rets)

# %%
sigp.plot_spectrogram(chirp_signal)

# %%
sigp.plot_spectrogram(rets)

# %% [markdown]
# # Multiresolution analysis tools

# %%
sigp.plot_wavelet_levels(chirp_signal, "sym5", 5)

# %%
sigp.plot_wavelet_levels(prices, "db5", 5)

# %%
sigp.plot_low_pass(pd.Series(chirp_signal), "db8", 2)

# %%
sigp.plot_low_pass(prices, "db8", 1)

# %%
sigp.plot_low_pass(rets, "db8", 0.2)

# %%
sigp.plot_scaleogram(prices, np.arange(1, 1024), "morl")

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
stats = collections.OrderedDict()
srs_out = sigp.process_outliers(srs, mode, lower_quantile, info=stats)
#
_analyze(srs_out)

# %%
mode = "winsorize"
lower_quantile = 0.01
upper_quantile = 0.90
stats = collections.OrderedDict()
srs_out = sigp.process_outliers(
    srs, mode, lower_quantile, upper_quantile=upper_quantile, info=stats
)
#
_analyze(srs_out)

# %%
mode = "set_to_nan"
lower_quantile = 0.01
stats = collections.OrderedDict()
srs_out = sigp.process_outliers(srs, mode, lower_quantile, info=stats)
#
_analyze(srs_out)

# %%
mode = "set_to_zero"
lower_quantile = 0.10
stats = collections.OrderedDict()
srs_out = sigp.process_outliers(srs, mode, lower_quantile, info=stats)
#
_analyze(srs_out)
