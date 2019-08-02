# Graphical tools for time and frequency analysis.
#
# Some use cases include:
#   - Determining whether a predictor exhibits autocorrelation
#   - Determining the characteristic time scale of a signal
#   - Signal filtering
#   - Lag determination
#   - etc.
import logging

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pywt
import scipy as sp
import statsmodels.api as sm

_LOG = logging.getLogger(__name__)


#
# Single-signal functions
#
def plot_autocorrelation(signal, lags=40):
    """
    Plot autocorrelation and partial autocorrelation of series.
    """
    fig = plt.figure(figsize=(12, 8))
    ax1 = fig.add_subplot(211)
    fig = sm.graphics.tsa.plot_acf(signal, lags=lags, ax=ax1)
    ax2 = fig.add_subplot(212)
    fig = sm.graphics.tsa.plot_pacf(signal, lags=lags, ax=ax2)


def plot_power_spectral_density(signal):
    """
    Estimates the power spectral density using Welch's method.

    Related to autocorrelation via the Fourier transform (Wiener-Khinchin).
    """
    freqs, psd = sp.signal.welch(signal)
    plt.figure(figsize=(5, 4))
    plt.semilogx(freqs, psd)
    plt.title('PSD: power spectral density')
    plt.xlabel('Frequency')
    plt.ylabel('Power')
    plt.tight_layout()


def plot_spectrogram(signal):
    """
    Plot spectrogram of signal.

    From the scipy documentation of spectrogram:
        "Spectrograms can be used as a way of visualizing the change of a
         nonstationary signal's frequency content over time."
    """
    freqs, times, spectrogram = sp.signal.spectrogram(signal)
    plt.figure(figsize=(5, 4))
    plt.imshow(spectrogram, aspect='auto', cmap='hot_r', origin='lower')
    plt.title('Spectrogram')
    plt.ylabel('Frequency band')
    plt.xlabel('Time window')
    plt.tight_layout()


def plot_wavelet_levels(signal, wavelet_name, levels):
    """
    Wavelet level decomposition plot. Higher levels are smoother.

    :param signal: Series-like numerical object
    :param wavelet_name: One of the names in pywt.wavelist()
    :param levels: The number of levels to plot
    """
    fig, ax = plt.subplots(figsize=(6, 1))
    ax.set_title("Original signal")
    ax.plot(signal)
    plt.show()

    data = signal.copy()
    fig, axarr = plt.subplots(nrows=levels, ncols=2, figsize=(6, 6))
    for idx in range(levels):
        (data, coeff_d) = pywt.dwt(data, wavelet_name)
        axarr[idx, 0].plot(data, 'r')
        axarr[idx, 1].plot(coeff_d, 'g')
        axarr[idx, 0].set_ylabel("Level {}".format(idx + 1),
                                 fontsize=14,
                                 rotation=90)
        axarr[idx, 0].set_yticklabels([])
        if idx == 0:
            axarr[idx, 0].set_title("Approximation coefficients", fontsize=14)
            axarr[idx, 1].set_title("Detail coefficients", fontsize=14)
        axarr[idx, 1].set_yticklabels([])
    plt.tight_layout()
    plt.show()


def low_pass_filter(signal, wavelet_name, threshold):
    """
    Wavelet low-pass filtering using a threshold.

    Currently configured to use 'periodic' mode.
    See https://pywavelets.readthedocs.io/en/latest/regression/modes.html.
    Uses soft thresholding.

    :param signal: Signal as pd.Series
    :param wavelet_name: One of the names in pywt.wavelist()
    :param threshold: Coefficient threshold (>= passes)

    :return: Smoothed signal as pd.Series, indexed like signal.index
    """
    threshold = threshold * np.nanmax(signal)
    coeff = pywt.wavedec(signal, wavelet_name, mode="per")
    coeff[1:] = (
        pywt.threshold(i, value=threshold, mode="soft") for i in coeff[1:])
    rec = pywt.waverec(coeff, wavelet_name, mode="per")
    if rec.size > signal.size:
        rec = rec[1:]
    reconstructed_signal = pd.Series(rec, index=signal.index)
    return reconstructed_signal


def plot_low_pass(signal, wavelet_name, threshold):
    """
    Overlays signal with result of low_pass_filter()
    """
    fig, ax = plt.subplots(figsize=(12, 8))
    ax.plot(signal, color="b", alpha=0.5, label='original signal')
    rec = low_pass_filter(signal, wavelet_name, threshold)
    ax.plot(rec, 'k', label='DWT smoothing}', linewidth=2)
    ax.legend()
    ax.set_title('Removing High Frequency Noise with DWT', fontsize=18)
    ax.set_ylabel('Signal Amplitude', fontsize=16)
    ax.set_xlabel('Sample', fontsize=16)
    plt.show()


def plot_scaleogram(signal,
                    scales,
                    wavelet_name,
                    cmap=plt.cm.seismic,
                    title='Wavelet Spectrogram of signal',
                    ylabel='Period',
                    xlabel='Time'):
    """
    Plots wavelet-based spectrogram (aka scaleogram).

    A nice reference and utility for plotting can be found at
    https://github.com/alsauve/scaleogram.

    See also
    https://github.com/PyWavelets/pywt/blob/master/demo/wp_scalogram.py.

    :param signal: signal to transform
    :param scales: numpy array, e.g., np.arange(1, 128)
    :param wavelet_name: continuous wavelet, e.g., 'cmor' or 'morl'
    """
    time = np.arange(0, signal.size)
    dt = time[1] - time[0]
    [coeffs, freqs] = pywt.cwt(signal, scales, wavelet_name, dt)
    power = np.abs(coeffs)**2
    periods = 1. / freqs
    levels = [0.0625, 0.125, 0.25, 0.5, 1, 2, 4, 8]
    contour_levels = np.log2(levels)

    fig, ax = plt.subplots(figsize=(15, 10))
    im = ax.contourf(time,
                     np.log2(periods),
                     np.log2(power),
                     contour_levels,
                     extend='both',
                     cmap=cmap)

    ax.set_title(title, fontsize=20)
    ax.set_ylabel(ylabel, fontsize=18)
    ax.set_xlabel(xlabel, fontsize=18)

    yticks = 2**np.arange(np.ceil(np.log2(periods.min())),
                          np.ceil(np.log2(periods.max())))
    ax.set_yticks(np.log2(yticks))
    ax.set_yticklabels(yticks)
    ax.invert_yaxis()
    ylim = ax.get_ylim()
    ax.set_ylim(ylim[0], -1)

    cbar_ax = fig.add_axes([0.95, 0.5, 0.03, 0.25])
    fig.colorbar(im, cax=cbar_ax, orientation='vertical')
    plt.show()


def fit_random_walk_plus_noise(signal):
    """
    Fit a random walk + Gaussian noise model using state space methods.

    After convergence the resulting model is equivalent to exponential
    smoothing. Using the state space approach we can
      - Calculate the signal-to-noise ratio
      - Determine an optimal (under model assumptions) ewma com
      - Analyze residuals

    :return: SSM model and fitted result
    """
    model = sm.tsa.UnobservedComponents(signal, level='local level',
            initialization='diffuse')
    result = model.fit(method='powell', disp=True)
    # Signal-to-noise ratio
    q = result.params[1] / result.params[0]
    _LOG.info("Signal-to-noise ratio q = %f", q)
    p = 0.5 * (q + np.sqrt(q**2 + 4 * q))
    kalman_gain = p / (p + 1)
    _LOG.info("Steady-state Kalman gain = %f", kalman_gain)
    # EWMA com
    com = 1 / kalman_gain - 1
    _LOG.info("EWMA com = %f", com)
    print(result.summary())
    result.plot_diagnostics()
    result.plot_components(legend_loc='lower right', figsize=(15, 9))
    return model, result


#
# Two-signal functions (e.g., predictor and response)
#
def plot_crosscorrelation(x, y):
    """
    Assumes x, y have been approximately demeaned and normalized (e.g.,
    z-scored with ewma).

    At index `k` in the result, the value is given by
        (1 / n) * \sum_{l = 0}^{n - 1} x_l * y_{l + k}
    """
    joint_idx = x.index.intersection(y.index)
    corr = sp.signal.correlate(x.loc[joint_idx], y.loc[joint_idx])
    # Normalize by number of points in series (e.g., take expectations)
    n = joint_idx.size
    corr /= n
    step_idx = pd.RangeIndex(-1 * n + 1, n)
    pd.Series(data=corr, index=step_idx).plot()


# TODO(Paul): Add coherence plotting function.
