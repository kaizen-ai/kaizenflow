# Graphical tools for time and frequency analysis.
#
# Some use cases include:
#   - Determining whether a predictor exhibits autocorrelation
#   - Determining the characteristic time scale of a signal
#   - Signal filtering
#   - Lag determination
#   - etc.
import functools
import logging

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pywt
import scipy as sp
import statsmodels.api as sm
import utilities.helpers.dbg as dbg


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


#
# EMAs and derived operators
#
def _com_to_tau(com):
    return 1. / np.log(1 + 1. / com)


def _tau_to_com(tau):
    return 1. / (np.exp(1. / tau) - 1)


def iter_ema(df, com, min_periods, depth):
    """
    Iterated EMA operator (e.g., see 3.3.6 of Dacorogna, et al).

    depth=1 corresponds to a single application of exponential smoothing.

    Greater depth tempers impulse response, introducing a phase lag.

    Dacorogna use the convention $\text{ema}(t) = \exp(-t / \tau) / \tau$.

    If $s_n = \lambda x_n + (1 - \lambda) s_{n - 1}$, where $s_n$ is the ema
    output, then $1 - \lambda = \exp(-1 / \tau)$. Now
    $\lambda = 1 / (1 + \text{com})$, and rearranging gives
    $\log(1 + 1 / \text{com}) = 1 / \tau$. Expanding in a Taylor series
    leads to $\tau \approx \text{com}$. 

    The kernel for an ema of depth $n$ is
    $(1 / (n - 1)!) (t / \tau)^{n - 1} \exp^{-t / \tau} / \tau$.

    Arbitrary kernels can be approximated by a combination of iterated emas. 

    For an iterated ema with given tau and depth n, we have
      - range = n \tau
      - <t^2> = n(n + 1) \tau^2
      - width = \sqrt{n} \tau
      - aspect ratio = \sqrt{1 + 1 / n}
    """
    dbg.dassert_isinstance(depth, int)
    dbg.dassert_lte(1, depth)
    _LOG.info('Calculating iterated ema of depth %i...', depth)
    _LOG.info('com = %0.2f', com)
    tau = _com_to_tau(com) 
    _LOG.info('tau = %0.2f', tau) 
    _LOG.info('range = %0.2f', depth * tau)
    _LOG.info('<t^2>^{1/2} = %0.2f', np.sqrt(depth * (depth + 1)) * tau)
    _LOG.info('width = %0.2f', np.sqrt(depth) * tau)
    _LOG.info('aspect ratio = %0.2f', np.sqrt(1 + 1. / depth))
    df_hat = df.copy()
    for i in range(0, depth):
        df_hat = df_hat.ewm(com=com, min_periods=min_periods,
                            adjust=True, ignore_na=False, axis=0).mean()
    return df_hat


def ema_diff(df, tau, min_periods, scaling):
    """
    'Low-noise' differential operator as in 3.3.9 of Dacorogna, et al.

    Computes difference of around time "now" over a time interval \tau_1
    and an average around time "now - \tau" over a time interval \tau_2.
    Here, \tau_1, \tau_2 are taken to be approximately \tau / 2.

    The normalization is chosen so that the differential of a constant is zero
    and so that the differential of 't' is approximately \tau.
    """
    _LOG.info('Calculating ema diff with tau = %0.2f...', tau)
    gamma = 1.22208
    beta = 0.65
    alpha = 1. / (gamma * (8 * beta - 3))
    _LOG.info('alpha = %0.2f', alpha)
    tau1 = alpha * tau 
    _LOG.info('tau1 = %0.2f', tau1)
    tau2 = alpha * beta * tau
    _LOG.info('tau2 = %0.2f', tau2)
    com1 = _tau_to_com(tau1)
    _LOG.info('com1 = %0.2f', com1)
    com2 = _tau_to_com(tau2)
    _LOG.info('com2 = %0.2f', com2)
    s1 = iter_ema(df, com1, min_periods, 1)
    s2 = iter_ema(df, com1, min_periods, 2)
    s3 = -2. * iter_ema(df, com2, min_periods, 4)
    differential = gamma * (s1 + s2 + s3)
    if scaling == 0:
        return differential
    return differential / (tau ** scaling)


def smooth_ma(df, range_, min_periods, min_depth, max_depth): 
    """
    Moving average operator defined in terms of iterated ema's.
    Choosing min_depth > 1 results in a lagged operator.
    Choosing min_depth = max_depth = 1 reduces to a single ema with
    com approximately equal to range_.

    Abrupt impulse response that tapers off smoothly like a sigmoid
    (hence smoother than an equally-weighted moving average).
    
    For min_depth = 1 and large max_depth, the series is approximately
    constant for t << 2 * range_. In particular, when max_depth >= 5,
    the kernels are more rectangular than ema-like.
    """
    dbg.dassert_isinstance(min_depth, int)
    dbg.dassert_isinstance(max_depth, int)
    dbg.dassert_lte(1, min_depth)
    dbg.dassert_lte(min_depth, max_depth)
    _LOG.info('Calculating smoothed moving average...')
    tau_prime = 2. * range_ / (min_depth + max_depth)
    _LOG.info('ema tau = %0.2f', tau_prime)
    com = _tau_to_com(tau_prime) 
    _LOG.info('com = %0.2f', com) 
    ema = functools.partial(iter_ema, df, com, min_periods)
    denom = float(max_depth - min_depth + 1)
    # Not the most efficient implementation, but follows 3.56 of Dacorogna
    # directly.
    return sum(map(ema, range(min_depth, max_depth + 1))) / denom


def ma_norm(df, range_, min_periods, min_depth, max_depth, p_moment):
    df_tmp = np.abs(df)**p_moment
    return smooth_ma(df_tmp, range_, min_periods, min_depth,
                     max_depth)**(1. / p_moment)


def ma_var(df, range_, min_periods, min_depth, max_depth, p_moment):
    df_ma = smooth_ma(df, range_, min_periods, min_depth, max_depth) 
    df_tmp = np.abs(df - df_ma)**p_moment 
    return smooth_ma(df_tmp, range_, min_periods, min_depth, max_depth)


def ma_std(df, range_, min_periods, min_depth, max_depth, p_moment):
    df_tmp = ma_var(df, range_, min_periods, min_depth, max_depth, p_moment)
    return df_tmp ** (1. / p_moment)


def z_score(df, range_, min_periods, min_depth, max_depth, p_moment):
    df_hat = df - smooth_ma(df, range_, min_periods, min_depth, max_depth)
    df_hat /= ma_std(df, range_, min_periods, min_depth, max_depth, p_moment)
    return df_hat


def get_heaviside(a, b, zero_loc, tick):
    array = np.arange(a, b, tick)
    srs = pd.Series(data=np.heaviside(array, zero_loc),
                    index=array,
                    name='Heaviside')
    return srs


def get_impulse(a, b, zero_loc, tick):
    heavi = get_heaviside(a, b, zero_loc, tick)
    impulse = (heavi - heavi.shift(1)).shift(-1).fillna(0)
    impulse.name = 'impulse'
    return impulse
