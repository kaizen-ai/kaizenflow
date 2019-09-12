import logging

import pymc3 as pm

_LOG = logging.getLogger(__name__)


def best(y1, y2, prior_tau=1e-6, **kwargs):
    """
    Bayesian Estimation Supersedes the T Test.
    See http://www.indiana.edu/~kruschke/BEST/BEST.pdf

    This is a generic t-test replacement.
    :param **kwargs:
    """
    with pm.Model() as model:
        # We set the mean of the prior to 0 because
        #   1. In the common use case of returns, `0` is a reasonable prior
        #      value.
        #   2. We typically expect the precision to be set very low so that the
        #      prior is very vague. The center is not so important provided the
        #      prior is sufficiently vague (and the sample sufficiently large).
        group1_mean = pm.Normal(
            "group1_mean", mu=0, tau=prior_tau, testval=y1.mean()
        )
        group2_mean = pm.Normal(
            "group2_mean", mu=0, tau=prior_tau, testval=y2.mean()
        )
        # BDA3 suggests log std ~ Uniform as a diffuse (improper) prior
        # (See Sec. 2.8).
        # Here, we opt for the "super-vague" but proper analogue by default.
        # (see https://github.com/stan-dev/stan/wiki/Prior-Choice-Recommendations)
        group1_log_std = pm.Normal(
            "group1_log_std", mu=0, tau=prior_tau, testval=pm.math.log(y1.std())
        )
        group2_log_std = pm.Normal(
            "group2_log_std", mu=0, tau=prior_tau, testval=pm.math.log(y2.std())
        )
        # Convert from log_std to std
        group1_std = pm.Deterministic("group1_std", pm.math.exp(group1_log_std))
        group2_std = pm.Deterministic("group2_std", pm.math.exp(group2_log_std))
        # Shape parameter for T-distribution
        # Require nu > 2 so that the second moment exists (for std_dev).
        # TODO(Paul): Justify prior parameter value 1/29.
        nu = pm.Exponential("nu_minus_2", 1 / 29.0, testval=4.0) + 2.0
        # Response distributions
        resp_group1 = pm.StudentT(
            "group1", nu=nu, mu=group1_mean, lam=group1_std ** -2, observed=y1
        )
        resp_group2 = pm.StudentT(
            "group2", nu=nu, mu=group2_mean, lam=group2_std ** -2, observed=y2
        )
        # T-test-like quantities
        diff_of_means = pm.Deterministic(
            "difference of means", group2_mean - group1_mean
        )
        pm.Deterministic("difference of stds", group2_std - group1_std)
        pm.Deterministic(
            "effect size",
            diff_of_means
            / pm.math.sqrt(0.5 * (group1_std ** 2 + group2_std ** 2)),
        )
        # SNR (signal-to-noise ratio, i.e., Sharpe ratio, but without
        # annualization)
        group1_vol = pm.Deterministic(
            "group1_vol", resp_group1.distribution.variance ** 0.5
        )
        group2_vol = pm.Deterministic(
            "group2_vol", resp_group2.distribution.variance ** 0.5
        )
        group1_snr = pm.Deterministic("group1_snr", resp_group1.mean / group1_vol)
        group2_snr = pm.Deterministic("group2_snr", resp_group2.mean / group2_vol)
        pm.Deterministic("difference of snrs", group2_snr - group1_snr)
        # MCMC
        trace = pm.sample(**kwargs)
    return model, trace


def fit_t_distribution(y, prior_tau=1e-6, **kwargs):
    """
    Fits a t-distribution to y.

    :param y: Data to be modeled by a t-distribution
    :param prior_tau: Controls precision of mean and log_std priors
    :param samples: Number of MCMC samples
    :return: PyMC3 model and trace
    """
    with pm.Model() as model:
        mean = pm.Normal("mean", mu=0, tau=prior_tau, testval=y.mean())
        log_std = pm.Normal(
            "log_std", mu=0, tau=prior_tau, testval=pm.math.log(y.std())
        )
        std = pm.Deterministic("std", pm.math.exp(log_std))
        nu = pm.Exponential("nu_minus_2", 1 / 10.0, testval=3.0)
        returns = pm.StudentT(
            "returns", nu=nu + 2, mu=mean, sigma=std, observed=y
        )
        vol = pm.Deterministic("vol", returns.distribution.variance ** 0.5)
        pm.Deterministic("snr", returns.distribution.mean / vol)
        trace = pm.sample(**kwargs)
    return model, trace


def trace_info(trace, **kwargs):
    """
    Standard trace plots and info.
    """
    _LOG.info("traceplot...")
    pm.traceplot(trace, **kwargs)
    _LOG.info("plot_posterior...")
    pm.plot_posterior(trace, ref_val=0, **kwargs)
    _LOG.info("forestplot...")
    pm.forestplot(trace, **kwargs)
    _LOG.info("summary...")
    print(pm.summary(trace, **kwargs))
