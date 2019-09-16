import logging

import numpy as np
import pymc3 as pm
import theano

# See https://stackoverflow.com/questions/51238578/error-non-constant-expression-cannot-be-narrowed-from-type-npy-intp-to-int
theano.config.gcc.cxxflags = "-Wno-c++11-narrowing"

_LOG = logging.getLogger(__name__)


DOF = "dof"
LOC = "location"
RET = "returns"
SCALE = "scale"
SHAPE = "shape"
SR = "sharpe"
VOL = "volatility"


# TODO(Paul): Use config-style constants for defining strings used by models.
# TODO(Paul): Consider factoring out common code


def best(y1, y2, prior_tau=1e-6, time_scaling=1, **kwargs):
    """
    Bayesian Estimation Supersedes the T Test.
    See http://www.indiana.edu/~kruschke/BEST/BEST.pdf

    This is a generic t-test replacement.
    :param **kwargs: Passed to pm.sample.
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
        # Sec. 5.7 of BDA suggests Half-Cauchy as a good choice, at least in
        # cases where a small number of groups (in the context of hierarchical
        # models) are used.
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
        # Vol in sampling units, unless adjusted.
        group1_vol = pm.Deterministic(
            "group1_volatilty",
            np.sqrt(time_scaling) * resp_group1.distribution.variance ** 0.5,
        )
        group2_vol = pm.Deterministic(
            "group2_volatility",
            np.sqrt(time_scaling) * resp_group2.distribution.variance ** 0.5,
        )
        # Sharpe ratio in sampling units, unless adjusted.
        group1_sr = pm.Deterministic(
            "group1_sharpe", time_scaling * resp_group1.mean / group1_vol
        )
        group2_sr = pm.Deterministic(
            "group2_sharpe", time_scaling * resp_group2.mean / group2_vol
        )
        pm.Deterministic("difference of Sharpe ratios", group2_sr - group1_sr)
        # MCMC
        trace = pm.sample(**kwargs)
    return model, trace


def fit_laplace_distribution(y, prior_tau=1e-6, time_scaling=1, **kwargs):
    """
    Fits a Laplace distribution to y.

    :param y: Data to be modeled by a Laplace distribution
    :param prior_tau: Controls precision of mean and log_std priors
    :param time_scaling: Scaling parameter for volatility and Sharpe.
        E.g. If the time sampling units (in the input) are days and volatility
        and Sharpe are to be reported in annualized units, use
        time_scaling = 252.
    :param kwargs: Passed to pm.sample
    :return: PyMC3 model and trace
    """
    with pm.Model() as model:
        loc = pm.Normal(LOC, mu=0, tau=prior_tau, testval=y.mean())
        log_scale = pm.Normal(
            "log_scale", mu=0, tau=prior_tau, testval=pm.math.log(y.std())
        )
        scale = pm.Deterministic(SCALE, pm.math.exp(log_scale))
        returns = pm.Laplace("returns", mu=loc, b=scale, observed=y)
        vol = pm.Deterministic(
            "volatility",
            np.sqrt(time_scaling) * returns.distribution.variance ** 0.5,
        )
        pm.Deterministic("sharpe", time_scaling * returns.distribution.mean / vol)
        trace = pm.sample(**kwargs)
    return model, trace


def fit_normal_distribution(y, prior_tau=1e-6, time_scaling=1, **kwargs):
    """
    Fits a normal distribution to y.

    TODO(Paul): Generalize this to multiple independent streams.

    :param y: Data to be modeled by a normal distribution
    :param prior_tau: Controls precision of mean and log_std priors
    :param samples: Number of MCMC samples
    :param kwargs: Passed to pm.sample
    :return: PyMC3 model and trace
    """
    with pm.Model() as model:
        mean = pm.Normal(LOC, mu=0, tau=prior_tau, testval=y.mean())
        # TODO(Paul): Compare with replacing these priors on sigma with
        # sigma = pm.HalfCauchy("std", beta=1, testval=y.std())
        log_std = pm.Normal(
            "log_std", mu=0, tau=prior_tau, testval=pm.math.log(y.std())
        )
        std = pm.Deterministic(SCALE, pm.math.exp(log_std))
        returns = pm.Normal(RET, mu=mean, sigma=std, observed=y)
        vol = pm.Deterministic(
            VOL, np.sqrt(time_scaling) * returns.distribution.variance ** 0.5
        )
        pm.Deterministic(SR, time_scaling * returns.distribution.mean / vol)
        trace = pm.sample(**kwargs)
    return model, trace


def fit_t_distribution(y, prior_tau=1e-6, time_scaling=1, **kwargs):
    """
    Fits a t-distribution to y.

    :param y: Data to be modeled by a t-distribution
    :param prior_tau: Controls precision of mean and log_std priors
    :param samples: Number of MCMC samples
    :param kwargs: Passed to pm.sample
    :return: PyMC3 model and trace
    """
    with pm.Model() as model:
        loc = pm.Normal(LOC, mu=0, tau=prior_tau, testval=y.mean())
        # TODO(Paul): Compare with replacing these priors on sigma with
        # std = pm.HalfCauchy("std", beta=1, testval=y.std())
        log_std = pm.Normal(
            "log_std", mu=0, tau=prior_tau, testval=pm.math.log(y.std())
        )
        scale = pm.Deterministic(SCALE, pm.math.exp(log_std))
        nu_shft = pm.Exponential(DOF + "_minus_2", 1 / 10.0, testval=3.0)
        dof = pm.Deterministic(DOF, nu_shft + 2)
        returns = pm.StudentT(RET, nu=dof, mu=loc, sigma=scale, observed=y)
        vol = pm.Deterministic(
            VOL, np.sqrt(time_scaling) * returns.distribution.variance ** 0.5
        )
        pm.Deterministic(SR, time_scaling * returns.distribution.mean / vol)
        trace = pm.sample(**kwargs)
    return model, trace


def fit_one_way_normal(df, prior_tau=1e-6, time_scaling=1, **kwargs):
    """
    Fits a one-way normal model.

    :param df: Df of obs, each column representing a group
    :param prior_tau: Controls precision of global mean prior
    :param sigma: Shared sigma of groups
    :param kwargs: Passed to pm.sample
    :return: PyMC3 model and trace
    """
    with pm.Model() as model:
        global_mean = pm.Normal(
            "global_mean", mu=0, tau=prior_tau, testval=df.mean().mean()
        )
        # http://www.stat.columbia.edu/~gelman/research/published/taumain.pdf
        # https://arxiv.org/pdf/1104.4937.pdf
        global_sigma = pm.HalfCauchy("global_sigma", beta=1)
        # Sometimes sampling when this is included
        # testval=df.mean().std())
        thetas = pm.Normal("thetas", mu=0, sigma=1, shape=df.shape[1])
        # Use a non-centered parametrization as discussed in
        # https://arxiv.org/pdf/1312.0906.pdf
        groups = pm.Normal(
            "groups",
            mu=global_mean + global_sigma * thetas,
            sigma=df.std(),
            observed=df,
        )
        vol = pm.Deterministic(
            "volatility",
            np.sqrt(time_scaling) * groups.distribution.variance ** 0.5,
        )
        pm.Deterministic("sharpe", time_scaling * groups.distribution.mean / vol)
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
