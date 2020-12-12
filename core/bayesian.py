import logging
from typing import Any, Union

import numpy as np
import pandas as pd
import pymc3  # type: ignore
import theano  # type: ignore

# See https://stackoverflow.com/questions/51238578/error-non-constant-expression-cannot-be-narrowed-from-type-npy-intp-to-int  # pylint: disable=line-too-long
theano.config.gcc.cxxflags = "-Wno-c++11-narrowing"  # pylint: disable=no-member

_LOG = logging.getLogger(__name__)


DOF = "dof"
LOC = "location"
RET = "returns"
SCALE = "scale"
SHAPE = "shape"
SR = "sharpe"
VOL = "volatility"

NORM_TAG = "normal"
T_TAG = "student_t"
LAP_TAG = "laplace"
ONE_WAY_NORM_TAG = "one_way_normal"


# TODO(Paul): Use config-style constants for defining strings used by models.
# TODO(Paul): Consider factoring out common code


def get_col_shape(data: Union[pd.Series, pd.DataFrame]) -> int:
    """

    :param data:
    :return:
    """
    if isinstance(data, pd.Series):
        return 1
    if isinstance(data, pd.DataFrame):
        s: int = data.shape[1]
        return s
    raise ValueError("Expects pd.Series or pd.DataFrame!")


def best(y1, y2, prior_tau: float = 1e-6, time_scaling: int = 1, **kwargs: Any):
    """
    Bayesian Estimation Supersedes the T Test. See
    http://www.indiana.edu/~kruschke/BEST/BEST.pdf.

    This is a generic t-test replacement.
    :param **kwargs: Passed to pymc3.sample.
    """
    with pymc3.Model() as model:
        # We set the mean of the prior to 0 because
        #   1. In the common use case of returns, `0` is a reasonable prior
        #      value.
        #   2. We typically expect the precision to be set very low so that the
        #      prior is very vague. The center is not so important provided the
        #      prior is sufficiently vague (and the sample sufficiently large).
        group1_mean = pymc3.Normal(
            "group1_mean", mu=0, tau=prior_tau, testval=y1.mean()
        )
        group2_mean = pymc3.Normal(
            "group2_mean", mu=0, tau=prior_tau, testval=y2.mean()
        )
        # BDA3 suggests log std ~ Uniform as a diffuse (improper) prior
        # (See Sec. 2.8).
        # Here, we opt for the "super-vague" but proper analogue by default.
        # (see https://github.com/stan-dev/stan/wiki/Prior-Choice-Recommendations)
        # Sec. 5.7 of BDA suggests Half-Cauchy as a good choice, at least in
        # cases where a small number of groups (in the context of hierarchical
        # models) are used.
        group1_log_std = pymc3.Normal(
            "group1_log_std",
            mu=0,
            tau=prior_tau,
            testval=pymc3.math.log(y1.std()),
        )
        group2_log_std = pymc3.Normal(
            "group2_log_std",
            mu=0,
            tau=prior_tau,
            testval=pymc3.math.log(y2.std()),
        )
        # Convert from log_std to std
        group1_std = pymc3.Deterministic(
            "group1_std", pymc3.math.exp(group1_log_std)
        )
        group2_std = pymc3.Deterministic(
            "group2_std", pymc3.math.exp(group2_log_std)
        )
        # Shape parameter for T-distribution
        # Require nu > 2 so that the second moment exists (for std_dev).
        # TODO(Paul): Justify prior parameter value 1/29.
        nu = pymc3.Exponential("nu_minus_2", 1 / 29.0, testval=4.0) + 2.0
        # Response distributions
        resp_group1 = pymc3.StudentT(
            "group1", nu=nu, mu=group1_mean, lam=group1_std ** -2, observed=y1
        )
        resp_group2 = pymc3.StudentT(
            "group2", nu=nu, mu=group2_mean, lam=group2_std ** -2, observed=y2
        )
        # T-test-like quantities
        diff_of_means = pymc3.Deterministic(
            "difference of means", group2_mean - group1_mean
        )
        pymc3.Deterministic("difference of stds", group2_std - group1_std)
        pymc3.Deterministic(
            "effect size",
            diff_of_means
            / pymc3.math.sqrt(0.5 * (group1_std ** 2 + group2_std ** 2)),
        )
        # Vol in sampling units, unless adjusted.
        group1_vol = pymc3.Deterministic(
            "group1_volatilty",
            np.sqrt(time_scaling)
            * resp_group1.distribution.variance
            ** 0.5,  # pylint: disable=no-member
        )
        group2_vol = pymc3.Deterministic(
            "group2_volatility",
            np.sqrt(time_scaling)
            * resp_group2.distribution.variance
            ** 0.5,  # pylint: disable=no-member
        )
        # Sharpe ratio in sampling units, unless adjusted.
        group1_sr = pymc3.Deterministic(
            "group1_sharpe", time_scaling * resp_group1.mean / group1_vol
        )
        group2_sr = pymc3.Deterministic(
            "group2_sharpe", time_scaling * resp_group2.mean / group2_vol
        )
        pymc3.Deterministic("difference of Sharpe ratios", group2_sr - group1_sr)
        # MCMC
        trace = pymc3.sample(**kwargs)
    return model, trace


def fit_laplace(
    data, prior_tau: float = 1e-6, time_scaling: int = 1, **kwargs: Any
):
    """
    Fit a Laplace distribution to each column of data (independently).

    :param data: pd.Series of pd.DataFrame (obs along rows)
    :param prior_tau: Controls precision of mean and log_std priors
    :param kwargs: Passed to pymc3.sample
    :param time_scaling: Scaling parameter for volatility and Sharpe.
        E.g. If the time sampling units (in the input) are days and volatility
        and Sharpe are to be reported in annualized units, use
        time_scaling = 252.
    :return: PyMC3 model and trace
    """
    cols = get_col_shape(data)
    with pymc3.Model() as model:
        loc = pymc3.Normal(
            LOC, mu=0, tau=prior_tau, testval=data.mean(), shape=cols
        )
        # TODO(Paul): Compare with replacing these priors on sigma with
        # sigma = pymc3.HalfCauchy("std", beta=1, testval=y.std())
        log_std = pymc3.Normal(
            "log_std",
            mu=0,
            tau=prior_tau,
            testval=pymc3.math.log(data.std()),
            shape=cols,
        )
        scale = pymc3.Deterministic(SCALE, pymc3.math.exp(log_std))
        returns = pymc3.Laplace(RET, mu=loc, b=scale, observed=data)
        vol = pymc3.Deterministic(
            VOL,
            np.sqrt(time_scaling)
            * returns.distribution.variance ** 0.5,  # pylint: disable=no-member
        )
        pymc3.Deterministic(
            SR, time_scaling * returns.distribution.mean / vol
        )  # pylint: disable=no-member
        trace = pymc3.sample(**kwargs)
    model.name = LAP_TAG
    return model, trace


def fit_normal(
    data, prior_tau: float = 1e-6, time_scaling: int = 1, **kwargs: Any
):
    """
    Fit a normal distribution to each column of data (independently).

    :param data: pd.Series of pd.DataFrame (obs along rows)
    :param prior_tau: Controls precision of mean and log_std priors
    :param kwargs: Passed to pymc3.sample
    :param time_scaling: Scaling parameter for volatility and Sharpe.
        E.g. If the time sampling units (in the input) are days and volatility
        and Sharpe are to be reported in annualized units, use
        time_scaling = 252.
    :return: PyMC3 model and trace
    """
    cols = get_col_shape(data)
    with pymc3.Model() as model:
        loc = pymc3.Normal(
            LOC, mu=0, tau=prior_tau, testval=data.mean(), shape=cols
        )
        # TODO(Paul): Compare with replacing these priors on sigma with
        # sigma = pymc3.HalfCauchy("std", beta=1, testval=y.std())
        log_std = pymc3.Normal(
            "log_std",
            mu=0,
            tau=prior_tau,
            testval=pymc3.math.log(data.std()),
            shape=cols,
        )
        scale = pymc3.Deterministic(SCALE, pymc3.math.exp(log_std))
        returns = pymc3.Normal(RET, mu=loc, sigma=scale, observed=data)
        vol = pymc3.Deterministic(
            VOL,
            np.sqrt(time_scaling)
            * returns.distribution.variance ** 0.5,  # pylint: disable=no-member
        )
        pymc3.Deterministic(
            SR, time_scaling * returns.distribution.mean / vol
        )  # pylint: disable=no-member
        trace = pymc3.sample(**kwargs)
    model.name = NORM_TAG
    return model, trace


def fit_t(data, prior_tau: float = 1e-6, time_scaling: int = 1, **kwargs: Any):
    """
    Fit a Student T distribution to each column of data (independently).

    :param data: pd.Series of pd.DataFrame (obs along rows)
    :param prior_tau: Controls precision of mean and log_std priors
    :param kwargs: Passed to pymc3.sample
    :param time_scaling: Scaling parameter for volatility and Sharpe.
        E.g. If the time sampling units (in the input) are days and volatility
        and Sharpe are to be reported in annualized units, use
        time_scaling = 252.
    :return: PyMC3 model and trace
    """
    cols = get_col_shape(data)
    with pymc3.Model() as model:
        loc = pymc3.Normal(
            LOC, mu=0, tau=prior_tau, testval=data.mean(), shape=cols
        )
        log_std = pymc3.Normal(
            "log_std",
            mu=0,
            tau=prior_tau,
            testval=pymc3.math.log(data.std()),
            shape=cols,
        )
        scale = pymc3.Deterministic(SCALE, pymc3.math.exp(log_std))
        nu_offset = pymc3.Exponential(DOF + "_minus_2", 1 / 10.0, testval=3.0)
        dof = pymc3.Deterministic(DOF, nu_offset + 2)
        returns = pymc3.StudentT(RET, nu=dof, mu=loc, sigma=scale, observed=data)
        vol = pymc3.Deterministic(
            VOL,
            np.sqrt(time_scaling)
            * returns.distribution.variance ** 0.5,  # pylint: disable=no-member
        )
        pymc3.Deterministic(
            SR, time_scaling * returns.distribution.mean / vol
        )  # pylint: disable=no-member
        trace = pymc3.sample(**kwargs)
    model.name = T_TAG
    return model, trace


def fit_one_way_normal(
    df: pd.DataFrame,
    prior_tau: float = 1e-6,
    time_scaling: int = 1,
    vol_mode: str = "point",
    **kwargs: Any
):
    """
    Fit a one-way normal model.

    :param df: Df of obs, each column representing a group
    :param prior_tau: Controls precision of global mean prior
    :param vol_mode: How to handle volatility estimation
        - 'point': Use df.std() and treat as fixed
        - 'no_pooling': Estimate std for each series without hierarchical
            modeling.
    :param kwargs: Passed to pymc3.sample
    :return: PyMC3 model and trace
    """
    with pymc3.Model() as model:
        global_mean = pymc3.Normal(
            "global_mean", mu=0, tau=prior_tau, testval=df.mean().mean()
        )
        # http://www.stat.columbia.edu/~gelman/research/published/taumain.pdf
        # https://arxiv.org/pdf/1104.4937.pdf
        global_sigma = pymc3.HalfCauchy("global_sigma", beta=1)
        # Sometimes sampling fails when this is included
        # testval=df.mean().std())
        thetas = pymc3.Normal("thetas", mu=0, sigma=1, shape=df.shape[1])
        if vol_mode == "no_pooling":
            log_std = pymc3.Normal(
                "global_std_mean",
                mu=0,
                tau=prior_tau,
                testval=pymc3.math.log(df.std()),
                shape=df.shape[1],
            )
            scale = pymc3.Deterministic(SCALE, pymc3.math.exp(log_std))
        elif vol_mode == "point":
            scale = df.std()
        # Use a non-centered parametrization as discussed in
        # https://arxiv.org/pdf/1312.0906.pdf
        groups = pymc3.Normal(
            "groups",
            mu=global_mean + global_sigma * thetas,
            sigma=scale,
            observed=df,
        )
        vol = pymc3.Deterministic(
            "volatility",
            np.sqrt(time_scaling)
            * groups.distribution.variance ** 0.5,  # pylint: disable=no-member
        )
        pymc3.Deterministic(
            "sharpe", time_scaling * groups.distribution.mean / vol
        )  # pylint: disable=no-member
        trace = pymc3.sample(**kwargs)
    model.name = ONE_WAY_NORM_TAG
    return model, trace


def trace_info(trace, **kwargs) -> None:
    """
    Standard trace plots and info.
    """
    _LOG.info("traceplot...")
    pymc3.traceplot(trace, **kwargs)
    _LOG.info("plot_posterior...")
    pymc3.plot_posterior(trace, ref_val=0, **kwargs)
    _LOG.info("forestplot...")
    pymc3.forestplot(trace, **kwargs)
    _LOG.info("summary...")
    print(pymc3.summary(trace, **kwargs))
