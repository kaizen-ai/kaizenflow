import logging

import pymc3 as pm


_LOG = logging.getLogger(__name__)


def best(y1, y2, prior_tau=1e-6, samples=1000):
    """
    Bayesian Estimation Supersedes the T Test
    """
    with pm.Model() as model:
        # We set the mean of the prior to 0 because
        #   1. In the common use case of returns, `0` is a reasonable prior
        #      value.
        #   2. We typically expect the precision to be set very low so that the
        #      prior is very vague. The center is not so important provided the
        #      prior is sufficiently vague.
        group1_mean = pm.Normal('group1_mean', mu=0, tau=prior_tau,
                                testval=y1.mean())
        group2_mean = pm.Normal('group2_mean', mu=0, tau=prior_tau,
                                testval=y2.mean())
        # BDA3 suggests log std ~ Uniform as a diffuse (improper) prior
        # (See Sec. 2.8).
        # Here, we opt for the "super-vague" but proper analogue by default.
        # (see https://github.com/stan-dev/stan/wiki/Prior-Choice-Recommendations)
        group1_log_std = pm.Normal('group1_log_std', mu=0, tau=prior_tau,
                                   testval=pm.math.log(y1.std()))
        group2_log_std = pm.Normal('group2_log_std', mu=0, tau=prior_tau,
                                   testval=pm.math.log(y2.std()))
        # Convert from log_std to std
        group1_std = pm.Deterministic('group1_std', pm.math.exp(group1_log_std))
        group2_std = pm.Deterministic('group2_std', pm.math.exp(group2_log_std))
        # Shape parameter for T-distribution
        # TODO(Paul): Justify a prior parameter value.
        nu = pm.Exponential('nu_minus_1', 1/29., testval=4.) + 1. 
        # Response distributions
        resp_group1 = pm.StudentT('group1', nu=nu, mu=group1_mean,
                                  lam=group1_std**-2, observed=y1)
        resp_group2 = pm.StudentT('group2', nu=nu, mu=group2_mean,
                                  lam=group2_std**-2, observed=y2)
        # T-test-like quantities
        diff_of_means = pm.Deterministic('difference of means',
                                         group2_mean - group1_mean)
        pm.Deterministic('difference of stds',
                         group2_std - group1_std)
        pm.Deterministic('effect size', diff_of_means /
                         pm.math.sqrt(0.5 * (group1_std**2 + group2_std**2)))

        trace = pm.sample(samples, progressbar=True)
    return model, trace
