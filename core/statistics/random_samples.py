"""
Import as:

import core.statistics.random_samples as cstatrs
"""

import logging
from typing import Callable, Iterable, List, Optional

import numpy as np
import pandas as pd
from tqdm.autonotebook import tqdm

import helpers.hdbg as hdbg

_LOG = logging.getLogger(__name__)


def get_iid_standard_gaussian_samples(n_samples: int, seed: int) -> pd.Series:
    """
    Draw `n_samples` of iid standard Gaussian rvs.

    :param n_samples: number of samples to draw
    :param seed: seed to forward to numpy random number generator
    :return: series of `n_samples` IID Gaussian random samples
    """
    rng = np.random.default_rng(seed=seed)
    gaussians = rng.standard_normal(size=n_samples)
    srs = pd.Series(
        gaussians,
        range(1, n_samples + 1),
        name=f"n_samples={n_samples}_seed={seed}",
    )
    return srs


def get_iid_rademacher_samples(n_samples: int, seed: int) -> pd.Series:
    """
    Draw `n_samples` of iid Rademacher rvs.

    :param n_samples: number of samples to draw
    :param seed: seed to forward to numpy random number generator
    :return: series of `n_samples` IID Rademacher random samples
    """
    rng = np.random.default_rng(seed=seed)
    samples = rng.integers(low=0, high=2, size=n_samples)
    srs = pd.Series(
        samples,
        range(1, n_samples + 1),
        name=f"n_samples={n_samples}_seed={seed}",
    )
    srs = (2 * srs - 1).astype(np.int64)
    return srs


def convert_increments_to_random_walk(srs: pd.Series) -> pd.Series:
    """
    Convert increments to a random walk starting from zero.
    """
    hdbg.dassert_isinstance(srs, pd.Series)
    vals = [0] + srs.to_list()
    #
    random_walk = pd.Series(vals, index=range(srs.size + 1), name=srs.name)
    random_walk = random_walk.cumsum()
    return random_walk


def annotate_random_walk(srs: pd.Series) -> pd.DataFrame:
    """
    Annotate a random walk with running features.
    """
    hdbg.dassert_isinstance(srs, pd.Series)
    srs_list = [srs]
    # Add running max.
    running_max = srs.cummax().rename("running_max")
    srs_list.append(running_max)
    # Add running min.
    running_min = srs.cummin().rename("running_min")
    srs_list.append(running_min)
    # Add running mean.
    running_mean = srs.expanding(1).mean().rename("running_mean")
    srs_list.append(running_mean)
    # Add running standard deviation.
    running_std = srs.expanding(1).std().rename("running_std")
    srs_list.append(running_std)
    #
    df = pd.concat(srs_list, axis=1)
    return df


def apply_func_to_random_walk(
    func: Callable,
    sample_sizes: List[int],
    seeds: Iterable[int],
    increment_func: Optional[Callable] = None,
) -> pd.DataFrame:
    """
    Apply function `func` to random walks.

    :param func: a function that accepts a random walk and returns a pd.Series
    :param sample_sizes: list of sample sizes to use in generating random walks
    :param seeds: seeds to iterate over for each sample size
    :param increment_func: function that accepts a "sample_size" and "seed" to
        generate random walk increments
    """
    if increment_func is None:
        increment_func = get_iid_standard_gaussian_samples
    func_results = []
    for n_samples in tqdm(sample_sizes, desc="samples_sizes"):
        for seed in tqdm(seeds, desc="seeds"):
            increments = increment_func(n_samples, seed)
            random_walk = convert_increments_to_random_walk(increments)
            result = func(random_walk)
            func_results.append(result)
    df = pd.concat(func_results, axis=1).T
    return df