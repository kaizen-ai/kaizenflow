"""
Import as:

import core.artificial_signal_generators as sig_gen
"""

import logging
from typing import Iterable, List, Optional, Tuple, Union

import gluonts
import gluonts.dataset.repository.datasets as gdrd  # isort: skip # noqa: F401 # pylint: disable=unused-import
import gluonts.dataset.util as gdu  # isort: skip # noqa: F401 # pylint: disable=unused-import
import numpy as np
import pandas as pd

import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


def get_gluon_dataset_names() -> List[str]:
    """
    Get names of available Gluon datasets. Each of those names can be
    used in `get_gluon_dataset` function.

    :return: list of names
    """
    return list(gluonts.dataset.repository.datasets.dataset_recipes.keys())


def get_gluon_dataset(
    dataset_name: str = "m4_hourly",
    train_length: Optional[int] = None,
    test_length: Optional[int] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load Gluon dataset, transform it into train and test dataframes.

    The default `m4_hourly` time series look like this:
    https://gluon-ts.mxnet.io/_images/examples_forecasting_tutorial_9_0.png

    :param dataset_name: name of the dataset. Supported names can be
        obtained using `get_gluon_dataset_names`.
    :param train_length: length of the train dataset
    :param test_length: length of the test dataset
    :return: train and test dataframes
    """
    dataset = gluonts.dataset.repository.datasets.get_dataset(
        dataset_name, regenerate=False
    )
    train_entry = next(iter(dataset.train))
    test_entry = next(iter(dataset.test))
    train_df = gluonts.dataset.util.to_pandas(train_entry)
    test_df = gluonts.dataset.util.to_pandas(test_entry)
    train_length = train_length or train_df.shape[0]
    test_length = test_length or test_df.shape[0]
    dbg.dassert_lte(train_length, train_df.shape[0])
    dbg.dassert_lte(test_length, test_df.shape[0])
    train_df = pd.DataFrame(train_df.head(train_length), columns=["y"])
    test_df = pd.DataFrame(test_df.head(test_length), columns=["y"])
    return train_df, test_df


def get_heaviside(a: int, b: int, zero_val: int, tick: int) -> pd.Series:
    """
    Generate Heaviside pd.Series.
    """
    dbg.dassert_lte(a, zero_val)
    dbg.dassert_lte(zero_val, b)
    array = np.arange(a, b, tick)
    srs = pd.Series(
        data=np.heaviside(array, zero_val), index=array, name="Heaviside"
    )
    return srs


def get_impulse(a: int, b: int, tick: int) -> pd.Series:
    """
    Generate unit impulse pd.Series.
    """
    heavi = get_heaviside(a, b, 1, tick)
    impulse = (heavi - heavi.shift(1)).shift(-1).fillna(0)
    impulse.name = "impulse"
    return impulse


def get_binomial_tree(
    p: Union[float, Iterable[float]],
    vol: float,
    size: Union[int, Tuple[int, ...], None],
    seed: Optional[int] = None,
) -> pd.Series:
    # binomial_tree(0.5, 0.1, 252, seed=0).plot()
    np.random.seed(seed=seed)
    pos = np.random.binomial(1, p, size)
    neg = np.full(size, 1) - pos
    delta = float(vol) * (pos - neg)
    return pd.Series(np.exp(delta.cumsum()), name="binomial_walk")


def get_gaussian_walk(
    drift: Union[float, Iterable[float]],
    vol: Union[float, Iterable[float]],
    size: Union[int, Tuple[int, ...], None],
    seed: Optional[int] = None,
) -> pd.Series:
    # get_gaussian_walk(0, .2, 252, seed=10)
    np.random.seed(seed=seed)
    gauss = np.random.normal(drift, vol, size)
    return pd.Series(np.exp(gauss.cumsum()), name="gaussian_walk")
