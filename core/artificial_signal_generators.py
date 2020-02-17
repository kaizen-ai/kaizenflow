"""
Import as:

import core.artificial_signal_generators as sig_gen
"""

import logging
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import gluonts
import gluonts.dataset.artificial as gda
import gluonts.dataset.artificial.recipe as rcp
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


def evaluate_recipe(
    recipe: List[Tuple[str, Callable]], length: int, **kwargs: Any
) -> Dict[str, np.array]:
    """
    Generate data based on recipe.

    For documentation on recipes, see
    gluonts.dataset.artificial._base.RecipeDataset.

    :param recipe: [(field, function)]
    :param length: length of data to generate
    :param kwargs: kwargs passed into gluonts.dataset.artificial.recipe.evaluate
    :return: field names mapped to generated data
    """
    return rcp.evaluate(recipe, length, **kwargs)


def add_recipes(
    recipe: List[Tuple[str, Callable]], name: str = "signal"
) -> List[Tuple[str, rcp.Lifted]]:
    """
    Add recipe components.

    :param recipe: [(field, function)]
    :param name: name of the sum
    :return: recipe with the sum component
    """
    recipe = recipe.copy()
    names = [name for name, _ in recipe]
    addition = rcp.Add(names)
    recipe.append((name, addition))
    return recipe


def generate_recipe_dataset(
    recipe: Union[Callable, List[Tuple[str, Callable]]],
    freq: str,
    start_date: pd.Timestamp,
    max_train_length: int,
    prediction_length: int,
    num_timeseries: int,
    trim_length_fun: Callable = lambda x, **kwargs: 0,
) -> gluonts.dataset.common.TrainDatasets:
    """
    Generate GluonTS TrainDatasets from recipe.

    For more information on recipes, see
    gluonts.dataset.artificial._base.RecipeDataset and
    https://gluon-ts.mxnet.io/examples/synthetic_data_generation_tutorial/tutorial.html.

    For `feat_dynamic_cat` and `feat_dynamic_real` generation pass in
    `shape=(n_features, 0)`. GluonTS replaces `0` in shape with
    `max_train_length + prediction_length`.

    :param recipe: GluonTS recipe. Datasets with keys `feat_dynamic_cat`,
        `feat_dynamic_real` and `target` are passed into `ListDataset`.
    :param freq: frequency
    :param start_date: start date of the dataset
    :param max_train_length: maximum length of a training time series
    :param prediction_length: length of prediction range
    :param num_timeseries: number of time series to generate
    :param trim_length_fun: Callable f(x: int) -> int returning the
        (shortened) training length
    :return: GluonTS TrainDatasets (with `train` and `test` attributes).
    """
    names = [name for name, _ in recipe]
    dbg.dassert_in("target", names)
    metadata = gluonts.dataset.common.MetaData(freq=freq)
    recipe_dataset = gda.RecipeDataset(
        recipe,
        metadata,
        max_train_length,
        prediction_length,
        num_timeseries,
        trim_length_fun=trim_length_fun,
        data_start=start_date,
    )
    return recipe_dataset.generate()
