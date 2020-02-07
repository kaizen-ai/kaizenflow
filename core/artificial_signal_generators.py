import logging
from typing import List, Optional, Tuple

import gluonts
import gluonts.dataset.repository.datasets as gdrd  # isort: skip # noqa: F401 # pylint: disable=unused-import
import gluonts.dataset.util as gdu  # isort: skip # noqa: F401 # pylint: disable=unused-import
import pandas as pd

_LOG = logging.getLogger(__name__)


def get_gluon_dataset_names() -> List[str]:
    return list(gluonts.dataset.repository.datasets.dataset_recipes.keys())


def get_gluon_dataset(
    dataset_name: str = "m4_hourly",
    train_length: Optional[int] = None,
    test_length: Optional[int] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    dataset = gluonts.dataset.repository.datasets.get_dataset(
        dataset_name, regenerate=False
    )
    train_entry = next(iter(dataset.train))
    test_entry = next(iter(dataset.test))
    train_df = gluonts.dataset.util.to_pandas(train_entry)
    test_df = gluonts.dataset.util.to_pandas(test_entry)
    train_length = train_length or train_df.shape[0]
    test_length = test_length or test_df.shape[0]
    train_df = pd.DataFrame(train_df.head(train_length), columns=["y"])
    test_df = pd.DataFrame(test_df.head(test_length), columns=["y"])
    return train_df, test_df
