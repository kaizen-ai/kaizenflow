import logging
from typing import List, Optional, Tuple, Union

import gluonts
import gluonts.model.forecast as gmf  # isort: skip # noqa: F401 # pylint: disable=unused-import
import numpy as np
import pandas as pd

import core.data_adapters as adpt
import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


def predict(
    predictor: gluonts.model.predictor.Predictor,
    test_df: pd.DataFrame,
    x_vars: Optional[List[str]],
    y_vars: Union[str, List[str]],
    prediction_length: int,
    frequency: str,
    num_samples: int,
) -> np.array:
    test_ts = adpt.transform_to_gluon(
        test_df, x_vars, y_vars, frequency=frequency
    )
    pred = predictor.predict(test_ts, num_samples=num_samples)
    pred = list(pred)
    dbg.dassert_eq(len(pred), 1)
    yhat = pred[0].samples
    # yhat.shape = `(num_samples, prediction_length)`.
    dbg.dassert_eq(yhat.shape, (num_samples, prediction_length))
    return yhat


def generate_predictions(
    predictor: gluonts.model.predictor.Predictor,
    df: pd.DataFrame,
    y_vars: Union[str, List[str]],
    prediction_length: int,
    frequency: str,
    num_samples: int,
    x_vars: Optional[List[str]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    dbg.dassert_isinstance(df.index, pd.DatetimeIndex)
    dbg.dassert(df.index.freq, "The dataframe should have uniform datetime grid")
    dbg.dassert_monotonic_index(df.index)
    if isinstance(y_vars, str):
        y_vars = [y_vars]
    dbg.dassert_isinstance(y_vars, list)
    dbg.dassert_eq(len(y_vars), 1, "Multitarget case is not supported.")
    y_cols = [f"{y_vars[0]}_{i}" for i in range(prediction_length)]
    yhat_cols = [f"{y_vars[0]}_hat_{i}" for i in range(prediction_length)]
    yhat_all = np.full((df.shape[0], prediction_length), np.nan)
    y_all = np.full((df.shape[0], prediction_length), np.nan)
    for i in range(df.shape[0]):
        test_df = df.iloc[: i + 1]
        yhat = predict(
            predictor,
            test_df,
            x_vars,
            y_vars,
            prediction_length,
            frequency,
            num_samples,
        )
        # TODO(Julia): This should not be the silent default behavior for
        #     num_samples.
        yhat = yhat.mean(axis=0)
        yhat_all[i] = yhat
        y = df.iloc[i : i + prediction_length][y_vars[0]].to_list()
        n_missing_y = prediction_length - len(y)
        if n_missing_y > 0:
            y += [np.nan] * n_missing_y
        y_all[i] = y
    yhat_all = pd.DataFrame(yhat_all, index=df.index, columns=yhat_cols)
    y_all = pd.DataFrame(y_all, index=df.index, columns=y_cols)
    return yhat_all, y_all
