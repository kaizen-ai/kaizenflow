import logging
from typing import List, Optional, Union

import gluonts
import gluonts.dataset.common as gdc  # isort: skip # noqa: F401 # pylint: disable=unused-import
import gluonts.model.forecast as gmf  # isort: skip # noqa: F401 # pylint: disable=unused-import
import numpy as np
import pandas as pd

import core.data_adapters as adpt
import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)

# def make_evaluation_predictions(data):


def generate_predictions(
    predictor: gluonts.model.predictor.Predictor,
    df: pd.DataFrame,
    x_vars: Optional[List[str]],
    y_vars: Union[str, List[str]],
    prediction_length: int,
    context_length: int,
    frequency: str,
    num_samples: int,
):
    dbg.dassert_lte(prediction_length + context_length, df.shape[0])
    dbg.dassert_isinstance(df.index, pd.DatetimeIndex)
    dbg.dassert(df.index.freq, "The dataframe should have uniform datetime grid")
    dbg.dassert_monotonic_index(df.index)
    if isinstance(y_vars, str):
        y_vars = [y_vars]
    dbg.dassert_isinstance(y_vars, list)
    dbg.dassert_eq(len(y_vars), 1, "Multitarget case is not supported.")
    # TODO(Julia): Optimize this by creating ListDataset once and then
    #     iterating it.
    y_cols = [f"{y_vars}_{i}" for i in range(prediction_length)]
    yhat_cols = [f"{y_vars}_hat_{i}" for i in range(prediction_length)]
    pred_indices = df.index[context_length:-prediction_length]
    _LOG.info("len(pred_indices)=%s", len(pred_indices))
    yhat_all = np.zeros((pred_indices.shape[0], prediction_length))
    y_all = np.zeros((pred_indices.shape[0], prediction_length))
    for i, idx in enumerate(pred_indices):
        curr_int_idx = context_length + i
        test_df = df.iloc[curr_int_idx : curr_int_idx + prediction_length]
        _LOG.info("test_df: %s", test_df)
        test_ts = adpt.transform_to_gluon(
            test_df, x_vars, y_vars, frequency=frequency
        )
        _LOG.info("test_ts: %s", list(test_ts)[0])
        pred = predictor.predict(test_ts, num_samples=num_samples)
        # return test_df, test_ts, pred
        pred = list(pred)
        dbg.dassert_eq(len(pred), 1)
        yhat = pred[0].samples
        _LOG.info("yhat.shape: %s", yhat.shape)
        # yhat.shape = `(num_samples, prediction_length)`.
        dbg.dassert_eq(yhat.shape[1], test_df.shape[0])
        # TODO(Julia): This should not be the silent default behavior for
        #     num_samples.
        yhat = yhat.mean(axis=0)
        # yhat_df = pd.DataFrame(yhat, index=[idx], columns=yhat_cols)
        # y_df = pd.DataFrame(test_df[y_vars].values, index=[idx], columns=yhat_cols)
        yhat_all[i] = yhat
        y_all[i] = test_df[y_vars].values
    yhat_all = pd.DataFrame(yhat_all, index=pred_indices, columns=yhat_cols)
    y_all = pd.DataFrame(y_all, index=pred_indices, columns=y_cols)
    return yhat_all, y_all
