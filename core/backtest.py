import logging
from typing import List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from tqdm.autonotebook import tqdm

import core.data_adapters as adpt
import helpers.dbg as dbg
import helpers.list as hlist

_LOG = logging.getLogger(__name__)

# TODO(gp): Remove after PartTask2335.
if True:
    import gluonts
    import gluonts.evaluation.backtest
    import gluonts.model.forecast as gmf  # isort: skip # noqa: F401 # pylint: disable=unused-import

    def predict(
        predictor: gluonts.model.predictor.Predictor,
        df: pd.DataFrame,
        y_vars: Union[str, List[str]],
        prediction_length: int,
        num_samples: int,
        x_vars: Optional[List[str]] = None,
    ) -> gluonts.model.forecast.SampleForecast:
        """
        Predict next values using trained predictor.

        It is assumed that x_vars and y_vars are both indexed by knowledge times.

        :param predictor: trained gluonts predictor
        :param df: dataframe with target and optionally features
        :param y_vars: target column. Only single target is supported.
        :param prediction_length: number of steps for which the prediction is made
        :param num_samples: number of traces (sample paths) generated
        :param x_vars: feature columns
        :return: SampleForecast with `samples` predictions np.array of shape
            `(num_samples, prediction_length)`
        """
        dbg.dassert_isinstance(df, pd.DataFrame)
        dbg.dassert_isinstance(df.index, pd.DatetimeIndex)
        dbg.dassert(df.index.freq)
        # We implicitly assume here that `x_vars` columns are numerical.
        # TODO(Paul): Add an assertion for this.
        if x_vars is None:
            use_feat_dynamic_real = False
        else:
            use_feat_dynamic_real = True
        #
        if use_feat_dynamic_real:
            y_truncate = prediction_length
        else:
            y_truncate = None
        data = adpt.transform_to_gluon(
            df,
            x_vars,
            y_vars,
            frequency=df.index.freq.freqstr,
            y_truncate=y_truncate,
        )
        # Make predictions.
        predictions = predictor.predict(data, num_samples=num_samples)
        predictions = hlist.assert_single_element_and_return(list(predictions))
        #
        dbg.dassert_eq(
            predictions.samples.shape, (num_samples, prediction_length)
        )
        return predictions

    def generate_predictions(
        predictor: gluonts.model.predictor.Predictor,
        df: pd.DataFrame,
        y_vars: Union[str, List[str]],
        prediction_length: int,
        num_samples: int,
        x_vars: Optional[List[str]] = None,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Generate forward predictions using trained predictor.

        For each time step, generate `num_samples` predictions for each of
        `prediction_length` steps, take the mean across samples.
        The output prediction dataframe is of shape
        `(df.shape[0], prediction_length)`, each row containing a prediction
         made using data up to and including data in corresponding `df` row.

        :param predictor: trained predictor
        :param df: dataframe with features and targets
        :param y_vars: target column. Only single target is supported.
        :param prediction_length: number of steps for which the prediction
            is made
        :param num_samples: number of traces (sample paths) that are
            generated
        :param x_vars: feature columns
        :return: forward predictions and forward target, each of shape
            `(df.shape[0], prediction_length)`. The columns are
            `<y_var>_hat_<timestep>`, `<y_var>_<timestep>` respectively.
        """
        dbg.dassert_isinstance(df, pd.DataFrame)
        dbg.dassert_isinstance(df.index, pd.DatetimeIndex)
        dbg.dassert(
            df.index.freq, "The dataframe should have uniform datetime grid"
        )
        dbg.dassert_strictly_increasing_index(df.index)
        if isinstance(y_vars, str):
            y_vars = [y_vars]
        dbg.dassert_isinstance(y_vars, list)
        dbg.dassert_eq(len(y_vars), 1, "Multitarget case is not supported.")
        y_cols = [f"{y_vars[0]}_{i+1}" for i in range(prediction_length)]
        yhat_cols = [f"{y_vars[0]}_hat_{i+1}" for i in range(prediction_length)]
        yhat_all = np.full((df.shape[0], prediction_length), np.nan)
        y_all = np.full((df.shape[0], prediction_length), np.nan)
        #
        if x_vars is None:
            use_feat_dynamic_real = False
            _LOG.warning("No predictors `x_vars` being used in prediction!")
        else:
            use_feat_dynamic_real = True
        #
        if not use_feat_dynamic_real:
            trunc_len = 0
        else:
            trunc_len = prediction_length
        #
        for i in tqdm(range(df.shape[0])):
            if use_feat_dynamic_real and i < prediction_length:
                # If there are no covariates to make forward prediction on,
                # return NaN predictions.
                y_hat = np.full(prediction_length, np.nan)
                y_hat_start_date = None
            else:
                test_df = df.iloc[: i + 1 + trunc_len]
                sample_forecast = predict(
                    predictor=predictor,
                    df=test_df,
                    y_vars=y_vars,
                    prediction_length=prediction_length,
                    num_samples=num_samples,
                    x_vars=x_vars,
                )
                y_hat = sample_forecast.samples.mean(axis=0)
                y_hat_start_date = sample_forecast.start_date
            yhat_all[i] = y_hat
            y = df.iloc[i + 1 : i + 1 + prediction_length][y_vars[0]].to_list()
            n_missing_y = prediction_length - len(y)
            if n_missing_y > 0:
                y += [np.nan] * n_missing_y
            y_all[i] = y
        # Check that the prediction start dates are the same as the `df`
        # index. It's enough to check only the last index because the grid
        # is uniform.
        pred_idx = df.index
        dbg.dassert_eq(
            pred_idx[-1],
            y_hat_start_date + pd.Timedelta(trunc_len - 1, df.index.freq.freqstr),
            "Prediction start dates are not aligned with the index",
        )
        yhat_all = pd.DataFrame(yhat_all, index=pred_idx, columns=yhat_cols)
        y_all = pd.DataFrame(y_all, index=pred_idx, columns=y_cols)
        return yhat_all, y_all
