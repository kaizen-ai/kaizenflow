import collections
import datetime
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
import sklearn as sklear

import core.data_adapters as cdataa
import core.signal_processing as csigna
import core.statistics as cstati
import helpers.dbg as dbg
from core.dataflow.nodes.base import FitPredictNode, RegFreqMixin, ToListMixin
from core.dataflow.nodes.transformers import ColModeMixin
from core.dataflow.utils import get_df_info_as_string

_LOG = logging.getLogger(__name__)


_COL_TYPE = Union[int, str]
_PANDAS_DATE_TYPE = Union[str, pd.Timestamp, datetime.datetime]
_TO_LIST_MIXIN_TYPE = Union[List[_COL_TYPE], Callable[[], List[_COL_TYPE]]]


# #############################################################################
# sklearn - supervised prediction models
# #############################################################################


class ContinuousSkLearnModel(
    FitPredictNode, RegFreqMixin, ToListMixin, ColModeMixin
):
    """
    Fit and predict an sklearn model.
    """

    # pylint: disable=too-many-ancestors

    def __init__(
        self,
        nid: str,
        model_func: Callable[..., Any],
        x_vars: _TO_LIST_MIXIN_TYPE,
        y_vars: _TO_LIST_MIXIN_TYPE,
        steps_ahead: int,
        model_kwargs: Optional[Any] = None,
        col_mode: Optional[str] = None,
        nan_mode: Optional[str] = None,
    ) -> None:
        """
        Specify the data and sklearn modeling parameters.

        :param nid: unique node id
        :param model_func: an sklearn model
        :param x_vars: indexed by knowledge datetimes
            - `x_vars` may contain lags of `y_vars`
        :param y_vars: indexed by knowledge datetimes
            - e.g., in the case of returns, this would correspond to `ret_0`
        :param steps_ahead: number of steps ahead for which a prediction is
            to be generated. E.g.,
            - if `steps_ahead == 0`, then the predictions are
              are contemporaneous with the observed response (and hence
              inactionable)
            - if `steps_ahead == 1`, then the model attempts to predict
              `y_vars` for the next time step
            - The model is only trained to predict the target
              `steps_ahead` steps ahead (and not all intermediate steps)
        :param model_kwargs: parameters to forward to the sklearn model
            (e.g., regularization constants)
        :param col_mode: "merge_all" or "replace_all", as in
            ColumnTransformer()
        :param nan_mode: "drop" or "raise"
        """
        super().__init__(nid)
        self._model_func = model_func
        self._model_kwargs = model_kwargs or {}
        self._x_vars = x_vars
        self._y_vars = y_vars
        self._model = None
        self._steps_ahead = steps_ahead
        dbg.dassert_lte(
            0, self._steps_ahead, "Non-causal prediction attempted! Aborting..."
        )
        # NOTE: Set to "replace_all" for backward compatibility.
        self._col_mode = col_mode or "replace_all"
        dbg.dassert_in(self._col_mode, ["replace_all", "merge_all"])
        self._nan_mode = nan_mode or "raise"

    def fit(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        self._validate_input_df(df_in)
        df = df_in.copy()
        # Obtain index slice for which forward targets exist.
        dbg.dassert_lt(self._steps_ahead, df.index.size)
        idx = df.index[: -self._steps_ahead]
        # Determine index where no x_vars are NaN.
        x_vars = self._to_list(self._x_vars)
        non_nan_idx_x = df.loc[idx][x_vars].dropna().index
        # Determine index where target is not NaN.
        fwd_y_df = self._get_fwd_y_df(df).loc[idx].dropna()
        non_nan_idx_fwd_y = fwd_y_df.dropna().index
        # Intersect non-NaN indices.
        non_nan_idx = non_nan_idx_x.intersection(non_nan_idx_fwd_y)
        dbg.dassert(not non_nan_idx.empty)
        fwd_y_df = fwd_y_df.loc[non_nan_idx]
        # Handle presence of NaNs according to `nan_mode`.
        self._handle_nans(idx, non_nan_idx)
        # Prepare x_vars in sklearn format.
        x_fit = cdataa.transform_to_sklearn(df.loc[non_nan_idx], x_vars)
        # Prepare forward y_vars in sklearn format.
        fwd_y_fit = cdataa.transform_to_sklearn(
            fwd_y_df, fwd_y_df.columns.tolist()
        )
        # Define and fit model.
        self._model = self._model_func(**self._model_kwargs)
        self._model = self._model.fit(x_fit, fwd_y_fit)
        # Generate insample predictions and put in dataflow dataframe format.
        fwd_y_hat = self._model.predict(x_fit)
        #
        fwd_y_hat_vars = [f"{y}_hat" for y in fwd_y_df.columns]
        fwd_y_hat = cdataa.transform_from_sklearn(
            non_nan_idx, fwd_y_hat_vars, fwd_y_hat
        )
        # TODO(Paul): Summarize model perf or make configurable.
        # TODO(Paul): Consider separating model eval from fit/predict.
        info = collections.OrderedDict()
        info["model_x_vars"] = x_vars
        info["model_params"] = self._model.get_params()
        model_attribute_info = collections.OrderedDict()
        for k, v in vars(self._model).items():
            model_attribute_info[k] = v
        info["model_attributes"] = model_attribute_info
        info["insample_perf"] = self._model_perf(fwd_y_df, fwd_y_hat)
        info["insample_score"] = self._score(fwd_y_df, fwd_y_hat)
        # Return targets and predictions.
        df_out = fwd_y_df.merge(
            fwd_y_hat, how="outer", left_index=True, right_index=True
        )
        df_out = df_out.reindex(idx)
        df_out = self._apply_col_mode(
            df, df_out, cols=self._to_list(self._y_vars), col_mode=self._col_mode
        )
        info["df_out_info"] = get_df_info_as_string(df_out)
        self._set_info("fit", info)
        return {"df_out": df_out}

    def predict(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        self._validate_input_df(df_in)
        df = df_in.copy()
        idx = df.index
        # Restrict to times where x_vars have no NaNs.
        x_vars = self._to_list(self._x_vars)
        non_nan_idx = df.loc[idx][x_vars].dropna().index
        # Handle presence of NaNs according to `nan_mode`.
        self._handle_nans(idx, non_nan_idx)
        # Transform x_vars to sklearn format.
        x_predict = cdataa.transform_to_sklearn(df.loc[non_nan_idx], x_vars)
        # Use trained model to generate predictions.
        dbg.dassert_is_not(
            self._model, None, "Model not found! Check if `fit` has been run."
        )
        fwd_y_hat = self._model.predict(x_predict)
        # Put predictions in dataflow dataframe format.
        fwd_y_df = self._get_fwd_y_df(df).loc[non_nan_idx]
        fwd_y_non_nan_idx = fwd_y_df.dropna().index
        fwd_y_hat_vars = [f"{y}_hat" for y in fwd_y_df.columns]
        fwd_y_hat = cdataa.transform_from_sklearn(
            non_nan_idx, fwd_y_hat_vars, fwd_y_hat
        )
        # Generate basic perf cstati.
        info = collections.OrderedDict()
        info["model_params"] = self._model.get_params()
        info["model_perf"] = self._model_perf(fwd_y_df, fwd_y_hat)
        info["model_score"] = self._score(
            fwd_y_df.loc[fwd_y_non_nan_idx], fwd_y_hat.loc[fwd_y_non_nan_idx]
        )
        # Return predictions.
        df_out = fwd_y_df.merge(
            fwd_y_hat, how="outer", left_index=True, right_index=True
        )
        df_out = df_out.reindex(idx)
        df_out = self._apply_col_mode(
            df, df_out, cols=self._to_list(self._y_vars), col_mode=self._col_mode
        )
        info["df_out_info"] = get_df_info_as_string(df_out)
        # TODO(*): Consider adding state to `info` as follows.
        # info["state"] = pickle.dumps(self._model)
        self._set_info("predict", info)
        return {"df_out": df_out}

    def get_fit_state(self) -> Dict[str, Any]:
        fit_state = {"_model": self._model, "_info['fit']": self._info["fit"]}
        return fit_state

    def set_fit_state(self, fit_state: Dict[str, Any]):
        self._model = fit_state["_model"]
        self._info["fit"] = fit_state["_info['fit']"]

    def _get_fwd_y_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Return dataframe of `steps_ahead` forward y values.
        """
        y_vars = self._to_list(self._y_vars)
        mapper = lambda y: str(y) + "_%i" % self._steps_ahead
        # TODO(Paul): Ensure that `fwd_y_vars` and `y_vars` do not overlap.
        fwd_y_df = df[y_vars].shift(-self._steps_ahead).rename(columns=mapper)
        return fwd_y_df

    def _handle_nans(
        self, idx: pd.DataFrame.index, non_nan_idx: pd.DataFrame.index
    ) -> None:
        if self._nan_mode == "raise":
            if idx.shape[0] != non_nan_idx.shape[0]:
                nan_idx = idx.difference(non_nan_idx)
                raise ValueError(f"NaNs detected at {nan_idx}")
        elif self._nan_mode == "drop":
            pass
        else:
            raise ValueError(f"Unrecognized nan_mode `{self._nan_mode}`")

    def _score(
        self,
        y_true: Union[pd.Series, pd.DataFrame],
        y_pred: Union[pd.Series, pd.DataFrame],
    ) -> Optional[float]:
        """
        Compute accuracy for classification or R^2 score for regression.
        """
        if sklear.base.is_classifier(self._model):
            metric = sklear.metrics.accuracy_score
        elif sklear.base.is_regressor(self._model):
            metric = sklear.metrics.r2_score
        else:
            return None
        # In `predict()` method, `y_pred` may exist for index where `y_true`
        # is already `NaN`.
        y_true = y_true.loc[: y_true.last_valid_index()]
        return metric(y_true, y_pred.loc[y_true.index])

    # TODO(Paul): Consider omitting this (and relying on downstream
    #     processing to e.g., adjust for number of hypotheses tested).
    @staticmethod
    def _model_perf(
        y: pd.DataFrame, y_hat: pd.DataFrame
    ) -> collections.OrderedDict:
        info = collections.OrderedDict()
        # info["hitrate"] = pip._compute_model_hitrate(self.model, x, y)
        pnl_rets = y.multiply(
            y_hat.rename(columns=lambda x: x.replace("_hat", ""))
        )
        info["pnl_rets"] = pnl_rets
        info["sr"] = cstati.compute_annualized_sharpe_ratio(
            csigna.resample(pnl_rets, rule="1B").sum()
        )
        return info


class SkLearnModel(FitPredictNode, ToListMixin, ColModeMixin):
    """
    Fit and predict an sklearn model.

    No NaN-handling or uniform sampling frequency requirement.
    """

    def __init__(
        self,
        nid: str,
        x_vars: _TO_LIST_MIXIN_TYPE,
        y_vars: _TO_LIST_MIXIN_TYPE,
        model_func: Callable[..., Any],
        model_kwargs: Optional[Any] = None,
        col_mode: Optional[str] = None,
    ) -> None:
        super().__init__(nid)
        self._model_func = model_func
        self._model_kwargs = model_kwargs or {}
        self._x_vars = x_vars
        self._y_vars = y_vars
        self._model = None
        self._col_mode = col_mode or "replace_all"
        dbg.dassert_in(self._col_mode, ["replace_all", "merge_all"])

    def fit(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        SkLearnModel._validate_input_df(df_in)
        dbg.dassert(
            df_in[df_in.isna().any(axis=1)].index.empty,
            "NaNs detected at index `%s`",
            str(df_in[df_in.isna().any(axis=1)].head().index),
        )
        df = df_in.copy()
        idx = df.index
        x_vars, x_fit, y_vars, y_fit = self._to_sklearn_format(df)
        self._model = self._model_func(**self._model_kwargs)
        self._model = self._model.fit(x_fit, y_fit)
        y_hat = self._model.predict(x_fit)
        #
        x_fit, y_fit, y_hat = self._from_sklearn_format(
            idx, x_vars, x_fit, y_vars, y_fit, y_hat
        )
        # TODO(Paul): Summarize model perf or make configurable.
        # TODO(Paul): Consider separating model eval from fit/predict.
        info = collections.OrderedDict()
        info["model_x_vars"] = x_vars
        info["model_params"] = self._model.get_params()
        # Return targets and predictions.
        y_hat = y_hat.reindex(idx)
        df_out = self._apply_col_mode(
            df, y_hat, cols=y_vars, col_mode=self._col_mode
        )
        info["df_out_info"] = get_df_info_as_string(df_out)
        self._set_info("fit", info)
        return {"df_out": df_out}

    def predict(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        SkLearnModel._validate_input_df(df_in)
        df = df_in.copy()
        idx = df.index
        x_vars, x_predict, y_vars, y_predict = self._to_sklearn_format(df)
        dbg.dassert_is_not(
            self._model, None, "Model not found! Check if `fit` has been run."
        )
        y_hat = self._model.predict(x_predict)
        x_predict, y_predict, y_hat = self._from_sklearn_format(
            idx, x_vars, x_predict, y_vars, y_predict, y_hat
        )
        info = collections.OrderedDict()
        info["model_params"] = self._model.get_params()
        info["model_perf"] = self._model_perf(x_predict, y_predict, y_hat)
        # Return predictions.
        y_hat = y_hat.reindex(idx)
        df_out = self._apply_col_mode(
            df, y_hat, cols=y_vars, col_mode=self._col_mode
        )
        info["df_out_info"] = get_df_info_as_string(df_out)
        self._set_info("predict", info)
        return {"df_out": df_out}

    def get_fit_state(self) -> Dict[str, Any]:
        fit_state = {"_model": self._model, "_info['fit']": self._info["fit"]}
        return fit_state

    def set_fit_state(self, fit_state: Dict[str, Any]):
        self._model = fit_state["_model"]
        self._info["fit"] = fit_state["_info['fit']"]

    @staticmethod
    def _validate_input_df(df: pd.DataFrame) -> None:
        """
        Assert if df violates constraints, otherwise return `None`.
        """
        dbg.dassert_isinstance(df, pd.DataFrame)
        dbg.dassert_no_duplicates(df.columns)

    # TODO(Paul): Add type hints.
    @staticmethod
    def _model_perf(
        x: pd.DataFrame, y: pd.DataFrame, y_hat: pd.DataFrame
    ) -> collections.OrderedDict:
        _ = x
        info = collections.OrderedDict()
        # info["hitrate"] = pip._compute_model_hitrate(self.model, x, y)
        pnl_rets = y.multiply(y_hat.rename(columns=lambda x: x.strip("_hat")))
        info["pnl_rets"] = pnl_rets
        info["sr"] = cstati.compute_sharpe_ratio(
            csigna.resample(pnl_rets, rule="1B").sum(), time_scaling=252
        )
        return info

    def _to_sklearn_format(
        self, df: pd.DataFrame
    ) -> Tuple[List[_COL_TYPE], np.array, List[_COL_TYPE], np.array]:
        x_vars = self._to_list(self._x_vars)
        y_vars = self._to_list(self._y_vars)
        x_vals, y_vals = cdataa.transform_to_sklearn_old(df, x_vars, y_vars)
        return x_vars, x_vals, y_vars, y_vals

    @staticmethod
    def _from_sklearn_format(
        idx: pd.Index,
        x_vars: List[_COL_TYPE],
        x_vals: np.array,
        y_vars: List[_COL_TYPE],
        y_vals: np.array,
        y_hat: np.array,
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        x = cdataa.transform_from_sklearn(idx, x_vars, x_vals)
        y = cdataa.transform_from_sklearn(idx, y_vars, y_vals)
        y_h = cdataa.transform_from_sklearn(
            idx, [f"{y}_hat" for y in y_vars], y_hat
        )
        return x, y, y_h


# #############################################################################
# sklearn - unsupervised models
# #############################################################################


class UnsupervisedSkLearnModel(
    FitPredictNode, RegFreqMixin, ToListMixin, ColModeMixin
):
    """
    Fit and transform an unsupervised sklearn model.
    """

    # pylint: disable=too-many-ancestors

    def __init__(
        self,
        nid: str,
        model_func: Callable[..., Any],
        x_vars: Optional[_TO_LIST_MIXIN_TYPE] = None,
        model_kwargs: Optional[Any] = None,
        col_mode: Optional[str] = None,
        nan_mode: Optional[str] = None,
    ) -> None:
        """
        Specify the data and sklearn modeling parameters.

        :param nid: unique node id
        :param model_func: an sklearn model
        :param x_vars: indexed by knowledge datetimes
        :param model_kwargs: parameters to forward to the sklearn model
            (e.g., regularization constants)
        """
        super().__init__(nid)
        self._model_func = model_func
        self._model_kwargs = model_kwargs or {}
        self._x_vars = x_vars
        self._model = None
        self._col_mode = col_mode or "replace_all"
        dbg.dassert_in(self._col_mode, ["replace_all", "merge_all"])
        self._nan_mode = nan_mode or "raise"

    def fit(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        return self._fit_predict_helper(df_in, fit=True)

    def predict(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        return self._fit_predict_helper(df_in, fit=False)

    def get_fit_state(self) -> Dict[str, Any]:
        fit_state = {"_model": self._model, "_info['fit']": self._info["fit"]}
        return fit_state

    def set_fit_state(self, fit_state: Dict[str, Any]):
        self._model = fit_state["_model"]
        self._info["fit"] = fit_state["_info['fit']"]

    def _fit_predict_helper(
        self, df_in: pd.DataFrame, fit: bool = False
    ) -> Dict[str, pd.DataFrame]:
        """
        Factor out common flow for fit/predict.

        :param df_in: as in `fit`/`predict`
        :param fit: fits model iff `True`
        :return: transformed df_in
        """
        self._validate_input_df(df_in)
        df = df_in.copy()
        # Determine index where no x_vars are NaN.
        if self._x_vars is None:
            x_vars = df_in.columns.tolist()
        else:
            x_vars = self._to_list(self._x_vars)
        non_nan_idx = df[x_vars].dropna().index
        dbg.dassert(not non_nan_idx.empty)
        # Handle presence of NaNs according to `nan_mode`.
        self._handle_nans(df.index, non_nan_idx)
        # Prepare x_vars in sklearn format.
        x_fit = cdataa.transform_to_sklearn(df.loc[non_nan_idx], x_vars)
        if fit:
            # Define and fit model.
            self._model = self._model_func(**self._model_kwargs)
            self._model = self._model.fit(x_fit)
        # Generate insample transformations and put in dataflow dataframe format.
        x_transform = self._model.transform(x_fit)
        #
        num_cols = x_transform.shape[1]
        x_hat = cdataa.transform_from_sklearn(
            non_nan_idx, list(range(num_cols)), x_transform
        )
        info = collections.OrderedDict()
        info["model_x_vars"] = x_vars
        info["model_params"] = self._model.get_params()
        model_attribute_info = collections.OrderedDict()
        for k, v in vars(self._model).items():
            model_attribute_info[k] = v
        info["model_attributes"] = model_attribute_info
        # Return targets and predictions.
        df_out = x_hat.reindex(index=df_in.index)
        df_out = self._apply_col_mode(
            df, df_out, cols=x_vars, col_mode=self._col_mode
        )
        info["df_out_info"] = get_df_info_as_string(df_out)
        if fit:
            self._set_info("fit", info)
        else:
            self._set_info("predict", info)
        dbg.dassert_no_duplicates(df_out.columns)
        return {"df_out": df_out}

    def _handle_nans(
        self, idx: pd.DataFrame.index, non_nan_idx: pd.DataFrame.index
    ) -> None:
        if self._nan_mode == "raise":
            if idx.shape[0] != non_nan_idx.shape[0]:
                nan_idx = idx.difference(non_nan_idx)
                raise ValueError(f"NaNs detected at {nan_idx}")
        elif self._nan_mode == "drop":
            pass
        else:
            raise ValueError(f"Unrecognized nan_mode `{self._nan_mode}`")


class MultiindexUnsupervisedSkLearnModel(
    FitPredictNode, RegFreqMixin, ToListMixin, ColModeMixin
):
    """
    Fit and transform an unsupervised sklearn model.
    """

    # pylint: disable=too-many-ancestors

    def __init__(
        self,
        nid: str,
        in_col_group: Tuple[_COL_TYPE],
        out_col_group: Tuple[_COL_TYPE],
        model_func: Callable[..., Any],
        model_kwargs: Optional[Any] = None,
        nan_mode: Optional[str] = None,
    ) -> None:
        """
        Specify the data and sklearn modeling parameters.

        TODO(*): Factor out code shared with `UnsupervisedSkLearnModel`.

        :param nid: unique node id
        :param in_col_group: a group of cols specified by the first N - 1
            levels
        :param out_col_group: new output col group names. This specifies the
            names of the first N - 1 levels. The leaf_cols names remain the
            same.
        :param model_func: an sklearn model
        :param model_kwargs: parameters to forward to the sklearn model
            (e.g., regularization constants)
        """
        super().__init__(nid)
        dbg.dassert_isinstance(in_col_group, tuple)
        dbg.dassert_isinstance(out_col_group, tuple)
        dbg.dassert_eq(
            len(in_col_group),
            len(out_col_group),
            msg="Column hierarchy depth must be preserved.",
        )
        self._in_col_group = in_col_group
        self._out_col_group = out_col_group
        self._model_func = model_func
        self._model_kwargs = model_kwargs or {}
        self._model = None
        self._nan_mode = nan_mode or "raise"

    def fit(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        return self._fit_predict_helper(df_in, fit=True)

    def predict(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        return self._fit_predict_helper(df_in, fit=False)

    def get_fit_state(self) -> Dict[str, Any]:
        fit_state = {"_model": self._model, "_info['fit']": self._info["fit"]}
        return fit_state

    def set_fit_state(self, fit_state: Dict[str, Any]):
        self._model = fit_state["_model"]
        self._info["fit"] = fit_state["_info['fit']"]

    def _fit_predict_helper(
        self, df_in: pd.DataFrame, fit: bool = False
    ) -> Tuple[pd.DataFrame, collections.OrderedDict]:
        """
        Factor out common flow for fit/predict.

        :param df_in: as in `fit`/`predict`
        :param fit: fits model iff `True`
        :return: transformed df_in
        """
        self._validate_input_df(df_in)
        # After indexing by `self._in_col_group`, we should have a flat column
        # index.
        dbg.dassert_eq(
            len(self._in_col_group),
            df_in.columns.nlevels - 1,
            "Dataframe multiindex column depth incompatible with config.",
        )
        # Do not allow overwriting existing columns.
        dbg.dassert_not_in(
            self._out_col_group,
            df_in.columns,
            "Desired column names already present in dataframe.",
        )
        df = df_in[self._in_col_group].copy()
        df.index
        df = df_in.copy()
        # Determine index where no x_vars are NaN.
        x_vars = df.columns.tolist()
        non_nan_idx = df.dropna().index
        dbg.dassert(not non_nan_idx.empty)
        # Handle presence of NaNs according to `nan_mode`.
        self._handle_nans(df.index, non_nan_idx)
        # Prepare x_vars in sklearn format.
        x_fit = cdataa.transform_to_sklearn(df.loc[non_nan_idx], x_vars)
        if fit:
            # Define and fit model.
            self._model = self._model_func(**self._model_kwargs)
            self._model = self._model.fit(x_fit)
        # Generate insample transformations and put in dataflow dataframe format.
        x_transform = self._model.transform(x_fit)
        #
        num_cols = x_transform.shape[1]
        x_hat = cdataa.transform_from_sklearn(
            non_nan_idx, list(range(num_cols)), x_transform
        )
        info = collections.OrderedDict()
        info["model_x_vars"] = x_vars
        info["model_params"] = self._model.get_params()
        model_attribute_info = collections.OrderedDict()
        for k, v in vars(self._model).items():
            model_attribute_info[k] = v
        info["model_attributes"] = model_attribute_info
        # Return targets and predictions.
        df_out = x_hat.reindex(index=df_in.index)
        df_out = pd.concat([df_out], axis=1, keys=[self._out_col_group])
        df_out = df_out.merge(
            df_in,
            how="outer",
            left_index=True,
            right_index=True,
        )
        info["df_out_info"] = get_df_info_as_string(df_out)
        if fit:
            self._set_info("fit", info)
        else:
            self._set_info("predict", info)
        dbg.dassert_no_duplicates(df_out.columns)
        return {"df_out": df_out}

    def _handle_nans(
        self, idx: pd.DataFrame.index, non_nan_idx: pd.DataFrame.index
    ) -> None:
        if self._nan_mode == "raise":
            if idx.shape[0] != non_nan_idx.shape[0]:
                nan_idx = idx.difference(non_nan_idx)
                raise ValueError(f"NaNs detected at {nan_idx}")
        elif self._nan_mode == "drop":
            pass
        else:
            raise ValueError(f"Unrecognized nan_mode `{self._nan_mode}`")


class Residualizer(FitPredictNode, RegFreqMixin, ToListMixin):
    """
    Residualize using an sklearn model with `inverse_transform()`.
    """

    def __init__(
        self,
        nid: str,
        model_func: Callable[..., Any],
        x_vars: _TO_LIST_MIXIN_TYPE,
        model_kwargs: Optional[Any] = None,
        nan_mode: Optional[str] = None,
    ) -> None:
        """
        Specify the data and sklearn modeling parameters.

        :param nid: unique node id
        :param model_func: an sklearn model
        :param x_vars: indexed by knowledge datetimes
        :param model_kwargs: parameters to forward to the sklearn model
            (e.g., regularization constants)
        """
        super().__init__(nid)
        self._model_func = model_func
        self._model_kwargs = model_kwargs or {}
        self._x_vars = x_vars
        self._model = None
        self._nan_mode = nan_mode or "raise"

    def fit(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        return self._fit_predict_helper(df_in, fit=True)

    def predict(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        return self._fit_predict_helper(df_in, fit=False)

    def get_fit_state(self) -> Dict[str, Any]:
        fit_state = {"_model": self._model, "_info['fit']": self._info["fit"]}
        return fit_state

    def set_fit_state(self, fit_state: Dict[str, Any]):
        self._model = fit_state["_model"]
        self._info["fit"] = fit_state["_info['fit']"]

    def _fit_predict_helper(
        self, df_in: pd.DataFrame, fit: bool = False
    ) -> Dict[str, pd.DataFrame]:
        """
        Factor out common flow for fit/predict.

        :param df_in: as in `fit`/`predict`
        :param fit: fits model iff `True`
        :return: transformed df_in
        """
        self._validate_input_df(df_in)
        df = df_in.copy()
        # Determine index where no x_vars are NaN.
        x_vars = self._to_list(self._x_vars)
        non_nan_idx = df[x_vars].dropna().index
        dbg.dassert(not non_nan_idx.empty)
        # Handle presence of NaNs according to `nan_mode`.
        self._handle_nans(df.index, non_nan_idx)
        # Prepare x_vars in sklearn format.
        x_fit = cdataa.transform_to_sklearn(df.loc[non_nan_idx], x_vars)
        if fit:
            # Define and fit model.
            self._model = self._model_func(**self._model_kwargs)
            self._model = self._model.fit(x_fit)
        # Generate insample transformations and put in dataflow dataframe format.
        x_transform = self._model.transform(x_fit)
        x_hat = self._model.inverse_transform(x_transform)
        #
        x_residual = cdataa.transform_from_sklearn(
            non_nan_idx, x_vars, x_fit - x_hat
        )
        info = collections.OrderedDict()
        info["model_x_vars"] = x_vars
        info["model_params"] = self._model.get_params()
        model_attribute_info = collections.OrderedDict()
        for k, v in vars(self._model).items():
            model_attribute_info[k] = v
        info["model_attributes"] = model_attribute_info
        df_out = x_residual.reindex(index=df_in.index)
        info["df_out_info"] = get_df_info_as_string(df_out)
        if fit:
            self._set_info("fit", info)
        else:
            self._set_info("predict", info)
        # Return targets and predictions.
        return {"df_out": df_out}

    def _handle_nans(
        self, idx: pd.DataFrame.index, non_nan_idx: pd.DataFrame.index
    ) -> None:
        if self._nan_mode == "raise":
            if idx.shape[0] != non_nan_idx.shape[0]:
                nan_idx = idx.difference(non_nan_idx)
                raise ValueError(f"NaNs detected at {nan_idx}")
        elif self._nan_mode == "drop":
            pass
        else:
            raise ValueError(f"Unrecognized nan_mode `{self._nan_mode}`")


class SkLearnInverseTransformer(
    FitPredictNode, RegFreqMixin, ToListMixin, ColModeMixin
):
    """
    Inverse transform cols using an unsupervised sklearn model.
    """

    # pylint: disable=too-many-ancestors

    def __init__(
        self,
        nid: str,
        model_func: Callable[..., Any],
        x_vars: _TO_LIST_MIXIN_TYPE,
        trans_x_vars: _TO_LIST_MIXIN_TYPE,
        model_kwargs: Optional[Any] = None,
        col_mode: Optional[str] = None,
        nan_mode: Optional[str] = None,
    ) -> None:
        """
        Specify the data and sklearn modeling parameters.

        :param nid: unique node id
        :param model_func: an unsupervised sklearn model with an
            `inverse_transform()` method
        :param x_vars: indexed by knowledge datetimes; the unsupervised model
            is learned on these cols
        :param trans_x_vars: the cols to apply the learned inverse transform to
        :param model_kwargs: parameters to forward to the sklearn model
            (e.g., regularization constants)
        """
        super().__init__(nid)
        self._model_func = model_func
        self._model_kwargs = model_kwargs or {}
        self._x_vars = self._to_list(x_vars)
        self._trans_x_vars = self._to_list(trans_x_vars)
        dbg.dassert_not_intersection(self._x_vars, self._trans_x_vars)
        self._model = None
        self._col_mode = col_mode or "replace_all"
        dbg.dassert_in(self._col_mode, ["replace_all", "merge_all"])
        self._nan_mode = nan_mode or "raise"

    def fit(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        return self._fit_predict_helper(df_in, fit=True)

    def predict(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        return self._fit_predict_helper(df_in, fit=False)

    def get_fit_state(self) -> Dict[str, Any]:
        fit_state = {"_model": self._model, "_info['fit']": self._info["fit"]}
        return fit_state

    def set_fit_state(self, fit_state: Dict[str, Any]):
        self._model = fit_state["_model"]
        self._info["fit"] = fit_state["_info['fit']"]

    def _fit_predict_helper(
        self, df_in: pd.DataFrame, fit: bool = False
    ) -> Dict[str, pd.DataFrame]:
        """
        Factor out common flow for fit/predict.

        :param df_in: as in `fit`/`predict`
        :param fit: fits model iff `True`
        :return: transformed df_in
        """
        self._validate_input_df(df_in)
        df = df_in.copy()
        # Determine index where no x_vars are NaN.
        x_vars = self._to_list(self._x_vars)
        non_nan_idx = df[x_vars].dropna().index
        dbg.dassert(not non_nan_idx.empty)
        # Handle presence of NaNs according to `nan_mode`.
        self._handle_nans(df.index, non_nan_idx)
        # Prepare x_vars in sklearn format.
        x_fit = cdataa.transform_to_sklearn(df.loc[non_nan_idx], x_vars)
        if fit:
            # Define and fit model.
            self._model = self._model_func(**self._model_kwargs)
            self._model = self._model.fit(x_fit)
        # Add info on unsupervised model.
        info = collections.OrderedDict()
        info["model_x_vars"] = x_vars
        info["model_params"] = self._model.get_params()
        model_attribute_info = collections.OrderedDict()
        for k, v in vars(self._model).items():
            model_attribute_info[k] = v
        info["model_attributes"] = model_attribute_info
        # Determine index where no trans_x_vars are NaN.
        trans_x_vars = self._to_list(self._trans_x_vars)
        trans_non_nan_idx = df[trans_x_vars].dropna().index
        dbg.dassert(not trans_non_nan_idx.empty)
        # Handle presence of NaNs according to `nan_mode`.
        self._handle_nans(df.index, trans_non_nan_idx)
        # Prepare trans_x_vars in sklearn format.
        trans_x_fit = cdataa.transform_to_sklearn(
            df.loc[non_nan_idx], trans_x_vars
        )
        trans_x_inv_trans = self._model.inverse_transform(trans_x_fit)
        trans_x_inv_trans = cdataa.transform_from_sklearn(
            trans_non_nan_idx, x_vars, trans_x_inv_trans
        )
        #
        df_out = trans_x_inv_trans.reindex(index=df_in.index)
        df_out = self._apply_col_mode(
            df, df_out, cols=trans_x_vars, col_mode=self._col_mode
        )
        info["df_out_info"] = get_df_info_as_string(df_out)
        if fit:
            self._set_info("fit", info)
        else:
            self._set_info("predict", info)
        dbg.dassert_no_duplicates(df_out.columns)
        return {"df_out": df_out}

    def _handle_nans(
        self, idx: pd.DataFrame.index, non_nan_idx: pd.DataFrame.index
    ) -> None:
        if self._nan_mode == "raise":
            if idx.shape[0] != non_nan_idx.shape[0]:
                nan_idx = idx.difference(non_nan_idx)
                raise ValueError(f"NaNs detected at {nan_idx}")
        elif self._nan_mode == "drop":
            pass
        else:
            raise ValueError(f"Unrecognized nan_mode `{self._nan_mode}`")
