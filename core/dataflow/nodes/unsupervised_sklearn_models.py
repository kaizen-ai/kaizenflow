import abc
import collections
import datetime
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import pandas as pd

import core.data_adapters as cdataa
import helpers.dbg as dbg
from core.dataflow.nodes.base import (
    ColModeMixin,
    CrossSectionalDfToDfColProcessor,
    FitPredictNode,
)
from core.dataflow.utils import get_df_info_as_string, validate_df_indices, convert_to_list, merge_dataframes

_LOG = logging.getLogger(__name__)


_COL_TYPE = Union[int, str]
_PANDAS_DATE_TYPE = Union[str, pd.Timestamp, datetime.datetime]
_TO_LIST_MIXIN_TYPE = Union[List[_COL_TYPE], Callable[[], List[_COL_TYPE]]]


# #############################################################################
# sklearn - unsupervised models
# #############################################################################


class AbstractUnsupervisedSkLearnModel(FitPredictNode, abc.ABC):
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
        validate_df_indices(df_in)
        df = df_in.copy()
        # Determine index where no x_vars are NaN.
        x_vars = df.columns.tolist()
        non_nan_idx = df.dropna().index
        dbg.dassert(not non_nan_idx.empty)
        # Handle presence of NaNs according to `nan_mode`.
        _handle_nans(self._nan_mode, df.index, non_nan_idx)
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
        dbg.dassert_no_duplicates(df_out.columns)
        return df_out, info


class UnsupervisedSkLearnModel(
    AbstractUnsupervisedSkLearnModel, ColModeMixin
):
    """
    Fit and transform an unsupervised sklearn model.
    """

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
        df = self._preprocess_df(df_in)
        df_out, info = self._fit_predict_helper(df, fit=True)
        df_out = self._apply_col_mode(
            df_in, df_out, cols=df.columns.to_list(), col_mode=self._col_mode
        )
        info["df_out_info"] = get_df_info_as_string(df_out)
        self._set_info("fit", info)
        return {"df_out": df_out}

    def predict(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        df = self._preprocess_df(df_in)
        df_out, info = self._fit_predict_helper(df, fit=False)
        df_out = self._apply_col_mode(
            df_in, df_out, cols=df.columns.to_list(), col_mode=self._col_mode
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

    def _preprocess_df(self, df_in):
        if self._x_vars is None:
            x_vars = df_in.columns.tolist()
        else:
            x_vars = convert_to_list(self._x_vars)
        return df_in[x_vars].copy()


class MultiindexUnsupervisedSkLearnModel(
    AbstractUnsupervisedSkLearnModel,
):
    """
    Fit and transform an unsupervised sklearn model.
    """

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
        df = CrossSectionalDfToDfColProcessor.preprocess(df_in, self._in_col_group)
        df_out, info = self._fit_predict_helper(df, fit=True)
        df_out = CrossSectionalDfToDfColProcessor.postprocess(df_out, self._out_col_group)
        df_out = merge_dataframes(df_in, df_out)
        info["df_out_info"] = get_df_info_as_string(df_out)
        self._set_info("fit", info)
        return {"df_out": df_out}

    def predict(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        df = CrossSectionalDfToDfColProcessor.preprocess(df_in, self._in_col_group)
        df_out, info = self._fit_predict_helper(df, fit=False)
        df_out = CrossSectionalDfToDfColProcessor.postprocess(df_out, self._out_col_group)
        df_out = merge_dataframes(df_in, df_out)
        info["df_out_info"] = get_df_info_as_string(df_out)
        self._set_info("predict", info)
        return {"df_out": df_out}


class Residualizer(FitPredictNode):
    """
    Residualize using an sklearn model with `inverse_transform()`.
    """

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

        :param nid: unique node id
        :param model_func: an sklearn model
        :param model_kwargs: parameters to forward to the sklearn model
            (e.g., regularization constants)
        """
        super().__init__(nid)
        self._in_col_group = in_col_group
        self._out_col_group = out_col_group
        self._model_func = model_func
        self._model_kwargs = model_kwargs or {}
        self._model = None
        self._nan_mode = nan_mode or "raise"

    def fit(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        df = CrossSectionalDfToDfColProcessor.preprocess(df_in, self._in_col_group)
        df_out, info = self._fit_predict_helper(df, fit=True)
        df_out = CrossSectionalDfToDfColProcessor.postprocess(df_out, self._out_col_group)
        df_out = merge_dataframes(df_in, df_out)
        info["df_out_info"] = get_df_info_as_string(df_out)
        self._set_info("fit", info)
        return {"df_out": df_out}

    def predict(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        df = CrossSectionalDfToDfColProcessor.preprocess(df_in, self._in_col_group)
        df_out, info = self._fit_predict_helper(df, fit=False)
        df_out = CrossSectionalDfToDfColProcessor.postprocess(df_out, self._out_col_group)
        df_out = merge_dataframes(df_in, df_out)
        info["df_out_info"] = get_df_info_as_string(df_out)
        self._set_info("predict", info)
        return {"df_out": df_out}

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
        validate_df_indices(df_in)
        df = df_in.copy()
        # Determine index where no x_vars are NaN.
        x_vars = df.columns.to_list()
        non_nan_idx = df[x_vars].dropna().index
        dbg.dassert(not non_nan_idx.empty)
        # Handle presence of NaNs according to `nan_mode`.
        _handle_nans(self._nan_mode, df.index, non_nan_idx)
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
        return df_out, info


class SkLearnInverseTransformer(
    FitPredictNode, ColModeMixin
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
        self._x_vars = convert_to_list(x_vars)
        self._trans_x_vars = convert_to_list(trans_x_vars)
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
        validate_df_indices(df_in)
        df = df_in.copy()
        # Determine index where no x_vars are NaN.
        x_vars = convert_to_list(self._x_vars)
        non_nan_idx = df[x_vars].dropna().index
        dbg.dassert(not non_nan_idx.empty)
        # Handle presence of NaNs according to `nan_mode`.
        _handle_nans(self._nan_mode, df.index, non_nan_idx)
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
        trans_x_vars = convert_to_list(self._trans_x_vars)
        trans_non_nan_idx = df[trans_x_vars].dropna().index
        dbg.dassert(not trans_non_nan_idx.empty)
        # Handle presence of NaNs according to `nan_mode`.
        _handle_nans(self._nan_mode, df.index, trans_non_nan_idx)
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
    nan_mode: str, idx: pd.DataFrame.index, non_nan_idx: pd.DataFrame.index
) -> None:
    if nan_mode == "raise":
        if idx.shape[0] != non_nan_idx.shape[0]:
            nan_idx = idx.difference(non_nan_idx)
            raise ValueError(f"NaNs detected at {nan_idx}")
    elif nan_mode == "drop":
        pass
    else:
        raise ValueError(f"Unrecognized nan_mode `{nan_mode}`")
