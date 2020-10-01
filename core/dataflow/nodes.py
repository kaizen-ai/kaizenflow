import abc
import collections
import copy
import datetime
import functools
import inspect
import io
import logging
import os
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

import pandas as pd

import core.finance as fin
import core.signal_processing as sigp
import core.statistics as stats
import helpers.dbg as dbg

# TODO(*): This is an exception to the rule waiting for PartTask553.
from core.dataflow.core import DAG, Node

_LOG = logging.getLogger(__name__)


_PANDAS_DATE_TYPE = Union[str, pd.Timestamp, datetime.datetime]


# #############################################################################
# DAG visiting
# #############################################################################


def extract_info(dag: DAG, methods: List[str]) -> collections.OrderedDict:
    """
    Extract node info from each DAG node.

    :param dag: Dag. Node info is populated upon running.
    :param methods: Node method infos to extract
    :return: nested OrderedDict
    """
    dbg.dassert_isinstance(dag, DAG)
    dbg.dassert_isinstance(methods, list)
    info = collections.OrderedDict()
    for nid in dag.dag.nodes():
        node_info = collections.OrderedDict()
        for method in methods:
            method_info = dag.get_node(nid).get_info(method)
            node_info[method] = copy.copy(method_info)
        info[nid] = copy.copy(node_info)
    return info


# #############################################################################
# Abstract Node classes with sklearn-style interfaces
# #############################################################################


class FitPredictNode(Node, abc.ABC):
    """
    Define an abstract class with sklearn-style `fit` and `predict` functions.

    Nodes may store a dictionary of information for each method following the
    method's invocation.
    """

    def __init__(
        self,
        nid: str,
        inputs: Optional[List[str]] = None,
        outputs: Optional[List[str]] = None,
    ) -> None:
        if inputs is None:
            inputs = ["df_in"]
        if outputs is None:
            outputs = ["df_out"]
        super().__init__(nid=nid, inputs=inputs, outputs=outputs)
        self._info = collections.OrderedDict()

    @abc.abstractmethod
    def fit(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        pass

    @abc.abstractmethod
    def predict(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        pass

    def get_info(
        self, method: str
    ) -> Optional[Union[str, collections.OrderedDict]]:
        # TODO(Paul): Add a dassert_getattr function to use here and in core.
        dbg.dassert_isinstance(method, str)
        dbg.dassert(getattr(self, method))
        if method in self._info.keys():
            return self._info[method]
        # TODO(Paul): Maybe crash if there is no info.
        _LOG.warning("No info found for nid=%s, method=%s", self.nid, method)
        return None

    def _set_info(self, method: str, values: collections.OrderedDict) -> None:
        dbg.dassert_isinstance(method, str)
        dbg.dassert(getattr(self, method))
        dbg.dassert_isinstance(values, collections.OrderedDict)
        # Save the info in the node: we make a copy just to be safe.
        self._info[method] = copy.copy(values)


class DataSource(FitPredictNode, abc.ABC):
    """
    A source node that can be configured for cross-validation.
    """

    def __init__(self, nid: str, outputs: Optional[List[str]] = None) -> None:
        if outputs is None:
            outputs = ["df_out"]
        # Do not allow any empty list.
        dbg.dassert(outputs)
        super().__init__(nid, inputs=[], outputs=outputs)
        #
        self.df = None
        self._fit_intervals = None
        self._predict_idxs = None

    def set_fit_intervals(self, intervals: List[Tuple[Any, Any]]) -> None:
        """
        :param intervals: closed time intervals like [start1, end1],
            [start2, end2]. `None` boundary is interpreted as data start/end
        """
        self._validate_intervals(intervals)
        self._fit_intervals = intervals

    # DataSource does not have a `df_in` in either `fit` or `predict` as a
    # typical `FitPredictNode` does.
    # pylint: disable=arguments-differ
    def fit(self) -> Dict[str, pd.DataFrame]:
        """
        :return: training set as df
        """
        if self._fit_intervals is not None:
            idx_slices = [
                self.df.loc[interval[0] : interval[1]].index
                for interval in self._fit_intervals
            ]
            idx = functools.reduce(lambda x, y: x.union(y), idx_slices)
            fit_df = self.df.loc[idx].copy()
        else:
            fit_df = self.df.copy()
        info = collections.OrderedDict()
        info["fit_df_info"] = get_df_info_as_string(fit_df)
        self._set_info("fit", info)
        dbg.dassert(not fit_df.empty)
        return {self.output_names[0]: fit_df}

    def set_predict_intervals(self, intervals: List[Tuple[Any, Any]]) -> None:
        """
        :param intervals: closed time intervals like [start1, end1],
            [start2, end2]. `None` boundary is interpreted as data start/end

        TODO(*): Warn if intervals overlap with `fit` intervals.
        TODO(*): Maybe enforce that the intervals be ordered.
        """
        self._validate_intervals(intervals)
        self._predict_intervals = intervals

    # pylint: disable=arguments-differ
    def predict(self) -> Dict[str, pd.DataFrame]:
        """
        :return: test set as df
        """
        if self._predict_intervals is not None:
            idx_slices = [
                self.df.loc[interval[0] : interval[1]].index
                for interval in self._predict_intervals
            ]
            idx = functools.reduce(lambda x, y: x.union(y), idx_slices)
            predict_df = self.df.loc[idx].copy()
        else:
            predict_df = self.df.copy()
        info = collections.OrderedDict()
        info["predict_df_info"] = get_df_info_as_string(predict_df)
        self._set_info("predict", info)
        dbg.dassert(not predict_df.empty)
        return {self.output_names[0]: predict_df}

    def get_df(self) -> pd.DataFrame:
        dbg.dassert_is_not(self.df, None, "No DataFrame found!")
        return self.df

    @staticmethod
    def _validate_intervals(intervals: List[Tuple[Any, Any]]) -> None:
        dbg.dassert_isinstance(intervals, list)
        for interval in intervals:
            dbg.dassert_eq(len(interval), 2)
            if interval[0] is not None and interval[1] is not None:
                dbg.dassert_lte(interval[0], interval[1])


class Transformer(FitPredictNode, abc.ABC):
    """
    Stateless Single-Input Single-Output node.
    """

    # TODO(Paul): Consider giving users the option of renaming the single
    # input and single output (but verify there is only one of each).
    def __init__(self, nid: str) -> None:
        super().__init__(nid)

    @abc.abstractmethod
    def _transform(
        self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, collections.OrderedDict]:
        """
        :return: df, info
        """

    def fit(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        dbg.dassert_no_duplicates(df_in.columns)
        # Transform the input df.
        df_out, info = self._transform(df_in)
        self._set_info("fit", info)
        dbg.dassert_no_duplicates(df_out.columns)
        return {"df_out": df_out}

    def predict(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        dbg.dassert_no_duplicates(df_in.columns)
        # Transform the input df.
        df_out, info = self._transform(df_in)
        self._set_info("predict", info)
        dbg.dassert_no_duplicates(df_out.columns)
        return {"df_out": df_out}


# #############################################################################
# Data source nodes
# #############################################################################


class ReadDataFromDf(DataSource):
    def __init__(self, nid: str, df: pd.DataFrame) -> None:
        super().__init__(nid)
        dbg.dassert_isinstance(df, pd.DataFrame)
        self.df = df


class DiskDataSource(DataSource):
    def __init__(
        self,
        nid: str,
        file_path: str,
        timestamp_col: Optional[str] = None,
        start_date: Optional[_PANDAS_DATE_TYPE] = None,
        end_date: Optional[_PANDAS_DATE_TYPE] = None,
        reader_kwargs: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Create data source node reading CSV or parquet data from disk.

        :param nid: node identifier
        :param file_path: path to the file
        # TODO(*): Don't the readers support this already?
        :param timestamp_col: name of the timestamp column. If `None`, assume
            that index contains timestamps
        :param start_date: data start date in timezone of the dataset, included
        :param end_date: data end date in timezone of the dataset, included
        :param reader_kwargs: kwargs for the data reading function
        """
        super().__init__(nid)
        self._file_path = file_path
        self._timestamp_col = timestamp_col
        self._start_date = start_date
        self._end_date = end_date
        self._reader_kwargs = reader_kwargs or {}

    def _read_data(self) -> None:
        ext = os.path.splitext(self._file_path)[-1]
        if ext == ".csv":
            if "index_col" not in self._reader_kwargs:
                self._reader_kwargs["index_col"] = 0
            read_data = pd.read_csv
        elif ext == ".pq":
            read_data = pd.read_parquet
        else:
            raise ValueError("Invalid file extension='%s'" % ext)
        self.df = read_data(self._file_path, **self._reader_kwargs)

    def _process_data(self) -> None:
        if self._timestamp_col is not None:
            self.df.set_index(self._timestamp_col, inplace=True)
        self.df.index = pd.to_datetime(self.df.index)
        dbg.dassert_strictly_increasing_index(self.df)
        self.df = self.df.loc[self._start_date : self._end_date]
        dbg.dassert(not self.df.empty, "Dataframe is empty")

    def _lazy_load(self) -> None:
        if self.df is not None:
            return
        self._read_data()
        self._process_data()

    def fit(self) -> Optional[Dict[str, pd.DataFrame]]:
        """
        :return: training set as df
        """
        self._lazy_load()
        return super().fit()


# #############################################################################
# Plumbing nodes
# #############################################################################


class YConnector(FitPredictNode):
    """
    Create an output dataframe from two input dataframes.
    """

    # TODO(Paul): Support different input/output names.
    def __init__(
        self,
        nid: str,
        connector_func: Callable[..., pd.DataFrame],
        connector_kwargs: Optional[Any] = None,
    ) -> None:
        """
        :param nid: unique node id
        :param connector_func:
            * Merge
            ```
            connector_func = lambda df_in1, df_in2, **connector_kwargs:
                df_in1.merge(df_in2, **connector_kwargs)
            ```
            * Reindexing
            ```
            connector_func = lambda df_in1, df_in2, connector_kwargs:
                df_in1.reindex(index=df_in2.index, **connector_kwargs)
            ```
            * User-defined functions
            ```
            # my_func(df_in1, df_in2, **connector_kwargs)
            connector_func = my_func
            ```
        :param connector_kwargs: kwargs associated with `connector_func`
        """
        super().__init__(nid, inputs=["df_in1", "df_in2"])
        self._connector_func = connector_func
        self._connector_kwargs = connector_kwargs or {}
        self._df_in1_col_names = None
        self._df_in2_col_names = None

    def get_df_in1_col_names(self) -> List[str]:
        """
        Allow introspection on column names of input dataframe #1.
        """
        return self._get_col_names(self._df_in1_col_names)

    def get_df_in2_col_names(self) -> List[str]:
        """
        Allow introspection on column names of input dataframe #2.
        """
        return self._get_col_names(self._df_in2_col_names)

    # pylint: disable=arguments-differ
    def fit(
        self, df_in1: pd.DataFrame, df_in2: pd.DataFrame
    ) -> Dict[str, pd.DataFrame]:
        df_out, info = self._apply_connector_func(df_in1, df_in2)
        self._set_info("fit", info)
        return {"df_out": df_out}

    # pylint: disable=arguments-differ
    def predict(
        self, df_in1: pd.DataFrame, df_in2: pd.DataFrame
    ) -> Dict[str, pd.DataFrame]:
        df_out, info = self._apply_connector_func(df_in1, df_in2)
        self._set_info("fit", info)
        return {"df_out": df_out}

    def _apply_connector_func(
        self, df_in1: pd.DataFrame, df_in2: pd.DataFrame
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        self._df_in1_col_names = df_in1.columns.tolist()
        self._df_in2_col_names = df_in2.columns.tolist()
        # TODO(Paul): Add meaningful info.
        df_out = self._connector_func(df_in1, df_in2, **self._connector_kwargs)
        info = collections.OrderedDict()
        info["df_merged_info"] = get_df_info_as_string(df_out)
        return df_out, info

    @staticmethod
    def _get_col_names(col_names: List[str]) -> List[str]:
        dbg.dassert_is_not(
            col_names,
            None,
            "No column names. This may indicate "
            "an invocation prior to graph execution.",
        )
        return col_names


# #############################################################################
# Transformer nodes
# #############################################################################


class ColumnTransformer(Transformer):
    """
    Perform non-index modifying changes of columns.
    """

    def __init__(
        self,
        nid: str,
        transformer_func: Callable[..., pd.DataFrame],
        transformer_kwargs: Optional[Dict[str, Any]] = None,
        # TODO(Paul): May need to assume `List` instead.
        cols: Optional[Iterable[str]] = None,
        col_rename_func: Optional[Callable[[Any], Any]] = None,
        col_mode: Optional[str] = None,
        nan_mode: Optional[str] = None,
    ) -> None:
        """
        :param nid: unique node id
        :param transformer_func: df -> df. The keyword `info` (if present) is
            assumed to have a specific semantic meaning. If present,
                - An empty dict is passed in to this `info`
                - The resulting (populated) dict is included in the node's
                  `_info`
        :param transformer_kwargs: transformer_func kwargs
        :param cols: columns to transform; `None` defaults to all available.
        :param col_rename_func: function for naming transformed columns, e.g.,
            lambda x: "zscore_" + x
        :param col_mode: `merge_all`, `replace_selected`, or `replace_all`.
            Determines what columns are propagated by the node.
        :param nan_mode: `leave_unchanged` or `drop`. If `drop`, applies to all
            columns simultaneously.
        """
        super().__init__(nid)
        if cols is not None:
            dbg.dassert_isinstance(cols, list)
        self._cols = cols
        if col_rename_func is not None:
            dbg.dassert_isinstance(col_rename_func, collections.Callable)
        self._col_rename_func = col_rename_func
        self._col_mode = col_mode or "merge_all"
        self._transformer_func = transformer_func
        self._transformer_kwargs = transformer_kwargs or {}
        # Store the list of columns after the transformation.
        self._transformed_col_names = None
        self._nan_mode = nan_mode or "leave_unchanged"

    def transformed_col_names(self) -> List[str]:
        dbg.dassert_is_not(
            self._transformed_col_names,
            None,
            "No transformed column names. This may indicate "
            "an invocation prior to graph execution.",
        )
        return self._transformed_col_names

    def _transform(
        self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, collections.OrderedDict]:
        df_in = df.copy()
        df = df.copy()
        if self._cols is not None:
            df = df[self._cols]
        idx = df.index
        if self._nan_mode == "leave_unchanged":
            pass
        elif self._nan_mode == "drop":
            df = df.dropna()
        else:
            raise ValueError(f"Unrecognized `nan_mode` {self._nan_mode}")
        # Initialize container to store info (e.g., auxiliary stats) in the
        # node.
        info = collections.OrderedDict()
        # Perform the column transformation operations.
        # Introspect to see whether `_transformer_func` contains an `info`
        # parameter. If so, inject an empty dict to be populated when
        # `_transformer_func` is executed.
        func_sig = inspect.signature(self._transformer_func)
        if "info" in func_sig.parameters:
            func_info = collections.OrderedDict()
            df = self._transformer_func(
                df, info=func_info, **self._transformer_kwargs
            )
            info["func_info"] = func_info
        else:
            df = self._transformer_func(df, **self._transformer_kwargs)
        df = df.reindex(index=idx)
        # TODO(Paul): Consider supporting the option of relaxing or
        # foregoing this check.
        dbg.dassert(
            df.index.equals(df_in.index),
            "Input/output indices differ but are expected to be the same!",
        )
        # Maybe rename transformed columns.
        if self._col_rename_func is not None:
            df.rename(columns=self._col_rename_func, inplace=True)
        # Store names of transformed columns.
        self._transformed_col_names = df.columns.tolist()
        # Maybe merge transformed columns with a subset of input df columns.
        if self._col_mode == "merge_all":
            dbg.dassert(
                df.columns.intersection(df_in.columns).empty,
                "Transformed column names `%s` conflict with existing column "
                "names `%s`.",
                df.columns,
                df_in.columns,
            )
            df = df_in.merge(df, left_index=True, right_index=True)
        elif self._col_mode == "replace_selected":
            dbg.dassert(
                df.drop(self._cols, axis=1)
                .columns.intersection(df_in[self._cols].columns)
                .empty,
                "Transformed column names `%s` conflict with existing column "
                "names `%s`.",
                df.columns,
                self._cols,
            )
            df = df_in.drop(self._cols, axis=1).merge(
                df, left_index=True, right_index=True
            )
        elif self._col_mode == "replace_all":
            pass
        else:
            dbg.dfatal("Unsupported column mode `%s`", self._col_mode)
        #
        info["df_transformed_info"] = get_df_info_as_string(df)
        return df, info


class DataframeMethodRunner(Transformer):
    def __init__(
        self,
        nid: str,
        method: str,
        method_kwargs: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(nid)
        dbg.dassert(method)
        # TODO(Paul): Ensure that this is a valid method.
        self._method = method
        self._method_kwargs = method_kwargs or {}

    def _transform(
        self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, collections.OrderedDict]:
        df = df.copy()
        df = getattr(df, self._method)(**self._method_kwargs)
        # Not all methods return DataFrames. We want to restrict to those that
        # do.
        dbg.dassert_isinstance(df, pd.DataFrame)
        #
        info = collections.OrderedDict()
        info["df_transformed_info"] = get_df_info_as_string(df)
        return df, info


class Resample(Transformer):
    def __init__(
        self,
        nid: str,
        rule: Union[pd.DateOffset, pd.Timedelta, str],
        agg_func: str,
        resample_kwargs: Optional[Dict[str, Any]] = None,
        agg_func_kwargs: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        :param nid: node identifier
        :param rule: resampling frequency passed into
            pd.DataFrame.resample
        :param agg_func: a function that is applied to the resampler
        :param resample_kwargs: kwargs for `resample`. Should not include
            `rule` since we handle this separately.
        :param agg_func_kwargs: kwargs for agg_func
        """
        super().__init__(nid)
        self._rule = rule
        self._agg_func = agg_func
        self._resample_kwargs = resample_kwargs or {}
        self._agg_func_kwargs = agg_func_kwargs or {}

    def _transform(
        self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, collections.OrderedDict]:
        df = df.copy()
        resampler = sigp.resample(df, rule=self._rule, **self._resample_kwargs)
        df = getattr(resampler, self._agg_func)(**self._agg_func_kwargs)
        #
        info: collections.OrderedDict[str, Any] = collections.OrderedDict()
        info["df_transformed_info"] = get_df_info_as_string(df)
        return df, info


# #############################################################################
# Results processing
# #############################################################################


class VolatilityNormalizer(FitPredictNode):
    def __init__(
        self,
        nid: str,
        col: str,
        target_volatility: float,
        col_mode: Optional[str] = None,
    ) -> None:
        """
        Normalize series to target annual volatility.

        :param nid: node identifier
        :param col: name of column to rescale
        :param target_volatility: target volatility as a proportion
        :param col_mode: `merge_all` or `replace_all`. If `replace_all`, return
            only the rescaled column, if `merge_all`, append the rescaled
            column to input dataframe
        """
        super().__init__(nid)
        self._col = col
        self._target_volatility = target_volatility
        self._col_mode = col_mode or "merge_all"
        dbg.dassert_in(
            self._col_mode,
            ["merge_all", "replace_all"],
            "Invalid `col_mode`='%s'",
            self._col_mode,
        )
        self._scale_factor: Optional[float] = None

    def fit(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        dbg.dassert_in(self._col, df_in.columns)
        self._scale_factor = fin.compute_volatility_normalization_factor(
            df_in[self._col], self._target_volatility
        )
        rescaled_y_hat = self._scale_factor * df_in[self._col]
        df_out = self._form_output_df(df_in, rescaled_y_hat)
        # Store info.
        info = collections.OrderedDict()
        info["scale_factor"] = self._scale_factor
        self._set_info("fit", info)
        return {"df_out": df_out}

    def predict(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        dbg.dassert_in(self._col, df_in.columns)
        rescaled_y_hat = self._scale_factor * df_in[self._col]
        df_out = self._form_output_df(df_in, rescaled_y_hat)
        return {"df_out": df_out}

    def _form_output_df(
        self, df_in: pd.DataFrame, srs: pd.Series
    ) -> pd.DataFrame:
        srs.name = f"rescaled_{srs.name}"
        # Maybe merge transformed columns with a subset of input df columns.
        if self._col_mode == "merge_all":
            dbg.dassert_not_in(
                srs.name,
                df_in.columns,
                "'%s' is already in `df_in` columns.",
                srs.name,
            )
            df_out = df_in.merge(srs, left_index=True, right_index=True)
        elif self._col_mode == "replace_all":
            df_out = srs.to_frame()
        else:
            raise ValueError("Invalid `col_mode`='%s'" % self._col_mode)
        return df_out


# #############################################################################
# Cross-validation
# #############################################################################


def _get_source_idxs(dag: DAG, mode: Optional[str] = None) -> Dict[str, pd.Index]:
    """
    Warm up source nodes and extract dataframe indices.

    :param mode: Determines how source indices extracted from dataframes
        - `default`: return index as-is
        - `dropna`: drop NaNs (any) and then return index
        - `ffill_dropna`: forward fill NaNs, drop leading NaNs, then return
                          index
    """
    mode = mode or "default"
    # Warm up source nodes to get dataframes from which we can generate splits.
    source_nids = dag.get_sources()
    for nid in source_nids:
        dag.run_leq_node(nid, "fit")
    # Collect source dataframe indices.
    source_idxs = {}
    for nid in source_nids:
        df = dag.get_node(nid).get_df()
        if mode == "default":
            source_idxs[nid] = df.index
        elif mode == "dropna":
            source_idxs[nid] = df.dropna().index
        elif mode == "ffill_dropna":
            source_idxs[nid] = df.fillna(method="ffill").dropna().index
        else:
            raise ValueError("Unsupported mode `%s`" % mode)
    return source_idxs


# TODO(Paul): Formalize what this returns and what can be done with it.
def cross_validate(
    dag: DAG,
    split_func: Callable,
    split_func_kwargs: Dict,
    idx_mode: Optional[str] = None,
) -> collections.OrderedDict:
    """
    Generate splits, run train/test, collect info.

    :param idx_mode: same meaning as mode in `_get_source_idxs`
    :return: DAG info for each split, keyed by split
    """
    # Get dataframe indices of source nodes.
    source_idxs = _get_source_idxs(dag, mode=idx_mode)
    composite_idx = stats.combine_indices(source_idxs.values())
    # Generate cross-validation splits from
    splits = split_func(composite_idx, **split_func_kwargs)
    _LOG.debug(stats.convert_splits_to_string(splits))
    #
    result_bundle = collections.OrderedDict()
    # TODO(Paul): rename train/test to fit/predict.
    for i, (train_idxs, test_idxs) in enumerate(splits):
        split_info = collections.OrderedDict()
        for nid, idx in source_idxs.items():
            node_info = collections.OrderedDict()
            node_train_idxs = idx.intersection(train_idxs)
            dbg.dassert_lte(1, node_train_idxs.size)
            node_info["fit_idxs"] = node_train_idxs
            dag.get_node(nid).set_fit_idxs(node_train_idxs)
            #
            node_test_idxs = idx.intersection(test_idxs)
            dbg.dassert_lte(1, node_test_idxs.size)
            node_info["predict_idxs"] = node_test_idxs
            dag.get_node(nid).set_predict_idxs(node_test_idxs)
            #
            split_info[nid] = node_info
        dag.run_dag("fit")
        dag.run_dag("predict")
        #
        split_info["stages"] = extract_info(dag, ["fit", "predict"])
        result_bundle["split_" + str(i)] = split_info
    return result_bundle


def process_result_bundle(result_bundle: Dict) -> collections.OrderedDict:
    info = collections.OrderedDict()
    split_names = []
    model_coeffs = []
    model_x_vars = []
    pnl_rets = []
    for split in result_bundle.keys():
        split_names.append(split)
        # model_coeffs.append(
        #    result_bundle[split]["stages"]["model"]["fit"]["model_coeffs"]
        # )
        model_x_vars.append(
            result_bundle[split]["stages"]["model"]["fit"]["model_x_vars"]
        )
        pnl_rets.append(
            result_bundle[split]["stages"]["model"]["predict"]["model_perf"][
                "pnl_rets"
            ]
        )
    model_df = pd.DataFrame(
        model_coeffs, index=split_names, columns=model_x_vars[0]
    )
    pnl_rets = pd.concat(pnl_rets)
    sr = stats.compute_sharpe_ratio(
        sigp.resample(pnl_rets, rule="1B").sum(), time_scaling=252
    )
    info["model_df"] = copy.copy(model_df)
    info["pnl_rets"] = copy.copy(pnl_rets)
    info["sr"] = copy.copy(sr)
    return info


def get_df_info_as_string(df: pd.DataFrame) -> str:
    buffer = io.StringIO()
    df.info(buf=buffer)
    return buffer.getvalue()
