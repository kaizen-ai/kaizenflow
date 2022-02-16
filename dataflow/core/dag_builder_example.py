"""
Import as:

import dataflow.core.dag_builder_example as dtfcdabuex
"""

import datetime
import logging

import core.config as cconfig
import core.finance as cofinanc
import dataflow.core.dag as dtfcordag
import dataflow.core.dag_builder as dtfcodabui
import dataflow.core.nodes.sources as dtfconosou
import dataflow.core.nodes.transformers as dtfconotra
import dataflow.core.nodes.volatility_models as dtfcnovomo

_LOG = logging.getLogger(__name__)


class DagBuilderExample1(dtfcodabui.DagBuilder):
    """
    Pipeline contain a single node with a data source node factory.
    """

    def get_config_template(self) -> cconfig.Config:
        """
        Same as abstract method.
        """
        dict_ = {
            self._get_nid("load_prices"): {
                "func": lambda x: x,
            },
        }
        config = cconfig.get_config_from_nested_dict(dict_)
        return config

    def _get_dag(
        self, config: cconfig.Config, mode: str = "strict"
    ) -> dtfcordag.DAG:
        """
        Same as abstract method.
        """
        dag = dtfcordag.DAG(mode=mode)
        _LOG.debug("%s", config)
        tail_nid = None
        # # Read data.
        stage = "load_prices"
        nid = self._get_nid(stage)
        # TDOO: Do not use this node in `core`.
        node = dtfconosou.FunctionDataSource(nid, **config[nid].to_dict())
        tail_nid = self._append(dag, tail_nid, node)
        #
        _ = tail_nid
        return dag


class ReturnsBuilder(dtfcodabui.DagBuilder):
    """
    Pipeline for generating filtered returns from a given `DataSource` node.
    """

    def get_config_template(self) -> cconfig.Config:
        """
        Same as abstract method.
        """
        config = cconfig.get_config_from_nested_dict(
            {
                # Filter ATH.
                self._get_nid("rets/filter_ath"): {
                    "col_mode": "replace_all",
                    "transformer_kwargs": {
                        "start_time": datetime.time(9, 30),
                        "end_time": datetime.time(16, 00),
                    },
                },
                # Compute TWAP and VWAP.
                self._get_nid("rets/resample"): {
                    "func_kwargs": {
                        "rule": "5T",
                        "resampling_groups": [
                            (
                                {"close": "twap"},
                                "mean",
                                {},
                            ),
                        ],
                        "vwap_groups": [
                            ("close", "volume", "vwap"),
                        ],
                    },
                },
                # Calculate rets.
                self._get_nid("rets/compute_ret_0"): {
                    "cols": ["twap", "vwap"],
                    "col_mode": "merge_all",
                    "transformer_kwargs": {
                        "mode": "pct_change",
                    },
                },
                # Model volatility.
                self._get_nid("rets/model_volatility"): {
                    "cols": ["vwap_ret_0"],
                    "steps_ahead": 2,
                    "nan_mode": "leave_unchanged",
                },
                # Clip rets.
                self._get_nid("rets/clip"): {
                    "cols": ["vwap_ret_0_vol_adj"],
                    "col_mode": "replace_selected",
                },
            }
        )
        return config

    def _get_dag(
        self, config: cconfig.Config, mode: str = "strict"
    ) -> dtfcordag.DAG:
        """
        Same as abstract method.
        """
        dag = dtfcordag.DAG(mode=mode)
        _LOG.debug("%s", config)
        # Set weekends to Nan.
        stage = "rets/filter_weekends"
        nid = self._get_nid(stage)
        node = dtfconotra.ColumnTransformer(
            nid,
            transformer_func=cofinanc.set_weekends_to_nan,
            col_mode="replace_all",
        )
        tail_nid = self._append(dag, None, node)
        # Set non-ATH to NaN.
        stage = "rets/filter_ath"
        nid = self._get_nid(stage)
        node = dtfconotra.ColumnTransformer(
            nid,
            transformer_func=cofinanc.set_non_ath_to_nan,
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        # Resample.
        stage = "rets/resample"
        nid = self._get_nid(stage)
        node = dtfconotra.FunctionWrapper(
            nid,
            func=cofinanc.resample_bars,
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        # Compute returns.
        stage = "rets/compute_ret_0"
        nid = self._get_nid(stage)
        node = dtfconotra.ColumnTransformer(
            nid,
            transformer_func=cofinanc.compute_ret_0,
            col_rename_func=lambda x: x + "_ret_0",
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        # Model volatility.
        stage = "rets/model_volatility"
        nid = self._get_nid(stage)
        node = dtfcnovomo.VolatilityModel(nid, **config[nid].to_dict())
        tail_nid = self._append(dag, tail_nid, node)
        # Clip rets.
        stage = "rets/clip"
        nid = self._get_nid(stage)
        node = dtfconotra.ColumnTransformer(
            nid,
            transformer_func=lambda x: x.clip(lower=-3, upper=3),
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        _ = tail_nid
        return dag


# TODO(gp): Remove the first node from these DAG and express ArmaReturnsBuilder and
#  MvnReturnsBuilder in terms of a DagAdapter and ReturnsBuilder.
class ArmaReturnsBuilder(dtfcodabui.DagBuilder):
    """
    Pipeline for generating filtered returns from an ARMA process.
    """

    def get_config_template(self) -> cconfig.Config:
        """
        Same as abstract method.
        """
        config = cconfig.get_config_from_nested_dict(
            {
                # Load prices.
                self._get_nid("rets/read_data"): {
                    "frequency": "T",
                    "start_date": "2010-01-04 09:00:00",
                    "end_date": "2010-01-04 16:30:00",
                    "ar_coeffs": [0],
                    "ma_coeffs": [0],
                    "scale": 0.1,
                    "burnin": 0,
                    "seed": 0,
                },
                # Filter ATH.
                self._get_nid("rets/filter_ath"): {
                    "col_mode": "replace_all",
                    "transformer_kwargs": {
                        "start_time": datetime.time(9, 30),
                        "end_time": datetime.time(16, 00),
                    },
                },
                # Compute TWAP and VWAP.
                self._get_nid("rets/resample"): {
                    "func_kwargs": {
                        "rule": "5T",
                        "resampling_groups": [
                            (
                                {"close": "twap"},
                                "mean",
                                {},
                            ),
                        ],
                        "vwap_groups": [
                            ("close", "volume", "vwap"),
                        ],
                    },
                },
                # Calculate rets.
                self._get_nid("rets/compute_ret_0"): {
                    "cols": ["twap", "vwap"],
                    "col_mode": "merge_all",
                    "transformer_kwargs": {
                        "mode": "pct_change",
                    },
                },
                # Model volatility.
                self._get_nid("rets/model_volatility"): {
                    "cols": ["vwap_ret_0"],
                    "steps_ahead": 2,
                    "nan_mode": "leave_unchanged",
                },
                # Clip rets.
                self._get_nid("rets/clip"): {
                    "cols": ["vwap_ret_0_vol_adj"],
                    "col_mode": "replace_selected",
                },
            }
        )
        return config

    def _get_dag(
        self, config: cconfig.Config, mode: str = "strict"
    ) -> dtfcordag.DAG:
        """
        Same as abstract method.
        """
        dag = dtfcordag.DAG(mode=mode)
        _LOG.debug("%s", config)
        # Read data.
        stage = "rets/read_data"
        nid = self._get_nid(stage)
        node = dtfconosou.ArmaDataSource(nid, **config[nid].to_dict())
        tail_nid = self._append(dag, None, node)
        # Set weekends to Nan.
        stage = "rets/filter_weekends"
        nid = self._get_nid(stage)
        node = dtfconotra.ColumnTransformer(
            nid,
            transformer_func=cofinanc.set_weekends_to_nan,
            col_mode="replace_all",
        )
        tail_nid = self._append(dag, tail_nid, node)
        # Set non-ATH to NaN.
        stage = "rets/filter_ath"
        nid = self._get_nid(stage)
        node = dtfconotra.ColumnTransformer(
            nid,
            transformer_func=cofinanc.set_non_ath_to_nan,
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        # Resample.
        stage = "rets/resample"
        nid = self._get_nid(stage)
        node = dtfconotra.FunctionWrapper(
            nid,
            func=cofinanc.resample_bars,
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        # Compute returns.
        stage = "rets/compute_ret_0"
        nid = self._get_nid(stage)
        node = dtfconotra.ColumnTransformer(
            nid,
            transformer_func=cofinanc.compute_ret_0,
            col_rename_func=lambda x: x + "_ret_0",
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        # Model volatility.
        stage = "rets/model_volatility"
        nid = self._get_nid(stage)
        node = dtfcnovomo.VolatilityModel(nid, **config[nid].to_dict())
        tail_nid = self._append(dag, tail_nid, node)
        # Clip rets.
        stage = "rets/clip"
        nid = self._get_nid(stage)
        node = dtfconotra.ColumnTransformer(
            nid,
            transformer_func=lambda x: x.clip(lower=-3, upper=3),
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        _ = tail_nid
        return dag


class MvnReturnsBuilder(dtfcodabui.DagBuilder):
    """
    Pipeline for generating filtered returns from an Multivariate Normal
    process.
    """

    def get_config_template(self) -> cconfig.Config:
        config = cconfig.get_config_from_nested_dict(
            {
                self._get_nid("filter_ath"): {
                    "col_mode": "replace_all",
                    "transformer_kwargs": {
                        "start_time": datetime.time(9, 30),
                        "end_time": datetime.time(16, 00),
                    },
                },
                self._get_nid("resample"): {
                    "in_col_groups": [
                        ("close",),
                        ("volume",),
                    ],
                    "out_col_group": (),
                    "transformer_kwargs": {
                        "rule": "5T",
                        "resampling_groups": [
                            (
                                {"close": "close"},
                                "last",
                                {},
                            ),
                            (
                                {"close": "twap"},
                                "mean",
                                {},
                            ),
                            (
                                {
                                    "volume": "volume",
                                },
                                "sum",
                                {"min_count": 1},
                            ),
                        ],
                        "vwap_groups": [
                            ("close", "volume", "vwap"),
                        ],
                    },
                    "reindex_like_input": False,
                    "join_output_with_input": False,
                },
                self._get_nid("compute_ret_0"): {
                    "in_col_groups": [
                        ("close",),
                        ("vwap",),
                        ("twap",),
                    ],
                    "out_col_group": (),
                    "transformer_kwargs": {
                        "mode": "log_rets",
                    },
                    "col_mapping": {
                        "close": "close.ret_0",
                        "vwap": "vwap.ret_0",
                        "twap": "twap.ret_0",
                    },
                },
            },
        )
        return config

    def _get_dag(
        self, config: cconfig.Config, mode: str = "strict"
    ) -> dtfcordag.DAG:
        """
        Generate pipeline DAG.
        """
        dag = dtfcordag.DAG(mode=mode)
        _LOG.debug("%s", config)
        #
        stage = "filter_weekends"
        nid = self._get_nid(stage)
        node = dtfconotra.ColumnTransformer(
            nid,
            transformer_func=cofinanc.set_weekends_to_nan,
            col_mode="replace_all",
        )
        tail_nid = self._append(dag, None, node)
        #
        stage = "filter_ath"
        nid = self._get_nid(stage)
        node = dtfconotra.ColumnTransformer(
            nid,
            transformer_func=cofinanc.set_non_ath_to_nan,
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        #
        stage = "resample"
        nid = self._get_nid(stage)
        node = dtfconotra.GroupedColDfToDfTransformer(
            nid,
            transformer_func=cofinanc.resample_bars,
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        #
        stage = "compute_ret_0"
        nid = self._get_nid(stage)
        node = dtfconotra.GroupedColDfToDfTransformer(
            nid,
            transformer_func=cofinanc.compute_ret_0,
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        #
        _ = tail_nid
        return dag
