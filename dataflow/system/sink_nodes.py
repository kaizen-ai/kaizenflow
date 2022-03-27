"""
Import as:

import dataflow.system.sink_nodes as dtfsysinod
"""


import collections
import logging
from typing import Any, Dict, Optional

import pandas as pd

import core.config as cconfig
import dataflow.core as dtfcore
import dataflow.model as dtfmod
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import oms.portfolio as omportfo
import oms.process_forecasts_ as oprofore

_LOG = logging.getLogger(__name__)


class ProcessForecasts(dtfcore.FitPredictNode):
    """
    Place trades from a model.
    """

    def __init__(
        self,
        nid: dtfcore.NodeId,
        prediction_col: str,
        volatility_col: str,
        spread_col: Optional[str],
        portfolio: omportfo.AbstractPortfolio,
        process_forecasts_config: Dict[str, Any],
        evaluate_forecasts_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Parameters have the same meaning as in `process_forecasts()`.
        """
        super().__init__(nid)
        self._prediction_col = prediction_col
        self._volatility_col = volatility_col
        self._spread_col = spread_col
        self._portfolio = portfolio
        process_forecasts_config = cconfig.get_config_from_nested_dict(
            process_forecasts_config
        )
        hdbg.dassert_isinstance(process_forecasts_config, cconfig.Config)
        self._process_forecasts_config = process_forecasts_config
        self._evaluate_forecasts_config = evaluate_forecasts_config

    def fit(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        return self._compute_forecasts(df_in, fit=True)

    def predict(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        return self._compute_forecasts(df_in, fit=False)

    async def process_forecasts(self) -> None:
        # Get the latest `df` index value.
        restrictions = None
        await oprofore.process_forecasts(
            self._prediction_df,
            self._volatility_df,
            self._portfolio,
            self._process_forecasts_config,
            self._spread_df,
            restrictions,
        )

    def _compute_forecasts(
        self, df: pd.DataFrame, fit: bool = True
    ) -> Dict[str, pd.DataFrame]:
        if self._evaluate_forecasts_config is not None:
            self._evaluate_forecasts(df)
        hdbg.dassert_in(self._prediction_col, df.columns)
        # Make sure it's multi-index.
        hdbg.dassert_lte(2, df.columns.nlevels)
        hdbg.dassert_isinstance(df.index, pd.DatetimeIndex)
        # TODO(gp): Maybe pass the entire multi-index df and the name of
        #  pred_col and vol_col.
        prediction_df = df[self._prediction_col]
        self._prediction_df = prediction_df
        _LOG.debug("prediction_df=\n%s", hpandas.df_to_str(prediction_df))
        #
        volatility_df = df[self._volatility_col]
        self._volatility_df = volatility_df
        _LOG.debug("volatility_df=\n%s", hpandas.df_to_str(volatility_df))
        #
        if self._spread_col is None:
            self._spread_df = None
            _LOG.debug("spread_df is `None`")
        else:
            spread_df = df[self._spread_col]
            self._spread_df = spread_df
            _LOG.debug("spread_df=\n%s", hpandas.df_to_str(spread_df))
        # Compute stats.
        info = collections.OrderedDict()
        info["df_out_info"] = dtfcore.get_df_info_as_string(df)
        mode = "fit" if fit else "predict"
        self._set_info(mode, info)
        # Pass the dataframe through.
        return {"df_out": df}

    def _evaluate_forecasts(self, df: pd.DataFrame) -> None:
        log_dir = self._evaluate_forecasts_config["log_dir"]
        target_gmv = self._evaluate_forecasts_config["target_gmv"]
        returns_col = self._evaluate_forecasts_config["returns_col"]
        forecast_evaluator = dtfmod.ForecastEvaluator(
            returns_col=returns_col,
            volatility_col=self._volatility_col,
            prediction_col=self._prediction_col,
        )
        forecast_evaluator.log_portfolio(
            df,
            log_dir,
            target_gmv=target_gmv,
        )
