"""
Import as:

import dataflow.system.sink_nodes as dtfsysinod
"""


import collections
import datetime
import logging
import os
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


# TODO(gp): -> ProcessForecastsNode to distinguish from process_forecasts?
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
        portfolio: omportfo.Portfolio,
        process_forecasts_config: Dict[str, Any],
        *,
        forecast_evaluator_from_prices_dict: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Parameters have the same meaning as in `process_forecasts()`.

        :param process_forecasts_config: configures `process_forecasts()`
        :param forecast_evaluator_from_prices_dict: if not None, it configures
            `ForecastEvaluatorFromPrices` which computes the vectorized shadow
            PnL
        """
        super().__init__(nid)
        self._prediction_col = prediction_col
        self._volatility_col = volatility_col
        self._spread_col = spread_col
        self._portfolio = portfolio
        if isinstance(process_forecasts_config, dict):
            process_forecasts_config = cconfig.get_config_from_nested_dict(process_forecasts_config)
        print("process_forecasts_config", process_forecasts_config)
        self._process_forecasts_config = process_forecasts_config
        self._forecast_evaluator_from_prices_dict = (
            forecast_evaluator_from_prices_dict
        )

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
        if self._forecast_evaluator_from_prices_dict is not None:
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
        forecast_evaluator = dtfmod.ForecastEvaluatorFromPrices(
            **self._forecast_evaluator_from_prices_dict["init"]
        )
        #
        log_dir = self._forecast_evaluator_from_prices_dict["log_dir"]
        _LOG.info("log_dir=%s", log_dir)
        forecast_evaluator.log_portfolio(
            df,
            log_dir,
            **self._forecast_evaluator_from_prices_dict["kwargs"],
        )


# #############################################################################
# Dict builders.
# #############################################################################


def get_process_forecasts_dict_example1(
    portfolio: omportfo.Portfolio,
    prediction_col: str,
    volatility_col: str,
    price_col: str,
    spread_col: Optional[str],
    order_duration_in_mins: int,
    style: str,
    compute_target_positions_kwargs: Dict[str, Any],
    log_dir: str,
) -> Dict[str, Any]:
    """
    Get the config for `ProcessForecast` node.
    """
    hdbg.dassert_isinstance(portfolio, omportfo.Portfolio)
    # TODO(gp): It's unclear if we should be able to enable or not
    # ForecastEvaluatorFromPrice.
    if log_dir is not None:
        # Params for `ForecastEvaluatorFromPrice`, which computes the pnl with
        # the vectorized PnL that we run in parallel.
        forecast_evaluator_from_prices_dict = {
            "init": {
                "price_col": price_col,
                "volatility_col": volatility_col,
                "prediction_col": prediction_col,
                "spread_col": spread_col,
            },
            "log_dir": os.path.join(log_dir, "evaluate_forecasts"),
            "kwargs": compute_target_positions_kwargs,
        }
    else:
        forecast_evaluator_from_prices_dict = None
    #
    order_type = "price@twap"
    process_forecasts_config_dict = {
        # Params for `ForecastProcessor`.
        "order_config": {
            "order_type": order_type,
            "order_duration_in_mins": order_duration_in_mins,
        },
        "optimizer_config": {
            "backend": "pomo",
            "params": {
                "style": style,
                "kwargs": compute_target_positions_kwargs,
            },
        },
        # Params for `process_forecasts()`.
        "ath_start_time": datetime.time(9, 30),
        "trading_start_time": datetime.time(9, 30),
        "ath_end_time": datetime.time(16, 40),
        "trading_end_time": datetime.time(16, 40),
        "execution_mode": "real_time",
        "log_dir": log_dir,
    }
    # This goes to the `ProcessForecasts` node.
    process_forecasts_dict = {
        "prediction_col": prediction_col,
        "volatility_col": volatility_col,
        "spread_col": spread_col,
        "portfolio": portfolio,
        # This configures `process_forecasts()`.
        "process_forecasts_config": process_forecasts_config_dict,
        # This configures `ForecastEvaluatorFromPrices`.
        "forecast_evaluator_from_prices_dict": forecast_evaluator_from_prices_dict,
    }
    return process_forecasts_dict


def get_process_forecasts_dict_example2(
    portfolio: omportfo.Portfolio,
    order_duration_in_mins: int,
) -> Dict[str, Any]:
    """
    Used by E8d.
    """
    prediction_col = "prediction"
    volatility_col = "vwap.ret_0.vol"
    price_col = "vwap"
    spread_col = "pct_bar_spread"
    style = "cross_sectional"
    #
    compute_target_positions_kwargs = {
        "bulk_frac_to_remove": 0.0,
        "target_gmv": 1e5,
    }
    log_dir = os.path.join("process_forecasts", datetime.date.today().isoformat())
    #
    process_forecasts_dict = get_process_forecasts_dict_example1(
        portfolio,
        prediction_col,
        volatility_col,
        price_col,
        spread_col,
        order_duration_in_mins,
        style,
        compute_target_positions_kwargs,
        log_dir=log_dir,
    )
    return process_forecasts_dict


def get_process_forecasts_dict_example3(
    portfolio: omportfo.Portfolio,
    order_duration_in_mins: int,
) -> Dict[str, Any]:
    """
    Used by E8f.
    """
    prediction_col = "prediction"
    volatility_col = "garman_klass_vol"
    price_col = "close_vwap"
    spread_col = None
    style = "cross_sectional"
    #
    compute_target_positions_kwargs = {
        "bulk_frac_to_remove": 0.0,
        "target_gmv": 1e5,
    }
    log_dir = os.path.join("process_forecasts", datetime.date.today().isoformat())
    #
    process_forecasts_dict = get_process_forecasts_dict_example1(
        portfolio,
        prediction_col,
        volatility_col,
        price_col,
        spread_col,
        order_duration_in_mins,
        style,
        compute_target_positions_kwargs,
        log_dir=log_dir,
    )
    return process_forecasts_dict
