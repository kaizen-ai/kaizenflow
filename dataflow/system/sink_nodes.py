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
import oms.process_forecasts as oprofore

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
        portfolio: omportfo.Portfolio,
        process_forecasts_config: Dict[str, Any],
        *,
        evaluate_forecasts_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Parameters have the same meaning as in `process_forecasts()`.

        :param process_forecasts_config: configures `process_forecasts()`
        :param evaluate_forecasts_config: if not None, it configures
            `ForecastEvaluatorFromPrices` which computes the vectorized shadow
            PnL
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
        # TODO(gp): We should pass a single dict.
        target_gmv = self._evaluate_forecasts_config["target_gmv"]
        price_col = self._evaluate_forecasts_config["price_col"]
        forecast_evaluator = dtfmod.ForecastEvaluatorFromPrices(
            price_col=price_col,
            volatility_col=self._volatility_col,
            prediction_col=self._prediction_col,
        )
        #
        log_dir = self._evaluate_forecasts_config["log_dir"]
        _LOG.info("log_dir=%s", log_dir)
        forecast_evaluator.log_portfolio(
            df,
            log_dir,
            target_gmv=target_gmv,
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
    *,
    bulk_frac_to_remove: float = 0.0,
    target_gmv: float = 1e5,
    log_dir: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Get the config for `ProcessForecast` node.
    """
    if log_dir is not None:
        # Params for `ForecastEvaluatorFromPrice`, which computes the pnl with
        # the vectorized PnL that we run in parallel.
        evaluate_forecasts_config_dict = {
            "log_dir": os.path.join(log_dir, "evaluate_forecasts"),
            "bulk_frac_to_remove": bulk_frac_to_remove,
            "target_gmv": target_gmv,
            "price_col": price_col,
        }
    else:
        evaluate_forecasts_config_dict = None
    #
    order_type = "price@twap"
    process_forecasts_config_dict = {
        # Params for `ForecastProcessor`.
        "order_config": {
            "order_type": order_type,
            # TODO(gp): pass this
            "order_duration": 5,
        },
        "optimizer_config": {
            "backend": "pomo",
            "bulk_frac_to_remove": bulk_frac_to_remove,
            "bulk_fill_method": "zero",
            "target_gmv": target_gmv,
        },
        # Params for `process_forecasts()`.
        # TODO(gp): Use datetime.time()
        "ath_start_time": pd.Timestamp(
            "2000-01-01 09:30:00-05:00", tz="America/New_York"
        ).time(),
        "trading_start_time": pd.Timestamp(
            "2000-01-01 09:30:00-05:00", tz="America/New_York"
        ).time(),
        "ath_end_time": pd.Timestamp(
            "2000-01-01 16:40:00-05:00", tz="America/New_York"
        ).time(),
        "trading_end_time": pd.Timestamp(
            "2000-01-01 16:40:00-05:00", tz="America/New_York"
        ).time(),
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
        "evaluate_forecasts_config": evaluate_forecasts_config_dict,
    }
    return process_forecasts_dict


def get_process_forecasts_dict_example2(
    portfolio: omportfo.Portfolio,
) -> Dict[str, Any]:
    prediction_col = "prediction"
    volatility_col = "vwap.ret_0.vol"
    price_col = "vwap"
    spread_col = "pct_bar_spread"
    bulk_frac_to_remove = 0.0
    target_gmv = 1e5
    # log_dir = None
    log_dir = os.path.join("process_forecasts", datetime.date.today().isoformat())
    #
    process_forecasts_dict = get_process_forecasts_dict_example1(
        portfolio,
        prediction_col,
        volatility_col,
        price_col,
        spread_col,
        bulk_frac_to_remove=bulk_frac_to_remove,
        target_gmv=target_gmv,
        log_dir=log_dir,
    )
    return process_forecasts_dict
