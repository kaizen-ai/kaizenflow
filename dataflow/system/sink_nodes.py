"""
Import as:

import dataflow.system.sink_nodes as dtfsysinod
"""


import collections
import logging
import os
from typing import Any, Dict, Optional

import pandas as pd

import dataflow.core as dtfcore
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import oms.portfolio as omportfo
import oms.process_forecasts_ as oprofore

_LOG = logging.getLogger(__name__)


class ProcessForecastsNode(dtfcore.FitPredictNode):
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
        process_forecasts_dict: Dict[str, Any],
    ) -> None:
        """
        Parameters have the same meaning as in `oms/process_forecasts_()`.

        :param process_forecasts_dict: configures `process_forecasts()`
        """
        _LOG.debug(
            hprint.to_str(
                "nid prediction_col volatility_col "
                "spread_col portfolio process_forecasts_dict"
            )
        )
        super().__init__(nid)
        self._prediction_col = prediction_col
        self._volatility_col = volatility_col
        self._spread_col = spread_col
        self._portfolio = portfolio
        hdbg.dassert_isinstance(process_forecasts_dict, dict)
        self._process_forecasts_dict = process_forecasts_dict

    def fit(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        return self._compute_forecasts(df_in, fit=True)

    def predict(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        return self._compute_forecasts(df_in, fit=False)

    async def process_forecasts(self) -> None:
        # Get the latest `df` index value.
        restrictions_df = None
        await oprofore.process_forecasts(
            self._prediction_df,
            self._volatility_df,
            self._portfolio,
            self._process_forecasts_dict,
            spread_df=self._spread_df,
            restrictions_df=restrictions_df,
        )

    def _compute_forecasts(
        self, df: pd.DataFrame, fit: bool = True
    ) -> Dict[str, pd.DataFrame]:
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


# #############################################################################
# Dict builders.
# #############################################################################


# TODO(Grisha): @all Move to `system_builder_utils.py` or sink_nodes_example.py
#   This function can become `get_ProcessForecastsNode_dict_example` (without a
#   number) which signify the innermost / most general builder.
def get_ProcessForecastsNode_dict_example1(
    portfolio: omportfo.Portfolio,
    prediction_col: str,
    volatility_col: str,
    spread_col: Optional[str],
    order_duration_in_mins: int,
    style: str,
    compute_target_positions_kwargs: Dict[str, Any],
    root_log_dir: Optional[str],
) -> Dict[str, Any]:
    """
    Get the config for `ProcessForecastNode`.

    :param root_log_dir: the root directory in which to log data. This function
        saves the data in `$root_log_dir/process_forecasts`, and then each related
        object decides where to save its own data underneath the
        `process_forecasts()` log dir
    """
    hdbg.dassert_isinstance(portfolio, omportfo.Portfolio)
    #
    order_type = "price@twap"
    if root_log_dir is not None:
        log_dir = os.path.join(root_log_dir, "process_forecasts")
    else:
        log_dir = None
    process_forecasts_dict = {
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
        "execution_mode": "real_time",
        "log_dir": log_dir,
    }
    # Params for `ProcessForecastsNode`.
    process_forecasts_node_dict = {
        "prediction_col": prediction_col,
        "volatility_col": volatility_col,
        "spread_col": spread_col,
        "portfolio": portfolio,
        # This configures `process_forecasts()`.
        "process_forecasts_dict": process_forecasts_dict,
    }
    return process_forecasts_node_dict