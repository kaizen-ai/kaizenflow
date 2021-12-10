"""
Import as:

import dataflow.system.dataflow_sink_nodes as dtfsdtfsino
"""

import collections
import logging
from typing import Any, Dict

import pandas as pd

import dataflow as dtf
import helpers.dbg as hdbg
import oms.place_orders as oplaorde

_LOG = logging.getLogger(__name__)


class ProcessForecasts(dtf.FitPredictNode):
    """
    Place trades from a model.
    """

    def __init__(
        self,
        nid: dtf.NodeId,
        prediction_col: str,
        execution_mode: bool,
        process_forecasts_config: Dict[str, Any],
    ) -> None:
        """
        Parameters have the same meaning as in `process_forecasts()`.
        """
        super().__init__(nid)
        hdbg.dassert_in(execution_mode, ("batch", "real_time"))
        self._prediction_col = prediction_col
        self._execution_mode = execution_mode
        self._process_forecasts_config = process_forecasts_config

    def fit(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        return self._place_trades(df_in, fit=True)

    def predict(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        return self._place_trades(df_in, fit=False)

    def _place_trades(
        self, df: pd.DataFrame, fit: bool = True
    ) -> Dict[str, pd.DataFrame]:
        hdbg.dassert_in(self._prediction_col, df.columns)
        # Make sure it's multi-index.
        hdbg.dassert_lte(2, df.columns.nlevels)
        hdbg.dassert_isinstance(df.index, pd.DatetimeIndex)
        # TODO(gp): Maybe pass the entire multi-index df and the name of
        #  pred_col and vol_col.
        prediction_df = df[self._prediction_col]
        # Get the latest `df` index value.
        oplaorde.place_orders(
            prediction_df, self._execution_mode, self._process_forecasts_config
        )
        # Compute stats.
        info = collections.OrderedDict()
        info["df_out_info"] = dtf.get_df_info_as_string(df)
        mode = "fit" if fit else "predict"
        self._set_info(mode, info)
        # Pass the dataframe through.
        return {"df_out": df}
