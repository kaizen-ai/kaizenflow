"""
Import as:

import dataflow.system.dataflow_sink_nodes as dtfsdtfsino
"""

# TODO(gp): -> sink_nodes.py

import collections
import logging
from typing import Any, Dict

import pandas as pd

import dataflow.core.node as dtfcornode
import dataflow.core.nodes.base as dtfconobas
import dataflow.core.utils as dtfcorutil
import helpers.dbg as hdbg
import helpers.printing as hprint
import oms.process_forecasts as oprofore

_LOG = logging.getLogger(__name__)


class ProcessForecasts(dtfconobas.FitPredictNode):
    """
    Place trades from a model.
    """

    def __init__(
        self,
        nid: dtfcornode.NodeId,
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
        return self._compute_forecasts(df_in, fit=True)

    def predict(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        return self._compute_forecasts(df_in, fit=False)

    async def process_forecasts(self) -> None:
        # Get the latest `df` index value.
        await oprofore.process_forecasts(
            self._prediction_df,
            self._execution_mode,
            self._process_forecasts_config,
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
        _LOG.debug("prediction_df=\n%s", hprint.dataframe_to_str(prediction_df))
        # Compute stats.
        info = collections.OrderedDict()
        info["df_out_info"] = dtfcorutil.get_df_info_as_string(df)
        mode = "fit" if fit else "predict"
        self._set_info(mode, info)
        # Pass the dataframe through.
        return {"df_out": df}