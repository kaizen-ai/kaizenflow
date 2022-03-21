"""
Import as:

import dataflow.system.real_time_dag_adapter as dtfsrtdaad
"""
import os
from typing import Optional

import pandas as pd

import core.config as cconfig
import dataflow.core as dtfcore
import dataflow.system.sink_nodes as dtfsysinod
import dataflow.system.source_nodes as dtfsysonod
import oms.portfolio as omportfo


# TODO(gp): Replace DagAdapter and build the Dag in a single place.
#  We want to create the config below in one place with a _example().
class RealTimeDagAdapter(dtfcore.DagAdapter):
    """
    Adapt a DAG builder to RT execution by injecting real-time nodes.
    """

    # TODO(gp): Expose more parameters as needed.
    def __init__(
        self,
        dag_builder: dtfcore.DagBuilder,
        portfolio: omportfo.AbstractPortfolio,
        prediction_col: str,
        volatility_col: str,
        returns_col: str,
        spread_col: Optional[str],
        timedelta: pd.Timedelta,
        asset_id_col: str,
        *,
        target_gmv: float = 1e5,
        dollar_neutrality: str = "no_constraint",
        log_dir: Optional[str] = None,
    ):
        market_data = portfolio.market_data
        #
        overriding_config = cconfig.Config()
        # Configure a `DataSourceNode`.
        source_node_kwargs = {
            "market_data": market_data,
            "timedelta": timedelta,
            "asset_id_col": asset_id_col,
            "multiindex_output": True,
        }
        overriding_config["load_prices"] = {
            "source_node_name": "RealTimeDataSource",
            "source_node_kwargs": source_node_kwargs,
        }
        # Configure a ProcessForecast node.
        order_type = "price@twap"
        if log_dir is not None:
            evaluate_forecasts_config = cconfig.get_config_from_nested_dict(
                {
                    "log_dir": os.path.join(log_dir, "evaluate_forecasts"),
                    "target_gmv": target_gmv,
                    "dollar_neutrality": dollar_neutrality,
                    "returns_col": returns_col,
                }
            )
        else:
            evaluate_forecasts_config = None
        overriding_config["process_forecasts"] = {
            "prediction_col": prediction_col,
            "volatility_col": volatility_col,
            "spread_col": spread_col,
            "portfolio": portfolio,
            "process_forecasts_config": {},
            "evaluate_forecasts_config": evaluate_forecasts_config,
        }
        # We could also write the `process_forecasts_config` key directly but we
        # want to show a `Config` created with multiple pieces.
        process_forecasts_config_dict = {
            "order_config": {
                "order_type": order_type,
                "order_duration": 5,
            },
            "optimizer_config": {
                "backend": "compute_target_positions_in_cash",
                "target_gmv": target_gmv,
                "dollar_neutrality": dollar_neutrality,
            },
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
        process_forecasts_config = cconfig.get_config_from_nested_dict(
            process_forecasts_config_dict
        )
        overriding_config["process_forecasts"][
            "process_forecasts_config"
        ] = process_forecasts_config
        # Insert a node.
        nodes_to_insert = []
        stage = "load_prices"
        node_ctor = dtfsysonod.data_source_node_factory
        nodes_to_insert.append((stage, node_ctor))
        # Append a `ProcessForecastNode` node.
        nodes_to_append = []
        stage = "process_forecasts"
        node_ctor = dtfsysinod.ProcessForecasts
        nodes_to_append.append((stage, node_ctor))
        #
        super().__init__(
            dag_builder, overriding_config, nodes_to_insert, nodes_to_append
        )
