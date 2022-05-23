"""
Import as:

import dataflow.system.example_pipeline1_system_runner as dtfsepsyru
"""

import logging
from typing import List, Optional, Tuple

import pandas as pd

import core.config as cconfig
import dataflow.core as dtfcore
import dataflow.pipelines.examples.example1_pipeline as dtfpexexpi
import dataflow.system as dtfsys
import dataflow.system.real_time_dag_adapter as dtfsrtdaad
import dataflow.system.real_time_dag_runner as dtfsrtdaru
import dataflow.system.source_nodes as dtfsysonod
import dataflow.system.system_runner as dtfsysyrun
import helpers.hdatetime as hdateti
import market_data as mdata
import oms

_LOG = logging.getLogger(__name__)


# TODO(gp): -> example1_system.py or system_example1.py?


# TODO(gp): I've tried to use a mixin put the class hierarchy was not working as
#  I would have expected in C++. In general I think it's better / clearer to use
#  functions and compositions, rather than inheritance (until I understand better
#  how exactly the inheritance rules are in Python).
def get_Example1_dag_runner(
    config: cconfig.Config,
    market_data: mdata.ReplayedMarketData,
    *,
    # TODO(gp): Move this to the config.
    real_time_loop_time_out_in_secs: Optional[int] = None,
) -> dtfsrtdaru.RealTimeDagRunner:
    """
    Build a DAG runner from a config.
    """
    stage = "read_data"
    asset_id_col = "asset_id"
    # The DAG works on multi-index dataframe containing multiple
    # features for multiple assets.
    multiindex_output = True
    # How much history is needed for the DAG to compute.
    timedelta = pd.Timedelta("20T")
    node = dtfsysonod.RealTimeDataSource(
        stage,
        market_data,
        timedelta,
        asset_id_col,
        multiindex_output,
    )
    # Build the DAG.
    dag_builder = config["meta", "dag_builder"]
    dag = dag_builder.get_dag(config["DAG"])
    # # This is for debugging. It saves the output of each node in a `csv` file.
    # dag.set_debug_mode("df_as_csv", False, "crypto_forever")
    # if False:
    #     dag.force_free_nodes = True
    # Add the data source node.
    dag.insert_at_head(node)
    sleep_interval_in_secs = 5 * 60
    # Set up the event loop.
    get_wall_clock_time = market_data.get_wall_clock_time
    execute_rt_loop_kwargs = {
        "get_wall_clock_time": get_wall_clock_time,
        "sleep_interval_in_secs": sleep_interval_in_secs,
        "time_out_in_secs": real_time_loop_time_out_in_secs,
    }
    dag_runner_kwargs = {
        "config": config,
        # TODO(Danya): Add a more fitting/transparent name.
        "dag_builder": dag,
        "fit_state": None,
        "execute_rt_loop_kwargs": execute_rt_loop_kwargs,
        "dst_dir": None,
    }
    dag_runner = dtfsrtdaru.RealTimeDagRunner(**dag_runner_kwargs)
    return dag_runner


# #############################################################################


class Example1_ForecastSystem(dtfsysyrun.ForecastSystem):
    """
    Create a system with:

    - a ReplayedMarketData
    - an Example1 DAG
    """

    def __init__(self, asset_ids: List[int], event_loop=None):
        self._asset_ids = asset_ids
        self._event_loop = event_loop

    def get_system_config_template(self) -> cconfig.Config:
        """
        See description in parent class.
        """
        _ = self
        config = cconfig.Config()
        # Save the `DagBuilder` and the `DagConfig` in the config object.
        dag_builder = dtfpexexpi.Example1_DagBuilder()
        dag_config = dag_builder.get_config_template()
        config["DAG"] = dag_config
        config["meta", "dag_builder"] = dag_builder
        return config

    def get_market_data(
        self,
        data: pd.DataFrame,
        initial_replayed_delay: int = 5,
    ) -> mdata.ReplayedMarketData:
        market_data, _ = mdata.get_ReplayedTimeMarketData_from_df(
            self._event_loop,
            initial_replayed_delay,
            data,
        )
        return market_data

    def get_dag(
        self,
        system_config: cconfig.Config,
    ) -> Tuple[cconfig.Config, dtfcore.DAG]:
        """
        Given a completely filled `system_config` build and return the DAG.

        We return the Config that generated the DAG only for book-
        keeping purposes (e.g., to write the config on file) in case
        there were some updates to the Config.
        """
        raise NotImplementedError

    def get_dag_runner(
        self,
        config: cconfig.Config,
        market_data: mdata.ReplayedMarketData,
        *,
        real_time_loop_time_out_in_secs: Optional[int] = None,
    ) -> dtfsrtdaru.RealTimeDagRunner:
        dag_runner = get_Example1_dag_runner(
            config,
            market_data,
            real_time_loop_time_out_in_secs=real_time_loop_time_out_in_secs,
        )
        return dag_runner


# #############################################################################


# TODO(gp): This is an _example builder.
def get_dag_runner(
    dag_builder: dtfcore.DagBuilder,
    config: cconfig.Config,
    get_wall_clock_time: hdateti.GetWallClockTime,
    *,
    sleep_interval_in_secs: int = 60 * 5,
    real_time_loop_time_out_in_secs: Optional[int] = None,
) -> dtfsys.RealTimeDagRunner:
    # Set up the event loop.
    execute_rt_loop_kwargs = {
        "get_wall_clock_time": get_wall_clock_time,
        "sleep_interval_in_secs": sleep_interval_in_secs,
        "time_out_in_secs": real_time_loop_time_out_in_secs,
    }
    dag_runner_kwargs = {
        "config": config,
        "dag_builder": dag_builder,
        "fit_state": None,
        "execute_rt_loop_kwargs": execute_rt_loop_kwargs,
        "dst_dir": None,
    }
    dag_runner = dtfsys.RealTimeDagRunner(**dag_runner_kwargs)
    return dag_runner


# TODO(gp): This should be merged with ForecastSystem and we should build the
#  DAG directly instead of using DagAdapter.
class Example1_TimeForecastSystemWithPortfolio(
    dtfsysyrun.TimeForecastSystemWithPortfolio
):
    """

    - ReplayedTimeMarketData
    - Example1 DAG
    - Portfolio
    """

    def __init__(self, asset_ids: List[int], event_loop=None):
        self._asset_ids = asset_ids
        self._event_loop = event_loop

    def get_market_data(
        self,
        data: pd.DataFrame,
        initial_replayed_delay: int = 5,
    ):
        market_data, _ = mdata.get_ReplayedTimeMarketData_from_df(
            self._event_loop,
            initial_replayed_delay,
            data,
        )
        return market_data

    def get_dag(
        self,
        portfolio: oms.Portfolio,
        *,
        prediction_col: str = "feature1",
        volatility_col: str = "vwap.ret_0.vol",
        returns_col: str = "vwap.ret_0",
        spread_col: Optional[str] = None,
        timedelta: pd.Timedelta = pd.Timedelta("7D"),
        asset_id_col: str = "asset_id",
        log_dir: Optional[str] = None,
    ) -> Tuple[cconfig.Config, dtfcore.DagBuilder]:
        base_dag_builder = dtfpexexpi.Example1_DagBuilder()
        dag_builder = dtfsrtdaad.RealTimeDagAdapter(
            base_dag_builder,
            portfolio,
            prediction_col,
            volatility_col,
            returns_col,
            spread_col,
            timedelta,
            asset_id_col,
            log_dir=log_dir,
        )
        _LOG.debug("dag_builder=\n%s", dag_builder)
        config = dag_builder.get_config_template()
        return config, dag_builder

    def get_dag_runner(
        self,
        dag_builder: dtfcore.DagBuilder,
        config: cconfig.Config,
        get_wall_clock_time: hdateti.GetWallClockTime,
        *,
        sleep_interval_in_secs: int = 60 * 5,
        real_time_loop_time_out_in_secs: Optional[int] = None,
    ) -> dtfsys.RealTimeDagRunner:
        dag_runner = get_dag_runner(
            dag_builder,
            config,
            get_wall_clock_time,
            sleep_interval_in_secs=sleep_interval_in_secs,
            real_time_loop_time_out_in_secs=real_time_loop_time_out_in_secs,
        )
        return dag_runner


# #############################################################################


class Example1_TimeForecastSystemWithDataFramePortfolio(
    Example1_TimeForecastSystemWithPortfolio
):

    # TODO(gp): To define all the abstract methods. We should use this function.
    def get_system_config_template(
        self,
    ) -> cconfig.Config:
        pass

    def get_portfolio(
        self,
        market_data: mdata.MarketData,
    ) -> oms.Portfolio:
        portfolio = oms.get_DataFramePortfolio_example1(
            self._event_loop,
            market_data=market_data,
            mark_to_market_col="close",
            pricing_method="twap.5T",
            asset_ids=self._asset_ids,
        )
        portfolio.broker._column_remap = {
            "bid": "bid",
            "ask": "ask",
            "midpoint": "midpoint",
            "price": "close",
        }
        return portfolio

    def get_dag_runner(
        self,
        dag_builder: dtfcore.DagBuilder,
        config: cconfig.Config,
        get_wall_clock_time: hdateti.GetWallClockTime,
        *,
        sleep_interval_in_secs: int = 60 * 5,
        real_time_loop_time_out_in_secs: Optional[int] = None,
    ) -> dtfsys.RealTimeDagRunner:
        dag_runner = get_dag_runner(
            dag_builder,
            config,
            get_wall_clock_time,
            sleep_interval_in_secs=sleep_interval_in_secs,
            real_time_loop_time_out_in_secs=real_time_loop_time_out_in_secs,
        )
        return dag_runner


# #############################################################################


class Example1_TimeForecastSystemWithDatabasePortfolio(
    dtfsysyrun.TimeForecastSystemWithDatabasePortfolioAndDatabaseBrokerAndOrderProcessor,
    Example1_TimeForecastSystemWithPortfolio,
):
    """
    A System with:

    - a `ReplayedMarketData`
    - an `Example1` DAG
    - a `DatabasePortfolio` (which includes a DatabaseBroker)
    - an `OrderProcessor`
    """

    # TODO(gp): To define all the abstract methods. We should use this function.
    def get_system_config_template(
        self,
    ) -> cconfig.Config:
        pass

    def get_dag(
        self,
        portfolio: oms.Portfolio,
        *,
        prediction_col: str = "feature1",
        volatility_col: str = "vwap.ret_0.vol",
        returns_col: str = "vwap.ret_0",
        spread_col: Optional[str] = None,
        timedelta: pd.Timedelta = pd.Timedelta("7D"),
        asset_id_col: str = "asset_id",
        log_dir: Optional[str] = None,
    ) -> Tuple[cconfig.Config, dtfcore.DagBuilder]:
        base_dag_builder = dtfpexexpi.Example1_DagBuilder()
        dag_builder = dtfsrtdaad.RealTimeDagAdapter(
            base_dag_builder,
            portfolio,
            prediction_col,
            volatility_col,
            returns_col,
            spread_col,
            timedelta,
            asset_id_col,
            log_dir=log_dir,
        )
        _LOG.debug("dag_builder=\n%s", dag_builder)
        config = dag_builder.get_config_template()
        return config, dag_builder

    def get_portfolio(
        self,
        market_data: mdata.MarketData,
    ) -> oms.Portfolio:
        table_name = oms.CURRENT_POSITIONS_TABLE_NAME
        portfolio = oms.get_mocked_portfolio_example1(
            self._event_loop,
            self._db_connection,
            table_name,
            market_data=market_data,
            mark_to_market_col="close",
            pricing_method="twap.5T",
            asset_ids=self._asset_ids,
        )
        portfolio.broker._column_remap = {
            "bid": "bid",
            "ask": "ask",
            "midpoint": "midpoint",
            "price": "close",
        }
        return portfolio

    def get_dag_runner(
        self,
        dag_builder: dtfcore.DagBuilder,
        config: cconfig.Config,
        get_wall_clock_time: hdateti.GetWallClockTime,
        *,
        sleep_interval_in_secs: int = 60 * 5,
        real_time_loop_time_out_in_secs: Optional[int] = None,
    ) -> dtfsys.RealTimeDagRunner:
        dag_runner = get_dag_runner(
            dag_builder,
            config,
            get_wall_clock_time,
            sleep_interval_in_secs=sleep_interval_in_secs,
            real_time_loop_time_out_in_secs=real_time_loop_time_out_in_secs,
        )
        return dag_runner
