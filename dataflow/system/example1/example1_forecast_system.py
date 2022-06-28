"""
Import as:

import dataflow.system.example1.example1_forecast_system as dtfseefosy
"""

import logging

import core.config as cconfig
import dataflow.core as dtfcore
import dataflow.model.experiment_config as dtfmoexcon
import dataflow.pipelines.example1.example1_pipeline as dtfpexexpi
import dataflow.system.example1.example1_builders as dtfsexexbu
import dataflow.system.real_time_dag_runner as dtfsrtdaru
import dataflow.system.system as dtfsyssyst
import helpers.hdbg as hdbg
import market_data as mdata
import oms

_LOG = logging.getLogger(__name__)


# #############################################################################
# Example1_ForecastSystem
# #############################################################################


class Example1_ForecastSystem(dtfsyssyst.ForecastSystem):
    """
    Create a System with:

    - a ReplayedMarketData
    - an non-timed Example1 DAG

    This is used to run an historical simulation of an Example1 system.
    """

    def get_dag_runner(self) -> dtfcore.DagRunner:
        dag_runner = dtfcore.FitPredictDagRunner(self.config, self.dag)
        return dag_runner

    def _get_system_config_template(self) -> cconfig.Config:
        _ = self
        dag_builder = dtfpexexpi.Example1_DagBuilder()
        system_config = dtfsexexbu.get_system_config_template_example1(
            dag_builder
        )
        return system_config

    def _get_market_data(self) -> mdata.ReplayedMarketData:
        market_data = dtfsexexbu.get_Example1_market_data_example2(self)
        return market_data

    def _get_dag(self) -> dtfcore.DAG:
        dag = dtfsexexbu.get_Example1_dag_example1(self)
        return dag


def get_Example1_ForecastSystem_example1(
    backtest_config: str,
) -> dtfsyssyst.ForecastSystem:
    # Parse the backtest experiment.
    (
        universe_str,
        trading_period_str,
        time_interval_str,
    ) = dtfmoexcon.parse_experiment_config(backtest_config)
    #
    system = Example1_ForecastSystem()
    # Set the trading period.
    hdbg.dassert_in(trading_period_str, ("1T", "5T", "15T"))
    # TODO(gp): Use a setter.
    system.config[
        "DAG", "resample", "transformer_kwargs", "rule"
    ] = trading_period_str
    # TODO(gp): We pass dag_runner through the config since somebody
    #  outside needs this function to run.
    system.config["dag_runner"] = system.get_dag_runner
    # Name of the asset_ids to save.
    # TODO(gp): Find a better place in the config, maybe "save_results"?
    system.config["meta", "asset_id_col_name"] = "egid"
    # TODO(gp):
    system.config["backtest", "universe_str"] = universe_str
    system.config["backtest", "trading_period_str"] = trading_period_str
    system.config["backtest", "time_interval_str"] = time_interval_str
    return system


# #############################################################################
# Example1_Time_ForecastSystem
# #############################################################################


class Example1_Time_ForecastSystem(dtfsyssyst.Time_ForecastSystem):
    """
    Create a System with:

    - a ReplayedMarketData
    - an Example1 DAG
    - a RealTimeDagRunner
    """

    def get_dag_runner(self) -> dtfsrtdaru.RealTimeDagRunner:
        dag_runner = dtfsexexbu.get_Example1_dag_runner_example1(self)
        return dag_runner

    def _get_system_config_template(self) -> cconfig.Config:
        _ = self
        dag_builder = dtfpexexpi.Example1_DagBuilder()
        system_config = dtfsexexbu.get_system_config_template_example1(
            dag_builder
        )
        return system_config

    def _get_market_data(self) -> mdata.ReplayedMarketData:
        market_data = dtfsexexbu.get_Example1_market_data_example1(self)
        return market_data

    def _get_dag(self) -> dtfcore.DAG:
        dag = dtfsexexbu.get_Example1_dag_example2(self)
        return dag


# #############################################################################
# Example1_Time_ForecastSystem_with_DataFramePortfolio
# #############################################################################


class Example1_Time_ForecastSystem_with_DataFramePortfolio(
    dtfsyssyst.Time_ForecastSystem_with_DataFramePortfolio
):
    """
    Build a system with:

    - a ReplayedTimeMarketData
    - a time Example1 DAG
    - a DataFramePortfolio
    """

    def get_dag_runner(self) -> dtfsrtdaru.RealTimeDagRunner:
        dag_runner = dtfsexexbu.get_Example1_dag_runner_example1(self)
        return dag_runner

    def _get_system_config_template(self) -> cconfig.Config:
        _ = self
        dag_builder = dtfpexexpi.Example1_DagBuilder()
        system_config = dtfsexexbu.get_system_config_template_example1(
            dag_builder
        )
        return system_config

    def _get_market_data(self) -> mdata.ReplayedMarketData:
        market_data = dtfsexexbu.get_Example1_market_data_example1(self)
        return market_data

    def _get_dag(self) -> dtfcore.DAG:
        dag = dtfsexexbu.get_Example1_dag_example3(self)
        return dag

    def _get_portfolio(self) -> oms.Portfolio:
        event_loop = self.config["event_loop"]
        market_data = self.market_data
        asset_ids = self.config["market_data", "asset_ids"]
        portfolio = oms.get_DataFramePortfolio_example1(
            event_loop,
            market_data=market_data,
            # TODO(gp): These should go in the config.
            mark_to_market_col="close",
            pricing_method="twap.5T",
            asset_ids=asset_ids,
        )
        # TODO(gp): These should go in the config?
        portfolio.broker._column_remap = {
            "bid": "bid",
            "ask": "ask",
            "midpoint": "midpoint",
            "price": "close",
        }
        return portfolio


# #############################################################################
# Example1_Time_ForecastSystem_with_DatabasePortfolio
# #############################################################################


class Example1_Time_ForecastSystem_with_DatabasePortfolio(
    dtfsyssyst.Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor
):
    """
    A System with:

    - a `ReplayedMarketData`
    - an `Example1` DAG
    - a `DatabasePortfolio` (which includes a `DatabaseBroker`)
    - an `OrderProcessor`
    """

    def get_dag_runner(self) -> dtfsrtdaru.RealTimeDagRunner:
        dag_runner = dtfsexexbu.get_Example1_dag_runner_example1(self)
        return dag_runner

    def _get_system_config_template(self) -> cconfig.Config:
        _ = self
        dag_builder = dtfpexexpi.Example1_DagBuilder()
        system_config = dtfsexexbu.get_system_config_template_example1(
            dag_builder
        )
        return system_config

    def _get_market_data(
        self,
    ):
        market_data = dtfsexexbu.get_Example1_market_data_example1(self)
        return market_data

    def _get_dag(self) -> dtfcore.DAG:
        dag = dtfsexexbu.get_Example1_dag_example3(self)
        return dag

    def _get_portfolio(
        self,
    ) -> oms.Portfolio:
        event_loop = self.config["event_loop"]
        market_data = self.market_data
        table_name = oms.CURRENT_POSITIONS_TABLE_NAME
        asset_ids = self.config["market_data", "asset_ids"]
        portfolio = oms.get_DatabasePortfolio_example1(
            event_loop,
            self._db_connection,
            table_name,
            market_data=market_data,
            mark_to_market_col="close",
            pricing_method="twap.5T",
            asset_ids=asset_ids,
        )
        portfolio.broker._column_remap = {
            "bid": "bid",
            "ask": "ask",
            "midpoint": "midpoint",
            "price": "close",
        }
        return portfolio


# #############################################################################
# Example1_Time_ForecastSystem_with_Portfolio
# #############################################################################

# # TODO(gp): We should finish porting and re-activate this test.
# class Example1_Time_ForecastSystem_with_Portfolio(
#     dtfsyssyst.Time_ForecastSystem_with_Portfolio
# ):
#     """
#
#     - ReplayedTimeMarketData
#     - Example1 DAG
#     - Portfolio
#     """
#
#     def _get_system_config_template(self) -> cconfig.Config:
#         _ = self
#         dag_builder = dtfpexexpi.Example1_DagBuilder()
#         system_config = get_system_config_template_example1(dag_builder)
#         return system_config
#
#     def _get_market_data(
#         self,
#     ):
#         market_data = get_Example1_market_data_example1(self.config)
#         return market_data
#
#     def _get_dag(
#         self,
#     ) -> dtfcore.DAG:
#         portfolio = self.portfolio
#         market_data = self.market_data
#         # TODO(gp): It should be factor out.
#         prediction_col = "feature1"
#         volatility_col = "vwap.ret_0.vol"
#         price_col = "vwap.ret_0"
#         spread_col = None
#         #
#         market_data_history_lookback = pd.Timedelta("7D")
#         asset_id_col = "asset_id"
#         dag_builder = dtfpexexpi.Example1_DagBuilder()
#         _LOG.debug("dag_builder=\n%s", dag_builder)
#         config = dag_builder.get_config_template()
#         _LOG.debug("config=\n%s", config)
#         dag = dag_builder.get_dag(config)
#         process_forecasts_dict = dtfsysinod.get_process_forecasts_dict_example1(
#             portfolio,
#             prediction_col,
#             volatility_col,
#             price_col,
#             spread_col,
#         )
#         # Adapt DAG to real-time.
#         dag = dtfsrtdaad.adapt_dag_to_real_time(
#             dag,
#             market_data,
#             market_data_history_lookback,
#             asset_id_col,
#             process_forecasts_dict
#         )
#         return dag
#
#     def get_dag_runner(
#         self,
#         dag_builder: dtfcore.DagBuilder,
#         config: cconfig.Config,
#         get_wall_clock_time: hdateti.GetWallClockTime,
#         *,
#         sleep_interval_in_secs: int = 60 * 5,
#         real_time_loop_time_out_in_secs: Optional[int] = None,
#     ) -> dtfsys.RealTimeDagRunner:
#         dag_runner = get_dag_runner(
#             dag_builder,
#             config,
#             get_wall_clock_time,
#             sleep_interval_in_secs=sleep_interval_in_secs,
#             real_time_loop_time_out_in_secs=real_time_loop_time_out_in_secs,
#         )
#         return dag_runner
