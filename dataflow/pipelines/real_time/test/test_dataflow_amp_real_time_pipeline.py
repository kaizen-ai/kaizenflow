import asyncio
import logging

import pandas as pd

import core.config as cconfig
import core.real_time_example as cretiexa
import dataflow as dtf
import dataflow.core.builders_example as dtfcobuexa
import dataflow.pipelines.dataflow_example as dtfpidtfexa
import dataflow.pipelines.returns.pipeline as dtfpirepip
import dataflow.system.real_time_dag_adapter as dtfsrtdaad
import dataflow.system.real_time_runner as dtfsretiru
import helpers.hasyncio as hasynci
import helpers.printing as hprint
import helpers.unit_test as hunitest
import market_data.market_data_interface as mdmadain
import market_data.market_data_interface_example as mdmdinex
import oms.oms_db as oomsdb
import oms.order_processor as oordproc
import oms.portfolio as omportfo
import oms.portfolio_example as oporexam
import oms.test.oms_db_helper as otodh

_LOG = logging.getLogger(__name__)


# TODO(gp): use dag_builder = dtfsrtdaad.RealTimeDagAdapter(base_dag_builder,
# portfolio)
class TestRealTimeReturnPipeline1(hunitest.TestCase):
    """
    This test is similar to `TestRealTimeDagRunner1`. It uses:

    - a real DAG `ReturnPipeline`
    - a replayed time data source node using synthetic data
    """

    def test1(self) -> None:
        """
        Test `RealTimeReturnPipeline` using synthetic data.
        """
        with hasynci.solipsism_context() as event_loop:
            # Create the pipeline.
            dag_builder = dtfpirepip.ReturnsPipeline()
            config = dag_builder.get_config_template()
            # Inject the real-time node.
            start_datetime = pd.Timestamp(
                "2000-01-01 09:30:00-05:00", tz="America/New_York"
            )
            end_datetime = pd.Timestamp(
                "2000-01-01 10:30:00-05:00", tz="America/New_York"
            )
            columns = ["close", "vol"]
            asset_ids = [101]
            df = mdmdinex.generate_random_price_data(
                start_datetime, end_datetime, columns, asset_ids
            )
            initial_replayed_delay = 5
            (
                market_data_interface,
                _,
            ) = mdmdinex.get_replayed_time_market_data_interface_example1(
                event_loop,
                initial_replayed_delay,
                df,
            )
            period = "last_5mins"
            source_node_kwargs = {
                "market_data_interface": market_data_interface,
                "period": period,
                "asset_id_col": "asset_id",
                "multiindex_output": False,
            }
            config["load_prices"] = cconfig.get_config_from_nested_dict(
                {
                    "source_node_name": "RealTimeDataSource",
                    "source_node_kwargs": source_node_kwargs,
                }
            )
            # Set up the event loop.
            sleep_interval_in_secs = 60 * 5
            execute_rt_loop_kwargs = (
                cretiexa.get_replayed_time_execute_rt_loop_kwargs(
                    sleep_interval_in_secs, event_loop=event_loop
                )
            )
            dag_runner_kwargs = {
                "config": config,
                "dag_builder": dag_builder,
                "fit_state": None,
                "execute_rt_loop_kwargs": execute_rt_loop_kwargs,
                "dst_dir": None,
            }
            # Run.
            dag_runner = dtfsretiru.RealTimeDagRunner(**dag_runner_kwargs)
            result_bundles = hasynci.run(
                dag_runner.predict(), event_loop=event_loop
            )
            events = dag_runner.events
            # Check.
            # TODO(Paul): Factor this out.
            actual = []
            events_as_str = "\n".join(
                [
                    event.to_str(
                        include_tenths_of_secs=False,
                        include_wall_clock_time=False,
                    )
                    for event in events
                ]
            )
            actual.append("events_as_str=\n%s" % events_as_str)
            #
            result_bundles_as_str = "\n".join(map(str, result_bundles))
            actual.append("result_bundles=\n%s" % result_bundles_as_str)
            #
            actual = "\n".join(map(str, actual))
            self.check_string(actual)


# #############################################################################


# TODO(gp): Use dag_builder = dtfsrtdaad.RealTimeDagAdapter(base_dag_builder,
#   portfolio)
# TOOD(gp): -> TestRealTimeNaivePipelineWithOms1
class TestRealTimePipelineWithOms1(hunitest.TestCase):
    """
    This test uses:

    - a DAG mimicking a true dataflow pipeline (e.g., resampling)
    - a DAG whose forecasts can be passed through a df
    - a replayed time data source node using synthetic data for prices
    """

    def test1(self) -> None:
        """
        Test `RealTimeReturnPipeline` using synthetic data.
        """
        with hasynci.solipsism_context() as event_loop:
            # Create the pipeline.
            # TODO(Paul): use another DAG instead of NaivePipeline().
            # dag_builder = dtfpretipi.RealTimeReturnPipeline()
            dag_builder = dtfpidtfexa._NaivePipeline()
            config = dag_builder.get_config_template()
            # Inject the real-time node.
            start_datetime = pd.Timestamp(
                "2000-01-01 09:30:00-05:00", tz="America/New_York"
            )
            end_datetime = pd.Timestamp(
                "2000-01-01 10:30:00-05:00", tz="America/New_York"
            )
            columns = ["price", "vol"]
            asset_ids = [1000]
            df = mdmdinex.generate_random_price_data(
                start_datetime, end_datetime, columns, asset_ids
            )
            initial_replayed_delay = 0
            (
                market_data_interface,
                get_wall_clock_time,
            ) = mdmdinex.get_replayed_time_market_data_interface_example1(
                event_loop,
                initial_replayed_delay,
                df,
            )
            period = "last_5mins"
            source_node_kwargs = {
                "market_data_interface": market_data_interface,
                "period": period,
                "asset_id_col": "asset_id",
                "multiindex_output": True,
            }
            # TODO(gp): We are making a mess by getting a DAG and then overriding
            #  the source node.
            config["get_data"] = cconfig.get_config_from_nested_dict(
                {
                    "source_node_name": "RealTimeDataSource",
                    "source_node_kwargs": source_node_kwargs,
                }
            )
            # Build Portfolio.
            initial_timestamp = pd.Timestamp(
                "2000-01-01 09:30:00-05:00", tz="America/New_York"
            )
            portfolio = oporexam.get_simulated_portfolio_example1(
                event_loop,
                initial_timestamp,
                market_data_interface=market_data_interface,
            )
            # Populate place trades.
            order_type = "price@twap"
            config["process_forecasts"]["process_forecasts_config"] = {
                "portfolio": portfolio,
                "order_type": order_type,
                "order_duration": 1,
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
                #
            }
            # Set up the event loop.
            sleep_interval_in_secs = 60 * 5
            execute_rt_loop_kwargs = (
                cretiexa.get_replayed_time_execute_rt_loop_kwargs(
                    sleep_interval_in_secs, event_loop=event_loop
                )
            )
            dag_runner_kwargs = {
                "config": config,
                "dag_builder": dag_builder,
                "fit_state": None,
                "execute_rt_loop_kwargs": execute_rt_loop_kwargs,
                "dst_dir": None,
            }
            # Run.
            dag_runner = dtfsretiru.RealTimeDagRunner(**dag_runner_kwargs)
            result_bundles = hasynci.run(
                dag_runner.predict(), event_loop=event_loop
            )
            events = dag_runner.events
            # Check.
            # TODO(Paul): Factor this out and from these tests. We want to have
            # several tests and share the code.
            actual = []
            events_as_str = "\n".join(
                [
                    event.to_str(
                        include_tenths_of_secs=False,
                        include_wall_clock_time=False,
                    )
                    for event in events
                ]
            )
            actual.append("events_as_str=\n%s" % events_as_str)
            #
            result_bundles_as_str = "\n".join(map(str, result_bundles))
            actual.append("result_bundles=\n%s" % result_bundles_as_str)
            #
            actual = "\n".join(map(str, actual))
            self.check_string(actual)


# #############################################################################


# TODO(gp): Add a DagAdapter also for running in real-time.
# class TestSimulatedMvnReturns1(hunitest.TestCase):
#     """
#     Run `MvnReturns` pipeline in simulation.
#     """
#
#     def test1(self) -> None:
#         # Build the pipeline.
#         # TODO(gp): We need a DagAdapter or inject the data source node.
#         dag_builder = dtfpretipi.MvnReturnsRealTimeBuilder()
#         config = dag_builder.get_config_template()
#         dag_runner = dtf.FitPredictDagRunner(config, dag_builder)
#         # Run.
#         result_bundle = dag_runner.fit()
#         # Check.
#         # TODO(gp): This is a common idiom. Factor out.
#         df_out = result_bundle.result_df
#         str_output = (
#             f"{hprint.frame('config')}\n{config}\n"
#             f"{hprint.frame('df_out')}\n{hunitest.convert_df_to_string(df_out, index=True)}\n"
#         )
#         self.check_string(str_output)


# #############################################################################


class TestRealTimeMvnReturnsWithOms1(otodh.TestOmsDbHelper):
    """
    Run `MvnReturns` pipeline in real-time with mocked OMS objects.
    """

    @staticmethod
    def get_market_data_df() -> pd.DataFrame:
        """
        Create a dataframe with the data for a `MarketDataInterface`.
        """
        start_datetime = pd.Timestamp(
            "2000-01-03 09:30:00-05:00", tz="America/New_York"
        )
        end_datetime = pd.Timestamp(
            "2000-01-03 10:30:00-05:00", tz="America/New_York"
        )
        # Run the node to get the df out.
        node_config = {
            "frequency": "T",
            "start_date": start_datetime,
            "end_date": end_datetime,
            "dim": 1,
            "target_volatility": 0.25,
            "seed": 247,
        }
        node = dtf.MultivariateNormalGenerator("fake", **node_config)
        df = node.fit()["df_out"]
        df = df.swaplevel(i=0, j=1, axis=1)
        df = df["MN0"]
        _LOG.debug("df=%s", hprint.dataframe_to_str(df))
        # Transform a DataFlow df into a MarketData df.
        df["end_datetime"] = df.index
        df["start_datetime"] = df.index - pd.DateOffset(minutes=1)
        df["timestamp_db"] = df["end_datetime"]
        df["asset_id"] = 101
        _LOG.debug("df=%s", hprint.dataframe_to_str(df))
        return df

    def get_market_data_interface(
        self, event_loop: asyncio.AbstractEventLoop
    ) -> mdmadain.AbstractMarketDataInterface:
        df = self.get_market_data_df()
        initial_replayed_delay = 0
        (
            market_data_interface,
            get_wall_clock_time,
        ) = mdmdinex.get_replayed_time_market_data_interface_example1(
            event_loop,
            initial_replayed_delay,
            df,
        )
        return market_data_interface

    def get_portfolio(
        self,
        event_loop: asyncio.AbstractEventLoop,
        market_data_interface: mdmadain.AbstractMarketDataInterface,
    ) -> omportfo.MockedPortfolio:
        db_connection = self.connection
        table_name = oomsdb.CURRENT_POSITIONS_TABLE_NAME
        initial_timestamp = pd.Timestamp(
            "2000-01-03 09:30:00-05:00", tz="America/New_York"
        )
        portfolio = oporexam.get_mocked_portfolio_example1(
            event_loop,
            db_connection,
            table_name,
            initial_timestamp,
            market_data_interface=market_data_interface,
            mark_to_market_col="close",
        )
        # TODO(Paul): Set this more systematically.
        portfolio.broker._column_remap = {
            "bid": "bid",
            "ask": "ask",
            "midpoint": "midpoint",
            "price": "close",
        }
        return portfolio

    def get_order_processor(
        self, portfolio: omportfo.MockedPortfolio
    ) -> oordproc.OrderProcessor:
        db_connection = self.connection
        get_wall_clock_time = portfolio._get_wall_clock_time
        order_processor_poll_kwargs = hasynci.get_poll_kwargs(get_wall_clock_time)
        # order_processor_poll_kwargs["sleep_in_secs"] = 1
        # Since orders should come every 5 mins we give it a buffer of 5 extra
        # mins.
        order_processor_poll_kwargs["timeout_in_secs"] = 60 * 20
        delay_to_accept_in_secs = 3
        delay_to_fill_in_secs = 10
        broker = portfolio.broker
        order_processor = oordproc.OrderProcessor(
            db_connection,
            delay_to_accept_in_secs,
            delay_to_fill_in_secs,
            broker,
            poll_kwargs=order_processor_poll_kwargs,
        )
        return order_processor

    def test1(self) -> None:
        # Clean the DB tables.
        oomsdb.create_oms_tables(self.connection, incremental=False)
        #
        with hasynci.solipsism_context() as event_loop:
            market_data_interface = self.get_market_data_interface(event_loop)
            portfolio = self.get_portfolio(event_loop, market_data_interface)
            # Create the real-time DAG.
            base_dag_builder = dtfcobuexa.MvnReturnsBuilder()
            dag_builder = dtfsrtdaad.RealTimeDagAdapter(
                base_dag_builder, portfolio
            )
            _LOG.debug("dag_builder=\n%s", dag_builder)
            config = dag_builder.get_config_template()
            # Set up the event loop.
            sleep_interval_in_secs = 60 * 5
            execute_rt_loop_kwargs = (
                cretiexa.get_replayed_time_execute_rt_loop_kwargs(
                    sleep_interval_in_secs, event_loop=event_loop
                )
            )
            dag_runner_kwargs = {
                "config": config,
                "dag_builder": dag_builder,
                "fit_state": None,
                "execute_rt_loop_kwargs": execute_rt_loop_kwargs,
                "dst_dir": None,
            }
            # Build OrderProcessor.
            order_processor = self.get_order_processor(portfolio)
            termination_condition = pd.Timestamp("2000-01-03 09:45:00-05:00")
            order_processor_coroutine = order_processor.run_loop(
                termination_condition
            )
            # Run.
            dag_runner = dtfsretiru.RealTimeDagRunner(**dag_runner_kwargs)
            coroutines = [dag_runner.predict(), order_processor_coroutine]
            result_bundles = hasynci.run(
                asyncio.gather(*coroutines), event_loop=event_loop
            )
            events = dag_runner.events
            # Check.
            # TODO(Paul): Factor this out and from these tests. We want to have
            # several tests and share the code.
            actual = []
            events_as_str = "\n".join(
                [
                    event.to_str(
                        include_tenths_of_secs=False,
                        include_wall_clock_time=False,
                    )
                    for event in events
                ]
            )
            actual.append("events_as_str=\n%s" % events_as_str)
            #
            result_bundles_as_str = "\n".join(map(str, result_bundles))
            actual.append("result_bundles=\n%s" % result_bundles_as_str)
            #
            actual = "\n".join(map(str, actual))
            self.check_string(actual)
