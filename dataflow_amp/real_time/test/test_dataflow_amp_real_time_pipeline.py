import logging

import pandas as pd

import core.config as cconfig
import core.dataflow.runners as cdtfrunn
import core.dataflow.test.test_price_interface as dartttdi
import core.dataflow.test.test_real_time as cdtfttrt
import dataflow_amp.real_time.pipeline as dtfaretipi
import helpers.hasyncio as hasynci
import helpers.unit_test as hunitest

_LOG = logging.getLogger(__name__)


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
            dag_builder = dtfaretipi.RealTimeReturnPipeline()
            config = dag_builder.get_config_template()
            # Inject the real-time node.
            start_datetime = pd.Timestamp("2000-01-01 09:30:00-05:00")
            end_datetime = pd.Timestamp("2000-01-01 10:30:00-05:00")
            columns = ["close", "vol"]
            asset_ids = [1000]
            df = dartttdi.generate_synthetic_db_data(
                start_datetime, end_datetime, columns, asset_ids
            )
            initial_replayed_delay = 5
            rtpi = dartttdi.get_replayed_time_price_interface_example1(
                event_loop,
                start_datetime,
                end_datetime,
                initial_replayed_delay,
                df=df,
            )
            period = "last_5mins"
            source_node_kwargs = {
                "price_interface": rtpi,
                "period": period,
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
                cdtfttrt.get_replayed_time_execute_rt_loop_kwargs(
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
            dag_runner = cdtfrunn.RealTimeDagRunner(**dag_runner_kwargs)
            result_bundles = hasynci.run(
                dag_runner.predict(), event_loop=event_loop
            )
            events = dag_runner.events
            # Check.
            # TODO(gp): Factor this out.
            # TODO(gp):
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
