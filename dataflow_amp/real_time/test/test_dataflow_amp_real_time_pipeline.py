import logging

_LOG = logging.getLogger(__name__)


# class TestRealTimeReturnPipeline1(hut.TestCase):
#    def test1(self) -> None:
#        """
#        Test `RealTimeReturnPipeline` using synthetic data.
#        """
#        # Create the pipeline.
#        dag_builder = dtfart.RealTimeReturnPipeline()
#        config = dag_builder.get_config_template()
#        _LOG.debug("\n# config=\n%s", config)
#
#        start_datetime = pd.Timestamp("2010-01-04 09:30:00")
#        end_datetime = pd.Timestamp("2010-01-04 11:30:00")
#
#        dag_builder.validate_config(config)
#        # Create the DAG runner.
#        execute_rt_loop_kwargs = cdtfttrt.get_test_execute_rt_loop_kwargs()
#        kwargs = {
#            "config": config,
#            "dag_builder": dag_builder,
#            "fit_state": None,
#            #
#            "execute_rt_loop_kwargs": execute_rt_loop_kwargs,
#            #
#            "dst_dir": None,
#        }
#        dag_runner = cdtf.RealTimeDagRunner(**kwargs)
#        # Run.
#        dtf.align_on_even_second()
#        dag_runner.predict()
#        # TODO(gp): Check.
