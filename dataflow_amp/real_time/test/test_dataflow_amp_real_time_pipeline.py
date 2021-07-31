# import logging
#
# import dataflow_amp.real_time.return_pipeline as dtfart
# import helpers.unit_test as hut
#
# _LOG = logging.getLogger(__name__)
#
#
# class TestRealTimeReturnPipeline1(hut.TestCase):
#    def test1(self) -> None:
#        """
#        Test the RealTimeDagRunner using synthetic data.
#        """
#        dag_builder = dtfart.RealTimeReturnPipeline()
#        config = dag_builder.get_config_template()
#        _LOG.debug("\n# config=\n%s", config)
#        dag_builder.validate_config(config)
#        #
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
#        dtf.align_on_even_second()
#        dag_runner.predict()
#        # TODO(gp): Check.
