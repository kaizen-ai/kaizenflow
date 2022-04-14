import logging

import pytest

import dataflow.model.run_prod_model_flow as dtfmrpmofl

_LOG = logging.getLogger(__name__)


class Test_Example1_TiledBacktest(dtfmrpmofl.TiledBacktest_TestCase):
    """
    Run end-to-end experiment for an Example1 pipeline:

    - run model
    - run the analysis flow to make sure that it works
    """

    @pytest.mark.superslow
    def test1(self) -> None:
        """
        Run on a single name for a few months.

        The output is a single tile with both asset_ids.
        """
        # The user thinks in terms of full_symbols.
        #experiment_config = "ccxt_v1-top2.5T.JanFeb2010"
        experiment_config = "example_v1-top2.5T.JanFeb2010"
        #asset_ids = [3303714233, 1467591036]
        # Simulate 2 hours.
        #start_ts = "2000-01-01T09:31:00-05:00"
        #end_ts = "2000-01-01T10:10:00-05:00"
        config_builder = (
            "dataflow.pipelines.examples.example1_configs."
            #+ f'build_tile_configs({asset_ids}, "{start_ts}", "{end_ts}")'
            + f'build_tile_configs({experiment_config})'
        )
        experiment_builder = (
            # "amp.dataflow.model.master_experiment.run_tiled_experiment"
            "dataflow.model.master_experiment.run_tiled_experiment"
        )
        # We abort on error since we don't expect failures.
        run_model_extra_opts = ""
        #
        self._test(config_builder, experiment_builder, run_model_extra_opts)
