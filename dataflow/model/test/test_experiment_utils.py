import logging
from typing import List

import core.config as cconfig
import dataflow.core as dtfcore
import dataflow.model.experiment_config as dtfmoexcon
import dataflow.model.experiment_utils as dtfmoexuti
import dataflow.pipelines.examples.pipeline1 as dtfpiexpip
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)

# #############################################################################
# Test_get_configs_from_command_line1
# #############################################################################


def _build_base_config() -> cconfig.Config:
    wrapper = cconfig.Config()
    #
    dag_builder = dtfpiexpip.ExamplePipeline1_DagBuilder()
    config = dag_builder.get_config_template()
    wrapper["DAG"] = config
    wrapper["meta", "dag_builder"] = dag_builder
    # wrapper["tags"] = []
    return wrapper


def _get_universe_tiny() -> List[int]:
    """
    Return a toy universe of liquid equities.
    """
    asset_ids = [
        13684,
        10971,
    ]
    return asset_ids


# TODO(gp): Pass universe_str.
def build_configs_with_tiled_universe(
    config: cconfig.Config, universe_str: str
) -> List[cconfig.Config]:
    """
    Create a list of `Config`s tiled by universe.
    """
    asset_ids = _get_universe_tiny()
    universe_tiles = (asset_ids,)
    egid_key = ("meta", "asset_ids")
    configs = dtfmoexcon.build_configs_varying_universe_tiles(
        config, egid_key, universe_tiles
    )
    return configs


# /////////////////////////////////////////////////////////////////////////////////


def get_dag_runner(config: cconfig.Config) -> dtfcore.DAG:
    """
    Build a DAG runner from a config.
    """
    # Build the DAG.
    dag_builder = config["meta", "dag_builder"]
    dag = dag_builder.get_dag(config["DAG"])
    #
    if False:
        # save_node_interface = "stats"
        save_node_interface = ""
        profile_execution = True
        # profile_execution = False
        dst_dir = "./tmp.dag_profile"
        dag.set_debug_mode(save_node_interface, profile_execution, dst_dir)
    # Build the DagRunner.
    dag_runner = dtfcore.FitPredictDagRunner(config, dag)
    return dag_runner


# /////////////////////////////////////////////////////////////////////////////////


def build_rc1_configs(
    experiment_config: str,
) -> List[cconfig.Config]:
    (
        universe_str,
        trading_period_str,
        time_interval_str,
    ) = dtfmoexcon.parse_experiment_config(experiment_config)
    #
    config = _build_base_config()
    #
    config["meta", "dag_runner"] = get_dag_runner
    # Name of the asset_ids to save.
    config["meta", "asset_id_name"] = "asset_id"
    configs = [config]
    # Apply the cross-product by the universe tiles.
    func = lambda cfg: build_configs_with_tiled_universe(cfg, universe_str)
    configs = dtfmoexcon.apply_build_configs(func, configs)
    _LOG.info("After applying universe tiles: num_configs=%s", len(configs))
    # Apply the cross-product by the time tiles.
    start_timestamp, end_timestamp = dtfmoexcon.get_period(time_interval_str)
    freq_as_pd_str = "M"
    lookback_as_pd_str = "10D"
    func = lambda cfg: dtfmoexcon.build_configs_varying_tiled_periods(
        cfg, start_timestamp, end_timestamp, freq_as_pd_str, lookback_as_pd_str
    )
    configs = dtfmoexcon.apply_build_configs(func, configs)
    _LOG.info("After applying time tiles: num_configs=%s", len(configs))
    return configs


class Test_get_configs_from_command_line1(hunitest.TestCase):
    """
    Run an experiment list of two experiments that both succeed.

    These tests are equivalent to `TestRunNotebook1` but using the
    `run_experiment.py` flow instead of `run_notebook.py`.
    """

    def test1(self) -> None:
        # Prepare inputs.
        class Args:
            experiment_list_config = "universe_v2_0-top2.5T.JanFeb2020"
            config_builder = (
                "dataflow.model.test.test_experiment_utils.build_rc1_configs"
                + f'("{experiment_list_config}")'
            )
            dst_dir = "./dst_dir"
            experiment_builder = (
                "dataflow.model.master_experiment.run_tiled_experiment"
            )
            index = 0
            start_from_index = 0
            no_incremental = True

        args = Args()
        # Run.
        configs = dtfmoexuti.get_configs_from_command_line(args)
        # Check.
        txt = cconfig.configs_to_str(configs)
        self.check_string(txt, purify_text=True)