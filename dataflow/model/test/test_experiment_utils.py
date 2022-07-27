import logging
from typing import List

import core.config as cconfig
import dataflow.core as dtfcore
import dataflow.model.experiment_config as dtfmoexcon
import dataflow.model.experiment_utils as dtfmoexuti
import dataflow.pipelines.example1.example1_pipeline as dtfpexexpi
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


# This mimics the code in example1_configs.py
# TODO(gp): Consider calling that code directly if it doesn't violate the
#  dependencies.

# TODO(gp): Port to new System style (see E8_config.py).


def _build_base_config() -> cconfig.Config:
    wrapper = cconfig.Config()
    #
    dag_builder = dtfpexexpi.Example1_DagBuilder()
    dag_config = dag_builder.get_config_template()
    wrapper["dag_config"] = dag_config
    wrapper["dag_builder_object"] = dag_builder
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


def get_dag_runner(config: cconfig.Config) -> dtfcore.DAG:
    """
    Build a DAG runner from a config.
    """
    # Build the DAG.
    dag_builder = config["dag_builder_object"]
    dag = dag_builder.get_dag(config["dag_config"])
    # Build the DagRunner.
    dag_runner = dtfcore.FitPredictDagRunner(dag)
    return dag_runner


def build_tile_configs(
    experiment_config: str,
) -> List[cconfig.Config]:
    (
        _,
        _,
        time_interval_str,
    ) = dtfmoexcon.parse_experiment_config(experiment_config)
    #
    config = _build_base_config()
    # TODO(gp): We should build Systems and not return a builder.
    config["dag_runner_builder"] = get_dag_runner
    # Name of the asset_ids to save.
    config["market_data_config", "asset_id_name"] = "asset_id"
    configs = [config]
    # Apply the cross-product by the universe tiles.
    asset_ids = _get_universe_tiny()
    func = lambda cfg: dtfmoexcon.build_configs_with_tiled_universe(
        cfg, asset_ids
    )
    configs = dtfmoexcon.apply_build_configs(func, configs)
    _LOG.info("After applying universe tiles: num_configs=%s", len(configs))
    # Apply the cross-product by the time tiles.
    start_timestamp, end_timestamp = dtfmoexcon.get_period(time_interval_str)
    freq_as_pd_str = "M"
    lookback_as_pd_str = "10D"
    func = lambda cfg: dtfmoexcon.build_configs_varying_tiled_periods(
        cfg, start_timestamp, end_timestamp, freq_as_pd_str, lookback_as_pd_str
    )
    configs: List[cconfig.Config] = dtfmoexcon.apply_build_configs(func, configs)
    _LOG.info("After applying time tiles: num_configs=%s", len(configs))
    return configs


class Test_get_configs_from_command_line_Amp1(hunitest.TestCase):
    """
    Test building (but not running) the configs for backtest.
    """

    def test1(self) -> None:
        # Prepare inputs.
        class Args:
            experiment_list_config = "universe_v2_0-top2.5T.2020-01-01_2020-03-01"
            config_builder = (
                "dataflow.model.test.test_experiment_utils.build_tile_configs"
                + f'("{experiment_list_config}")'
            )
            dst_dir = "./dst_dir"
            experiment_builder = (
                "dataflow.model.master_experiment.run_tiled_backtest"
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
