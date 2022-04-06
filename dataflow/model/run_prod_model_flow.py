"""
Run a backtest for a DAG model.

Import as:

import dataflow.model.run_prod_model_flow as dtfmrpmofl
"""

import abc
import logging
import os
from typing import Any, Optional

import core.config as cconfig
import dataflow.model as dtfmod
import dataflow.model.model_evaluator as dtfmomoeva
import helpers.hdbg as hdbg
import helpers.hgit as hgit
import helpers.hintrospection as hintros
import helpers.hjupyter as hjupyte
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import helpers.hsystem as hsystem
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


# TODO(gp): -> backtest_test_case.py


class Backtest_TestCase(abc.ABC, hunitest.TestCase):
    """
    Run a backtest for a Dag and checking its result.
    """

    @staticmethod
    def get_configs_signature(config_builder: str) -> str:
        configs = cconfig.get_configs_from_builder(config_builder)
        txt = cconfig.configs_to_str(configs)
        return txt

    @staticmethod
    def get_dir_signature(dir_name: str) -> str:
        include_file_content = False
        remove_dir_name = True
        txt = hunitest.get_dir_signature(
            dir_name, include_file_content, remove_dir_name=remove_dir_name
        )
        return txt

    # TODO(gp): -> run_dag_config
    @staticmethod
    def _run_model(
        config_builder: str,
        experiment_builder: str,
        extra_opts: str,
        dst_dir: str,
    ) -> None:
        """
        Invoke run_experiment.py on a config.
        """
        # Execute a command line like:
        # ```
        # > /app/amp/core/dataflow_model/run_experiment.py \
        #       --experiment_builder \
        #           amp.dataflow_model.master_experiment.run_experiment \
        #       --config_builder \
        #           'dataflow....build_model_configs("kibot_v1-top1.5T")' \
        #       --dst_dir .../run_model/oos_experiment.RH1E.kibot_v1-top1.5T \
        #       --clean_dst_dir \
        #       --no_confirm \
        #       --num_threads serial
        # ```
        if os.path.exists(dst_dir):
            # TODO(gp): Maybe assert.
            _LOG.warning("Dir with experiment already exists: skipping...")
            return
        #
        opts = []
        opts.append("--clean_dst_dir --no_confirm")
        opts.append("--num_threads serial")
        opts.append(extra_opts)
        opts = " ".join(opts)
        #
        exec_filename = hgit.get_client_root(super_module=False)
        # TODO(gp): Use find_amp
        exec_filename = os.path.join(
            exec_filename, "amp/dataflow/model/run_experiment.py"
        )
        hdbg.dassert_path_exists(exec_filename)
        #
        cmd = []
        cmd.append(exec_filename)
        # Experiment builder.
        # E.g., "amp.dataflow_model.master_experiment.run_experiment"
        cmd.append(f"--experiment_builder {experiment_builder}")
        # Config builder.
        # E.g.,
        # builder = f'build_model_configs("{backtest_config}", 1)'
        # config_builder = f'dataflow_lemonade.RH1E.RH1E_configs.{builder}'
        cmd.append(f"--config_builder '{config_builder}'")
        #
        cmd.append(f"--dst_dir {dst_dir}")
        if _LOG.getEffectiveLevel() >= logging.DEBUG:
            cmd.append("-v DEBUG")
        cmd.append(opts)
        # Assemble the command line and run.
        cmd = " ".join(cmd)
        _LOG.info("cmd=%s", cmd)
        hsystem.system(cmd)

    @abc.abstractmethod
    def _test(self, *args: Any, **kwargs: Any) -> None:
        """
        Run the entire flow.
        """
        pass


# #############################################################################


class TiledBacktest_TestCase(Backtest_TestCase):
    """
    Run an end-to-end backtest for a model by:
    - checking the configs against frozen representation
    - running model using tiled backtest
    - checking that the output is a valid tiled results
    - running the analysis flow to make sure that it works
    """

    def compute_tile_output_signature(self, file_name: str) -> str:
        parquet_tile_analyzer = dtfmod.ParquetTileAnalyzer()
        parquet_tile_metadata = (
            parquet_tile_analyzer.collate_parquet_tile_metadata(file_name)
        )
        signature = []
        # 	    n_years	n_unique_months	n_files	    size
        # egid
        # 10025	      1	              2	      2	711.7 KB
        df = parquet_tile_analyzer.compute_metadata_stats_by_asset_id(
            parquet_tile_metadata
        )
        df_as_str = hpandas.df_to_str(df.drop("size", axis="columns"))
        signature.append("# compute_metadata_stats_by_asset_id")
        signature.append(df_as_str)
        # 		        n_asset_ids	size
        # year	month
        # 2020	    1	         1	360.3 KB
        #           2	         1	351.4 KB
        df = parquet_tile_analyzer.compute_universe_size_by_time(
            parquet_tile_metadata
        )
        signature.append("# compute_universe_size_by_time")
        df_as_str = hpandas.df_to_str(df.drop("size", axis="columns"))
        signature.append(df_as_str)
        #
        signature = "\n".join(signature)
        return signature

    def _test(
        self,
        config_builder: str,
        experiment_builder: str,
        run_model_extra_opts: str,
    ) -> None:
        """
        Run the entire flow.
        """
        scratch_dir = self.get_scratch_space()
        # 1) Test config builder.
        configs_signature = self.get_configs_signature(config_builder)
        tag = "configs_signature"
        self.check_string(configs_signature, fuzzy_match=True, tag=tag)
        # 2) Run the model.
        run_model_dir = os.path.join(scratch_dir, "run_model")
        if hunitest.get_incremental_tests() and os.path.exists(run_model_dir):
            _LOG.warning(
                "Skipping running _run_model since '%s' already exists",
                run_model_dir,
            )
        else:
            self._run_model(
                config_builder,
                experiment_builder,
                run_model_extra_opts,
                run_model_dir,
            )
        # 3) Freeze the output of the entire dir.
        tag = "output_signature"
        output_signature = self.get_dir_signature(run_model_dir)
        # Remove lines like 'log.20220304-200306.txt'.
        output_signature = hprint.filter_text(r"log\..*\.txt", output_signature)
        self.check_string(output_signature, fuzzy_match=True, tag=tag)
        # 4) Run the analysis.
        tile_output_dir = os.path.join(run_model_dir, "tiled_results")
        tile_output_signature = self.compute_tile_output_signature(
            tile_output_dir
        )
        tag = "tile_output_signature"
        self.check_string(tile_output_signature, fuzzy_match=True, tag=tag)
