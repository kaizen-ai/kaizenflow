"""
Import as:

import core.dataflow_model.run_prod_model_flow as cdtfmrpmofl
"""

import logging
import os
from typing import Optional

import core.config as cconfig
import core.dataflow_model.model_evaluator as cdtfmomoev
import helpers.dbg as hdbg
import helpers.git as hgit
import helpers.jupyter as hjupyte
import helpers.printing as hprint
import helpers.system_interaction as hsysinte
import helpers.unit_test as hunitest

_LOG = logging.getLogger(__name__)


def run_prod_model_flow(
    config_builder: str,
    experiment_builder: str,
    run_model_opts: str,
    run_model_dir: str,
    model_eval_config: Optional[cconfig.Config],
    strategy_eval_config: Optional[cconfig.Config],
    run_notebook_dir: str,
) -> str:
    """
    Run end-to-end flow for a model:

    - run model
    - run the analysis flow to make sure that it works

    :param config_builder: config builder to run the model
    :param experiment_builder: experiment builder to run the model
    :param run_model_dir: dir to run the model
    :param model_eval_config: config for the model evaluator notebook. `None` to skip
    :param strategy_eval_config: config for the strategy evaluator notebook. `None` to skip
    :param run_notebook_dir: dir to run the notebook
    :return: string with the signature of the experiment
    """
    actual_outcome = []
    # 1) Run the model.
    _LOG.debug("\n%s", hprint.frame("Run model", char1="<"))
    _run_model(config_builder, experiment_builder, run_model_opts, run_model_dir)
    # 2) Run the `ModelEvaluator` notebook.
    if model_eval_config is not None:
        _LOG.debug("\n%s", hprint.frame("Run model analyzer notebook", char1="<"))
        amp_dir = hgit.get_amp_abs_path()
        # TODO(gp): Rename -> Master_model_evaluator
        file_name = os.path.join(
            amp_dir, "core/dataflow_model/notebooks/Master_model_analyzer.ipynb"
        )
        #
        run_notebook_dir_tmp = os.path.join(
            run_notebook_dir, "run_model_analyzer"
        )
        #
        python_code = model_eval_config.to_python(check=True)
        env_var = "AM_CONFIG_CODE"
        pre_cmd = f'export {env_var}="{python_code}"'
        #
        hjupyte.run_notebook(file_name, run_notebook_dir_tmp, pre_cmd=pre_cmd)
    # 3) Freeze statistics from the `ModelEvaluator` flow.
    # TODO(gp): We might want to store the aggregate PnL since it's more stable
    #  and more meaningful.
    evaluator = cdtfmomoev.ModelEvaluator.from_eval_config(model_eval_config)
    pnl_stats = evaluator.calculate_stats(
        mode=model_eval_config["mode"],
        target_volatility=model_eval_config["target_volatility"],
    )
    actual_outcome.append(hprint.frame("ModelEvaluator stats"))
    actual_outcome.append(hunitest.convert_df_to_string(pnl_stats, index=True))
    # 4) Run the StrategyEvaluator notebook.
    if strategy_eval_config is not None:
        _LOG.debug(
            "\n%s", hprint.frame("Run strategy analyzer notebook", char1="<")
        )
        amp_dir = hgit.get_amp_abs_path()
        # TODO(gp): Rename -> Master_strategy_evaluator
        file_name = os.path.join(
            amp_dir,
            "core/dataflow_model/notebooks/Master_strategy_analyzer.ipynb",
        )
        #
        run_notebook_dir_tmp = os.path.join(
            run_notebook_dir, "run_strategy_analyzer"
        )
        #
        python_code = strategy_eval_config.to_python(check=True)
        env_var = "AM_CONFIG_CODE"
        pre_cmd = f'export {env_var}="{python_code}"'
        #
        hjupyte.run_notebook(file_name, run_notebook_dir_tmp, pre_cmd=pre_cmd)
    # 5) Freeze statistics from the `StrategyEvaluator` flow.
    # TODO(gp): Add some info from Strategy PnL.
    actual_outcome = "\n".join(actual_outcome)
    return actual_outcome


def _run_model(
    config_builder: str, experiment_builder: str, extra_opts: str, dst_dir: str
) -> None:
    # Execute a command line like:
    #   /app/amp/core/dataflow_model/run_experiment.py \
    #       --experiment_builder \
    #           amp.core.dataflow_model.master_experiment.run_experiment \
    #       --config_builder \
    #           'dataflow_lemonade.RH1E.RH1E_configs.build_model_configs("kibot_v1-top1.5T", 1)'
    #       --dst_dir .../run_model/oos_experiment.RH1E.kibot_v1-top1.5T \
    #       --clean_dst_dir \
    #       --no_confirm \
    #       --num_threads serial
    if os.path.exists(dst_dir):
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
    exec_filename = os.path.join(
        exec_filename, "amp/core/dataflow_model/run_experiment.py"
    )
    hdbg.dassert_exists(exec_filename)
    #
    cmd = []
    cmd.append(exec_filename)
    # Experiment builder.
    # E.g., "amp.core.dataflow_model.master_experiment.run_experiment"
    cmd.append(f"--experiment_builder {experiment_builder}")
    # Config builder.
    # E.g.,
    # builder = f'build_model_configs("{bm_config}", 1)'
    # config_builder = f'dataflow_lemonade.RH1E.RH1E_configs.{builder}'
    cmd.append(f"--config_builder '{config_builder}'")
    #
    cmd.append(f"--dst_dir {dst_dir}")
    cmd.append(opts)
    cmd = " ".join(cmd)
    hsysinte.system(cmd)
