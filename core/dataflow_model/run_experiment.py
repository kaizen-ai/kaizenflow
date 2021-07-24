#!/usr/bin/env python
r"""
Run an experiment consisting of multiple model runs based on the passed
- `config_builder`, which describes DAG and configs
- `experiment_builder`, which describes the model driver

# Run an RH1E pipeline using 2 threads:
> run_experiment.py \
    --experiment_builder "core.dataflow_model.master_experiment.run_experiment" \
    --config_builder "dataflow_lem.RH1E.config.build_15min_model_configs()" \
    --dst_dir experiment1 \
    --num_threads 2
"""
import argparse
import logging
import os
from typing import cast

import core.config as cconfig
import core.dataflow_model.utils as cdtfut
import helpers.datetime_ as hdatetime
import helpers.dbg as dbg
import helpers.git as git
import helpers.joblib_helpers as hjoblib
import helpers.printing as hprint
import helpers.parser as prsr
import helpers.system_interaction as hsinte

_LOG = logging.getLogger(__name__)


# #############################################################################


def _run_experiment(
    config: cconfig.Config,
    #
    incremental: bool,
    num_attempts: int,
) -> int:
    """
    Run a pipeline for a specific `Config`.

    :param config: config for the experiment
    :param num_attempts: maximum number of times to attempt running the
        notebook
    :return: rc from executing the pipeline
    """
    dbg.dassert_eq(1, num_attempts, "Multiple attempts not supported yet")
    _ = incremental
    cdtfut.setup_experiment_dir(config)
    # Execute experiment.
    # TODO(gp): Rename id -> idx everywhere
    #  jackpy "meta" | grep id | grep config
    idx = config[("meta", "id")]
    _LOG.info("\n%s", hprint.frame(f"Executing experiment for config {idx}"))
    _LOG.info("config=\n%s", config)
    dst_dir = config[("meta", "dst_dir")]
    # Prepare the log file.
    # TODO(gp): -> experiment_dst_dir
    experiment_result_dir = config[("meta", "experiment_result_dir")]
    log_file = os.path.join(experiment_result_dir, "run_experiment.%s.log" % idx)
    log_file = os.path.abspath(os.path.abspath(log_file))
    # Prepare command line.
    experiment_builder = config[("meta", "experiment_builder")]
    config_builder = config[("meta", "config_builder")]
    file_name = "run_experiment_stub.py"
    exec_name = git.find_file_in_git_tree(file_name, super_module=True)
    cmd = [
        exec_name,
        f"--experiment_builder '{experiment_builder}'",
        f"--config_builder '{config_builder}'",
        f"--config_idx {idx}",
        f"--dst_dir {dst_dir}",
        "-v INFO",
    ]
    cmd = " ".join(cmd)
    # Execute.
    _LOG.info("Executing '%s'", cmd)
    rc = hsinte.system(
        cmd, output_file=log_file, suppress_output=False, abort_on_error=False
    )
    _LOG.info("Executed cmd")
    # TODO(gp): We don't really have to catch the error and rethrow since the outer
    #  layer handles the exceptions.
    if rc != 0:
        # The notebook run wasn't successful.
        msg = f"Execution failed for experiment {idx}"
        _LOG.error(msg)
        raise RuntimeError(msg)
    else:
        # Mark as success.
        cdtfut.mark_config_as_success(experiment_result_dir)
    rc = cast(int, rc)
    return rc


def _get_workload(args: argparse.Namespace) -> hjoblib.Workload:
    """
    Prepare the workload using the parameters from command line.
    """
    # Get the configs to run.
    configs = cdtfut.get_configs_from_command_line(args)
    # Prepare the tasks.
    tasks = []
    for config in configs:
        task = (
            # args.
            (config,),
            # kwargs.
            {},
        )
        tasks.append(task)
    #
    func_name = "_run_experiment"
    workload = (_run_experiment, func_name, tasks)
    hjoblib.validate_workload(workload)
    return workload


# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    # Add common experiment options.
    parser = cdtfut.add_experiment_arg(parser, dst_dir_required=True)
    # Add pipeline options.
    parser.add_argument(
        "--experiment_builder",
        action="store",
        required=True,
        help="File storing the pipeline to iterate over",
    )
    parser = prsr.add_verbosity_arg(parser)
    return parser  # type: ignore


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Create the dst dir.
    dst_dir, clean_dst_dir = prsr.parse_dst_dir_arg(args)
    _ = clean_dst_dir
    # Prepare the workload.
    workload = _get_workload(args)
    # Parse command-line options.
    dry_run = args.dry_run
    num_threads = args.num_threads
    incremental = not args.no_incremental
    abort_on_error = not args.skip_on_error
    num_attempts = args.num_attempts
    # Prepare the log file.
    timestamp = hdatetime.get_timestamp("et")
    log_file = os.path.join(dst_dir, f"log.{timestamp}.txt")
    _LOG.info("log_file='%s'", log_file)
    # Execute.
    hjoblib.parallel_execute(
        workload,
        dry_run,
        num_threads,
        incremental,
        abort_on_error,
        num_attempts,
        log_file,
    )
    #
    _LOG.info("dst_dir='%s'", dst_dir)
    _LOG.info("log_file='%s'", log_file)


if __name__ == "__main__":
    _main(_parse())
