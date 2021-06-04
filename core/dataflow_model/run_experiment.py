#!/usr/bin/env python
r"""
Run an experiment given a list of configs.

# Run an RH1E pipeline using 2 threads:
> run_experiment.py \
    --experiment_builder "core.dataflow_model.master_experiment.run_experiment" \
    --config_builder "dataflow_lemonade.RH1E.task89_config_builder.build_15min_ar_model_configs()" \
    --dst_dir experiment1 \
    --num_threads 2
"""
import argparse
import logging
import os
import sys

import joblib
import tqdm

import core.config as cfg
import core.dataflow_model.utils as cdtfut
import helpers.dbg as dbg
import helpers.git as git
import helpers.io_ as io_
import helpers.parser as prsr
import helpers.printing as printing
import helpers.system_interaction as hsinte

_LOG = logging.getLogger(__name__)


# #############################################################################


def _run_experiment(
    config: cfg.Config,
    num_attempts: int,
    abort_on_error: bool,
) -> int:
    """
    Run a pipeline for a specific `Config`.

    :param config: config for the experiment
    :param num_attempts: maximum number of times to attempt running the
        notebook
    :param abort_on_error: if `True`, raise an error
    :return: rc from executing the pipeline
    """
    dbg.dassert_eq(1, num_attempts, "Multiple attempts not supported yet")
    cdtfut.setup_experiment_dir(config)
    # Execute experiment.
    # TODO(gp): Rename id -> idx everywhere
    #  jackpy "meta" | grep id | grep config
    idx = config[("meta", "id")]
    _LOG.info("Executing experiment for config %d\n%s", idx, config)
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
    if rc != 0:
        # The notebook run wasn't successful.
        _LOG.error("Execution failed for experiment %d", idx)
        if abort_on_error:
            dbg.dfatal("Aborting on experiment error")
        else:
            _LOG.error("Continuing execution for next experiments")
    else:
        # Mark as success.
        cdtfut.mark_config_as_success(experiment_result_dir)
    return rc


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    # Add common experiment options.
    parser = cdtfut.add_experiment_arg(parser)
    # Add pipeline options.
    parser.add_argument(
        "--experiment_builder",
        action="store",
        required=True,
        help="File storing the pipeline to iterate over",
    )
    parser = prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Create the dst dir.
    dst_dir = os.path.abspath(args.dst_dir)
    io_.create_dir(dst_dir, incremental=not args.clean_dst_dir)
    # Get the configs to run.
    configs = cdtfut.get_configs_from_command_line(args)
    # Parse command-line options.
    num_attempts = args.num_attempts
    abort_on_error = not args.skip_on_error
    num_threads = args.num_threads
    # TODO(gp): Try to factor out this. Pass a function and a list of params.
    # Execute.
    if num_threads == "serial":
        rcs = []
        for i, config in tqdm.tqdm(enumerate(configs)):
            _LOG.debug("\n%s", printing.frame("Config %s" % i))
            #
            rc = _run_experiment(
                config,
                num_attempts,
                abort_on_error,
            )
            rcs.append(rc)
    else:
        num_threads = int(num_threads)
        # -1 is interpreted by joblib like for all cores.
        _LOG.info("Using %d threads", num_threads)
        rcs = joblib.Parallel(n_jobs=num_threads, verbose=50)(
            joblib.delayed(_run_experiment)(
                config,
                num_attempts,
                abort_on_error,
            )
            for config in configs
        )
    # Report failing experiments.
    rc = cdtfut.report_failed_experiments(configs, rcs)
    sys.exit(rc)


if __name__ == "__main__":
    _main(_parse())
