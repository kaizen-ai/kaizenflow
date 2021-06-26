"""
Contain functions used by both `run_experiment.py` and `run_notebook.py` to run
experiments.

Import as:

import core.dataflow_model.utils as cdtfut
"""

# TODO(gp): experiment_utils.py

import argparse
import logging
import os
import sys
from typing import List, Optional, Tuple

import core.config as cconfig
import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.pickle_ as hpickle
import helpers.printing as hprint

_LOG = logging.getLogger(__name__)


# TODO(gp): Use `add_parallel_processing_arg()`.
def add_experiment_arg(
    parser: argparse.ArgumentParser,
) -> argparse.ArgumentParser:
    """
    Add common command line options to run the experiments.
    """
    parser.add_argument(
        "--dst_dir",
        action="store",
        required=True,
        help="Directory storing the results",
    )
    parser.add_argument(
        "--clean_dst_dir",
        action="store_true",
        help="Delete the destination dir before running experiments",
    )
    parser.add_argument(
        "--no_incremental",
        action="store_true",
        help="Skip experiments already performed",
    )
    parser.add_argument(
        "--config_builder",
        action="store",
        required=True,
        help="""
        Full invocation of Python function to create configs, e.g.,
        `nlp.build_configs.build_Task1297_configs(random_seed_variants=[911,2,0])`
        """,
    )
    parser.add_argument(
        "--skip_on_error",
        action="store_true",
        help="Continue execution of experiments after encountering an error",
    )
    parser.add_argument(
        "--index",
        action="store",
        default=None,
        help="Run a single experiment corresponding to the i-th config",
    )
    parser.add_argument(
        "--start_from_index",
        action="store",
        default=None,
        help="Run experiments starting from a specified index",
    )
    parser.add_argument(
        "--only_print_configs",
        action="store_true",
        help="Print the configs and exit",
    )
    parser.add_argument(
        "--dry_run",
        action="store_true",
        help="Print configs and exit without running",
    )
    # TODO(gp): Add an option to run a short experiment to sanity check the flow.
    parser.add_argument(
        "--num_attempts",
        default=1,
        type=int,
        help="Repeat running the experiment up to `num_attempts` times",
        required=False,
    )
    parser.add_argument(
        "--num_threads",
        action="store",
        help="Number of threads to use (-1 to use all CPUs)",
        required=True,
    )
    return parser


def skip_configs_already_executed(
    configs: List[cconfig.Config], incremental: bool
) -> Tuple[List[cconfig.Config], int]:
    """
    Remove from the list the configs that have already been executed.
    """
    configs_out = []
    num_skipped = 0
    for config in configs:
        # If there is already a success file in the dir, skip the experiment.
        experiment_result_dir = config[("meta", "experiment_result_dir")]
        file_name = os.path.join(experiment_result_dir, "success.txt")
        if incremental and os.path.exists(file_name):
            idx = config[("meta", "id")]
            _LOG.warning("Found file '%s': skipping run %d", file_name, idx)
            num_skipped += 1
        else:
            configs_out.append(config)
    return configs_out, num_skipped


def mark_config_as_success(experiment_result_dir: str) -> None:
    """
    Publish an empty file to indicate a successful finish.
    """
    file_name = os.path.join(experiment_result_dir, "success.txt")
    _LOG.info("Creating file_name='%s'", file_name)
    io_.to_file(file_name, "success")


def setup_experiment_dir(config: cconfig.Config) -> None:
    """
    Set up the directory and the book-keeping artifacts for the experiment
    running `config`.

    :return: whether we need to run this config or not
    """
    dbg.dassert_isinstance(config, cconfig.Config)
    # Create subdirectory structure for experiment results.
    experiment_result_dir = config[("meta", "experiment_result_dir")]
    _LOG.info("Creating experiment dir '%s'", experiment_result_dir)
    io_.create_dir(experiment_result_dir, incremental=True)
    # Prepare book-keeping files.
    file_name = os.path.join(experiment_result_dir, "config.pkl")
    _LOG.debug("Saving '%s'", file_name)
    hpickle.to_pickle(config, file_name)
    #
    file_name = os.path.join(experiment_result_dir, "config.txt")
    _LOG.debug("Saving '%s'", file_name)
    io_.to_file(file_name, str(config))


def select_config(
    configs: List[cconfig.Config],
    index: Optional[int],
    start_from_index: Optional[int],
) -> List[cconfig.Config]:
    """
    Select configs to run based on the command line parameters.

    :param configs: list of configs
    :param index: index of a config to execute, if not `None`
    :param start_from_index: index of a config to start execution from, if not `None`
    :return: list of configs to execute
    """
    dbg.dassert_container_type(configs, List, cconfig.Config)
    dbg.dassert_lte(1, len(configs))
    if index is not None:
        index = int(index)
        _LOG.warning("Only config %d will be executed because of --index", index)
        dbg.dassert_lte(0, index)
        dbg.dassert_lt(index, len(configs))
        configs = [configs[index]]
    elif start_from_index is not None:
        start_from_index = int(start_from_index)
        _LOG.warning(
            "Only configs >= %d will be executed because of --start_from_index",
            start_from_index,
        )
        dbg.dassert_lte(0, start_from_index)
        dbg.dassert_lt(start_from_index, len(configs))
        configs = [c for idx, c in enumerate(configs) if idx >= start_from_index]
    _LOG.info("Selected %s configs", len(configs))
    dbg.dassert_container_type(configs, List, cconfig.Config)
    return configs


def get_configs_from_command_line(
    args: argparse.Namespace,
) -> List[cconfig.Config]:
    """
    Return all the configs to run given the command line interface.

    The configs are patched with all the information from the command
    line (e.g., `idx`, `config_builder`, `experiment_builder`,
    `dst_dir`, `experiment_result_dir`).
    """
    # Build the map with the config parameters.
    config_builder = args.config_builder
    configs = cconfig.get_configs_from_builder(config_builder)
    params = {
        "config_builder": args.config_builder,
        "dst_dir": args.dst_dir,
    }
    if hasattr(args, "experiment_builder"):
        params["experiment_builder"] = args.experiment_builder
    # Patch the configs with the command line parameters.
    configs = cconfig.patch_configs(configs, params)
    _LOG.info("Generated %d configs from the builder", len(configs))
    # Select the configs based on command line options.
    index = args.index
    start_from_index = args.start_from_index
    configs = select_config(configs, index, start_from_index)
    _LOG.info("Selected %d configs from command line", len(configs))
    # Remove the configs already executed.
    incremental = not args.no_incremental
    configs, num_skipped = skip_configs_already_executed(configs, incremental)
    _LOG.info("Removed %d configs since already executed", num_skipped)
    _LOG.info("Need to execute %d configs", len(configs))
    # Handle --dry_run, if needed.
    if args.dry_run:
        _LOG.warning(
            "The following configs will not be executed due to passing --dry_run:"
        )
        for i, config in enumerate(configs):
            print(hprint.frame("Config %d/%s" % (i + 1, len(configs))))
            print(str(config))
        sys.exit(0)
    return configs


def report_failed_experiments(
    configs: List[cconfig.Config], rcs: List[int]
) -> int:
    """
    Report failing experiments.

    :return: return code
    """
    # Get the experiment idxs.
    experiment_ids = [int(config[("meta", "id")]) for config in configs]
    # Match experiment idxs with their return codes.
    failed_experiment_ids = [
        i for i, rc in zip(experiment_ids, rcs) if rc is not None and rc != 0
    ]
    # Report.
    if failed_experiment_ids:
        _LOG.error(
            "There are %d failed experiments: %s",
            len(failed_experiment_ids),
            failed_experiment_ids,
        )
        rc = -1
    else:
        rc = 0
    # TODO(gp): Save on a file the failed experiments' configs.
    return rc
