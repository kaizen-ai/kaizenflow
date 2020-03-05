#!/usr/bin/env python
r"""
Run a notebook given a config or a list of configs.

Use example:
> run_notebook.py --dst_dir nlp/test_results \
 --no_incremental \
 --notebook nlp/notebooks/PartTask1102_RP_Pipeline.ipynb \
 --function ncfgbld.build_PartTask1088_configs \
  --num_threads 2
"""

import argparse
import copy
import logging
import os
from typing import List

import joblib
import tqdm

import core.config as cfg
import core.config_builders as ccfgbld
import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.parser as prsr
import helpers.pickle_ as hpickle
import helpers.printing as printing
import helpers.system_interaction as si
import nlp.build_configs as ncfgbld  # noqa: F401 # pylint: disable=unused-import

_LOG = logging.getLogger(__name__)


def build_configs(dst_dir: str, dry_run: bool) -> List[cfg.Config]:
    # TODO (*) Where to move this file?
    config = cfg.Config()
    config_tmp = config.add_subconfig("read_data")
    config_tmp["file_name"] = None
    if dry_run:
        config_tmp["nrows"] = 10000
    #
    config["output_dir"] = dst_dir
    config["sim_tag"] = None
    #
    config["zscore_style"] = "compute_rolling_std"
    config["zscore_com"] = 28
    config["filter_ath"] = True
    config["target_y_var"] = "zret_0"
    config["delay_lag"] = 1
    # config["delay_lag"] = 2
    config["num_lags"] = 3
    # config["num_lags"] = 5
    # config["num_lags"] = 10
    config["cv_split_style"] = "TimeSeriesSplit"
    config["cv_n_splits"] = 5
    #
    configs = []
    if dry_run:
        futures = "ES CL".split()
    else:
        futures = "AD C GC NG S CL ES HE NN TY".split()
        # futures = "ES CL".split()
    for f in futures:
        config["descr"] = f
        config_tmp = copy.deepcopy(config)
        file_name = "s3://kibot/All_Futures_Contracts_1min/%s.csv.gz" % f
        config_tmp["file_name"] = file_name
        config_tmp["sim_tag"] = f
        configs.append(config_tmp)
    return configs


# #############################################################################


def _run_notebook(
    i: int,
    notebook_file: str,
    dst_dir: str,
    config: cfg.Config,
    config_builder: str,
) -> None:
    """
    Run a notebook for the particular config from a list.

    The `config_builder` is passed inside the notebook to generate a list
    of all configs to be run as part of a series of experiments, but only the
    `i`-th config is run inside a particular notebook.
    :param i: Index of config in a list of configs
    :param notebook_file: Path to file with experiment template
    :param dst_dir: Path to directory with results
    :param config: Config for the experiment
    :param config_builder: Function used to generate all configs
    :return:
    """
    dbg.dassert_exists(notebook_file)
    dbg.dassert_isinstance(config, cfg.Config)
    dbg.dassert_exists(dst_dir)
    # Create subdirectory structure for simulation results.
    result_subdir = "result_%s" % i
    html_subdir_name = os.path.join(os.path.basename(dst_dir), result_subdir)
    # TODO(gp): experiment_result_dir -> experiment_result_dir.
    experiment_result_dir = os.path.join(dst_dir, result_subdir)
    config = ccfgbld.set_experiment_result_dir(experiment_result_dir, config)
    _LOG.info("experiment_result_dir=%s", experiment_result_dir)
    io_.create_dir(experiment_result_dir, incremental=True)
    # Generate book-keeping files.
    file_name = os.path.join(experiment_result_dir, "config.pkl")
    _LOG.info("file_name=%s", file_name)
    hpickle.to_pickle(config, file_name)
    #
    file_name = os.path.join(experiment_result_dir, "config.txt")
    _LOG.info("file_name=%s", file_name)
    io_.to_file(file_name, str(config))
    #
    file_name = os.path.join(experiment_result_dir, "config_builder.txt")
    _LOG.info("file_name=%s", file_name)
    io_.to_file(
        file_name,
        "Config builder: %s\nConfig index: %s" % (config_builder, str(i)),
    )
    #
    dst_file = os.path.join(
        experiment_result_dir,
        os.path.basename(notebook_file).replace(".ipynb", ".%s.ipynb" % i),
    )
    _LOG.info(dst_file)
    dst_file = os.path.abspath(dst_file)
    log_file = os.path.join(experiment_result_dir, "run_notebook.%s.log" % i)
    log_file = os.path.abspath(os.path.abspath(log_file))
    # Execute notebook.
    _LOG.info("Executing notebook %s", i)
    # Export config function and its id to the notebook.
    cmd = (
        f'export __CONFIG_BUILDER__="${config_builder}"; '
        + f'export __CONFIG_IDX__="{i}"; '
        + f'export __CONFIG_DST_DIR__="{experiment_result_dir}"'
    )
    cmd += (
        f"; jupyter nbconvert {notebook_file} "
        + " --execute"
        + " --to notebook"
        + f" --output {dst_file}"
        + " --ExecutePreprocessor.kernel_name=python"
        +
        # https://github.com/ContinuumIO/anaconda-issues/issues/877
        " --ExecutePreprocessor.timeout=-1"
    )
    si.system(cmd, output_file=log_file)
    # Convert to html and publish.
    _LOG.info("Converting notebook %s", i)
    log_file = log_file.replace(".log", ".html.log")
    cmd = (
        "python amp/dev_scripts/notebooks/publish_notebook.py"
        + f" --file {dst_file}"
        + f" --subdir {html_subdir_name}"
        + " --action publish"
    )
    si.system(cmd, output_file=log_file)


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    #
    dst_dir = os.path.abspath(args.dst_dir)
    io_.create_dir(dst_dir, incremental=not args.no_incremental)
    #
    _LOG.info("Executing function '%s(%s)'", args.function, args.function_args)
    config_builder = f"{args.function}({args.function_args})"
    configs = eval(config_builder)
    dbg.dassert_isinstance(configs, list)
    ccfgbld.assert_on_duplicated_configs(configs)
    configs = ccfgbld.add_result_dir(dst_dir, configs)
    configs = ccfgbld.add_config_idx(configs)
    _LOG.info("Created %s configs", len(configs))
    if args.dry_run:
        _LOG.warning(
            "The following configs will not be executed due to passing --dry_run:"
        )
        for i, config in enumerate(configs):
            print("config_%s:\n %s", i, config)
        return
    #
    notebook_file = args.notebook
    notebook_file = os.path.abspath(notebook_file)
    dbg.dassert_exists(notebook_file)
    #
    num_threads = args.num_threads
    if num_threads == "serial":
        for i, config in tqdm.tqdm(enumerate(configs)):
            _LOG.debug("\n%s", printing.frame("Config %s" % i))
            #
            _run_notebook(i, notebook_file, dst_dir, config, config_builder)
    else:
        num_threads = int(num_threads)
        # -1 is interpreted by joblib like for all cores.
        _LOG.info("Using %d threads", num_threads)
        joblib.Parallel(n_jobs=num_threads, verbose=50)(
            joblib.delayed(_run_notebook)(
                i, notebook_file, dst_dir, config, config_builder
            )
            for i, config in enumerate(configs)
        )


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--dst_dir",
        action="store",
        required=True,
        help="Directory storing the results",
    )
    parser.add_argument(
        "--no_incremental",
        action="store_true",
        help="Delete the dir before storing results",
    )
    parser.add_argument(
        "--notebook",
        action="store",
        required=True,
        help="File storing the notebook to iterate over",
    )
    # TODO(gp): Pass the entire python function:
    # `ncfgbld.build_PartTask1297_configs(random_seed_variants='[911,2,42,0])'
    parser.add_argument(
        "--function",
        action="store",
        required=True,
        help="Function name to execute to generate configs. Make sure to include import:"
        " NLP config builders are imported through `ncfgbld`.",
    )
    parser.add_argument(
        "--function_args",
        action="store",
        default="",
        help="Arguments to pass into function",
    )
    parser.add_argument(
        "--dry_run",
        action="store_true",
        help="Run a short sim to sanity check the flow",
    )
    parser.add_argument(
        "--num_threads",
        action="store",
        help="Number of threads to use (-1 to use all CPUs)",
        required=True,
    )
    prsr.add_verbosity_arg(parser)
    return parser


if __name__ == "__main__":
    _main(_parse())
