#!/usr/bin/env python
r"""
Run a notebook given a config or a list of configs.

Use example:
> run_notebook.py --dst_dir nlp/test_results \
 --notebook nlp/notebooks/NLP_RP_pipeline.ipynb \
 --function "nlp.build_configs.build_PartTask1088_configs()" \
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
    publish: bool,
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
    :param publish: publish notebook iff `True`
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
    # If there is already a success file in the dir, skip the experiment.
    file_name = os.path.join(experiment_result_dir, "success.txt")
    if os.path.exists(file_name):
        _LOG.warning("Found file '%s': skipping simulation run", file_name)
        return
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
        f'export __CONFIG_BUILDER__="{config_builder}"; '
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
    if publish:
        _LOG.info("Converting notebook %s", i)
        log_file = log_file.replace(".log", ".html.log")
        cmd = (
            "python amp/dev_scripts/notebooks/publish_notebook.py"
            + f" --file {dst_file}"
            + f" --subdir {html_subdir_name}"
            + " --action publish"
        )
        si.system(cmd, output_file=log_file)
    # Publish an empty file to indicate a successful finish.
    file_name = os.path.join(experiment_result_dir, "success.txt")
    _LOG.info("file_name=%s", file_name)
    io_.to_file(file_name, "")


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    #
    dst_dir = os.path.abspath(args.dst_dir)
    io_.create_dir(dst_dir, incremental=not args.no_incremental)
    #
    config_builder = args.function
    _LOG.info("Executing function '%s'", config_builder)
    configs = ccfgbld.get_configs_from_builder(config_builder)
    dbg.dassert_isinstance(configs, list)
    ccfgbld.assert_on_duplicated_configs(configs)
    configs = ccfgbld.add_result_dir(dst_dir, configs)
    configs = ccfgbld.add_config_idx(configs)
    #
    if args.index:
        ind = int(args.index)
        dbg.dassert_lte(0, ind)
        dbg.dassert_lt(ind, len(configs))
        _LOG.warning(
            "Only config %s will be executed due to passing --index", ind
        )
        configs = [x for x in configs if int(x[("meta", "id")]) == ind]
    elif args.start_from_index:
        start_from_index = int(args.start_from_index)
        dbg.dassert_lte(0, start_from_index)
        dbg.dassert_lt(start_from_index, len(configs))
        _LOG.warning(
            "Only configs %s and higher will be executed due to passing --start_from_index",
            start_from_index,
        )
        configs = [
            x for x in configs if int(x[("meta", "id")]) >= start_from_index
        ]
    _LOG.info("Created %s config(s)", len(configs))
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
    publish = args.publish_notebook
    #
    num_threads = args.num_threads
    if num_threads == "serial":
        for config in tqdm.tqdm(configs):
            i = int(config[("meta", "id")])
            _LOG.debug("\n%s", printing.frame("Config %s" % i))
            #
            _run_notebook(
                i, notebook_file, dst_dir, config, config_builder, publish
            )
    else:
        num_threads = int(num_threads)
        # -1 is interpreted by joblib like for all cores.
        _LOG.info("Using %d threads", num_threads)
        joblib.Parallel(n_jobs=num_threads, verbose=50)(
            joblib.delayed(_run_notebook)(
                int(config[("meta", "id")]),
                notebook_file,
                dst_dir,
                config,
                config_builder,
                publish
            )
            for config in configs
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
    parser.add_argument(
        "--function",
        action="store",
        required=True,
        help="Full function to create configs, e.g., "
        "nlp.build_configs.build_PartTask1297_configs("
        "random_seed_variants=[911,2,42,0])",
    )
    parser.add_argument(
        "--index", action="store", help="Run a single experiment",
    )
    parser.add_argument(
        "--start_from_index",
        action="store",
        help="Run experiments starting from a specified index",
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
    parser.add_argument(
        "--publish_notebook",
        action="store_true",
        help="Publish each notebook after it executes",
    )
    prsr.add_verbosity_arg(parser)
    return parser


if __name__ == "__main__":
    _main(_parse())
