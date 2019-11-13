#!/usr/bin/env python
"""
> run_notebook.py --notebook research/Task11_Model_for_1min_futures_data/Task11_Simple_model_for_1min_futures_data.ipynb --function build_configs --no_incremental --dst_dir tmp.run_notebooks/ --num_threads -1
"""

import argparse
import copy
import logging
import os

import tqdm
from joblib import Parallel, delayed

import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.printing as printing
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)


def build_configs(dst_dir, dry_run):
    config = cfg.Config()
    config_tmp = config.add_subconfig("read_data")
    config_tmp["file_name"] = None
    if dry_run:
        config_tmp["nrows"] = 10000
    #
    config["output_dir"] = dst_dir
    config["sim_tag"] = None
    #
    config["zscore_style"] = "rolling_std"
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


def _run_notebook(i, notebook_file, config, dst_dir):
    dbg.dassert_exists(notebook_file)
    dbg.dassert_isinstance(config, cfg.Config)
    dbg.dassert_exists(dst_dir)
    #
    dst_file = (
        dst_dir
        + "/"
        + os.path.basename(notebook_file).replace(".ipynb", ".%s.ipynb" % i)
    )
    dst_file = os.path.abspath(dst_file)
    #
    log_file = dst_dir + "/run_notebook.%s.log" % i
    log_file = os.path.abspath(os.path.abspath(log_file))
    # Execute notebook.
    _LOG.info("Executing notebook %s", i)
    cmd = 'export __CONFIG__="%s"' % config.to_python()
    cmd += (
        "; jupyter nbconvert"
        + " "
        + notebook_file
        + " --execute"
        + " --to notebook"
        + " --output "
        + dst_file
        + " --ExecutePreprocessor.kernel_name=python"
        +
        # https://github.com/ContinuumIO/anaconda-issues/issues/877
        " --ExecutePreprocessor.timeout=-1"
    )
    si.system(cmd, output_file=log_file)
    # Convert to html.
    _LOG.info("Converting notebook %s", i)
    html_file = dst_file.replace(".ipynb", ".html")
    log_file = log_file.replace(".log", ".html.log")
    cmd = (
        "jupyter nbconvert"
        + " "
        + dst_file
        + " --to html"
        + " --output "
        + html_file
    )
    si.system(cmd, output_file=log_file)


def _main(parser):
    args = parser.parse_args()
    dbg.init_logger(verb=args.log_level, use_exec_path=True)
    #
    dst_dir = os.path.abspath(args.dst_dir)
    io_.create_dir(dst_dir, incremental=not args.no_incremental)
    #
    _LOG.info("Executing function '%s()'", args.function)
    configs = eval(args.function + "(dst_dir, args.dry_run)")
    dbg.dassert_isinstance(configs, list)
    _LOG.info("Found %s configs", len(configs))
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
            _run_notebook(i, notebook_file, config, dst_dir)
    else:
        num_threads = int(num_threads)
        # -1 is interpreted by joblib like for all cores.
        _LOG.info("Using %d threads", num_threads)
        Parallel(n_jobs=num_threads, verbose=50)(
            delayed(_run_notebook)(i, notebook_file, config, dst_dir)
            for i, config in enumerate(configs)
        )


def _parse():
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
        "--num_threads",
        action="store",
        help="Number of threads to use (-1 to use all CPUs)",
        required=True,
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
        help="Function name to execute to generate configs",
    )
    parser.add_argument(
        "--dry_run",
        action="store_true",
        help="Run a short sim to sanity check the flow",
    )
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )
    return parser


if __name__ == "__main__":
    _main(_parse())
