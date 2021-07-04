#!/usr/bin/env python
r"""
Run a notebook given a config or a list of configs.

# Use example:
> run_notebook.py \
    --notebook nlp/notebooks/NLP_RP_pipeline.ipynb \
    --config_builder "nlp.build_configs.build_PTask1088_configs()" \
    --dst_dir nlp/test_results \
    --num_threads 2
"""

import argparse
import logging
import os
import sys
from typing import Optional

import joblib
import tqdm

import core.config as cconfig
import core.dataflow_model.utils as cdtfut
import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.parser as prsr
import helpers.printing as printing
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)


# #############################################################################


def _run_notebook(
    config: cconfig.Config,
    notebook_file: str,
    num_attempts: int,
    abort_on_error: bool,
    publish: bool,
) -> Optional[int]:
    """
    Run a notebook for a specific `Config`.

    :param config: config for the experiment
    :param notebook_file: path to file with experiment template
    :param num_attempts: maximum number of times to attempt running the
        notebook
    :param abort_on_error: if `True`, raise an error
    :param publish: publish notebook if `True`
    :return: if notebook is skipped ("success.txt" file already exists), return
        `None`; otherwise, return `rc`
    """
    cdtfut.setup_experiment_dir(config)
    # Prepare the destination file.
    idx = config[("meta", "id")]
    experiment_result_dir = config[("meta", "experiment_result_dir")]
    dst_file = os.path.join(
        experiment_result_dir,
        os.path.basename(notebook_file).replace(".ipynb", ".%s.ipynb" % idx),
    )
    _LOG.info("dst_file=%s", dst_file)
    dst_file = os.path.abspath(dst_file)
    # Export config function and its `id` to the notebook.
    config_builder = config[("meta", "config_builder")]
    dst_dir = config[("meta", "dst_dir")]
    cmd = [
        f'export __CONFIG_BUILDER__="{config_builder}";',
        f'export __CONFIG_IDX__="{idx}";',
        f'export __CONFIG_DST_DIR__="{dst_dir}"',
        f"; jupyter nbconvert {notebook_file}",
        "--execute",
        "--to notebook",
        f"--output {dst_file}",
        "--ExecutePreprocessor.kernel_name=python",
        # From https://github.com/ContinuumIO/anaconda-issues/issues/877
        "--ExecutePreprocessor.timeout=-1",
    ]
    cmd = " ".join(cmd)
    # Prepare the log file.
    log_file = os.path.join(experiment_result_dir, "run_notebook.%s.log" % idx)
    log_file = os.path.abspath(os.path.abspath(log_file))
    _LOG.info("log_file=%s", log_file)
    # TODO(gp): Repeating a command n-times is an idiom that we could move to
    #  system_interaction.
    # Try running the notebook up to `num_attempts` times.
    dbg.dassert_lte(1, num_attempts)
    rc = None
    for n in range(1, num_attempts + 1):
        if n > 1:
            _LOG.warning(
                "Run the notebook: %d / %d attempt",
                n,
                num_attempts,
            )
        _LOG.info("cmd='%s'", cmd)
        rc = si.system(cmd, output_file=log_file, abort_on_error=False)
        if rc == 0:
            _LOG.info("Running notebook was successful")
            break
    if rc != 0:
        # The notebook run wasn't successful.
        _LOG.error("Execution failed for experiment %d", idx)
        if abort_on_error:
            dbg.dfatal("Aborting")
        else:
            _LOG.error("Continuing execution of next experiments")
    else:
        # Mark as success.
        cdtfut.mark_config_as_success(experiment_result_dir)
        # Convert to HTML and publish.
        if publish:
            _LOG.info("Publishing notebook %d", idx)
            html_subdir_name = os.path.join(
                os.path.basename(dst_dir), experiment_result_dir
            )
            # TODO(gp): Look for the script.
            cmd = (
                "python amp/dev_scripts/notebooks/publish_notebook.py"
                + f" --file {dst_file}"
                + f" --subdir {html_subdir_name}"
                + " --action publish"
            )
            log_file = log_file.replace(".log", ".html.log")
            si.system(cmd, output_file=log_file)
    return rc


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    # Add common experiment options.
    parser = cdtfut.add_experiment_arg(parser)
    # Add notebook options.
    parser.add_argument(
        "--notebook",
        action="store",
        required=True,
        help="File storing the notebook to iterate over",
    )
    parser.add_argument(
        "--publish_notebook",
        action="store_true",
        help="Publish each notebook after it executes",
    )
    parser = prsr.add_verbosity_arg(parser)
    # TODO(gp): For some reason, not even this makes mypy happy.
    # cast(argparse.ArgumentParser, parser)
    return parser  # type: ignore


# TODO(gp): Make the notebook save the config that it sees. This might
#  make the code simpler and more robust.
# TODO(gp): We could try to serialize/deserialize the config and pass to the notebook
#  a pointer to the file.


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Create the dst dir.
    dst_dir = os.path.abspath(args.dst_dir)
    io_.create_dir(dst_dir, incremental=not args.clean_dst_dir)
    # Get the configs to run.
    configs = cdtfut.get_configs_from_command_line(args)
    # Get the notebook file.
    notebook_file = os.path.abspath(args.notebook)
    dbg.dassert_exists(notebook_file)
    # Parse command-line options.
    num_attempts = args.num_attempts
    abort_on_error = not args.skip_on_error
    publish = args.publish_notebook
    num_threads = args.num_threads
    # Execute.
    if num_threads == "serial":
        rcs = []
        for config in tqdm.tqdm(configs, desc="Running notebooks"):
            i = int(config[("meta", "id")])
            _LOG.debug("\n%s", printing.frame("Config %s" % i))
            #
            rc = _run_notebook(
                config,
                notebook_file,
                num_attempts,
                abort_on_error,
                publish,
            )
            rcs.append(rc)
    else:
        num_threads = int(num_threads)
        # -1 is interpreted by joblib like for all cores.
        _LOG.info("Using %d threads", num_threads)
        rcs = joblib.Parallel(n_jobs=num_threads, verbose=50)(
            joblib.delayed(_run_notebook)(
                config,
                notebook_file,
                num_attempts,
                abort_on_error,
                publish,
            )
            for config in configs
        )
    # Report failing experiments.
    rc = cdtfut.report_failed_experiments(configs, rcs)
    sys.exit(rc)


if __name__ == "__main__":
    _main(_parse())
