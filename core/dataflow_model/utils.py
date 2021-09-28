"""
Contain functions used by both `run_experiment.py` and `run_notebook.py` to run
experiments.

Import as:

import core.dataflow_model.utils as cdtfmouti
"""

# TODO(gp): -> experiment_utils.py
# TODO(gp): All these functions should be tested. The problem is that these functions
#  rely on results from experiments and the format of the experiment is still in flux.
#  One approach is to:
#  - have unit tests that run short experiments (typically disabled)
#  - save the results of the experiments on S3 (or on disk)
#  - use the S3 experiments as mocks for the real unit tests
#  If the format of the experiment changes, we can just re-run the mock experiments
#  and refresh them on S3.

import argparse
import collections
import glob
import json
import logging
import os
import re
import sys
from typing import Any, Dict, Iterable, List, Match, Optional, Tuple, cast

import pandas as pd
from tqdm.autonotebook import tqdm

import core.config as cconfig
import core.dataflow as dtf
import core.signal_processing as csipro
import helpers.dbg as hdbg
import helpers.io_ as hio
import helpers.parser as hparser
import helpers.pickle_ as hpickle
import helpers.printing as hprintin
import helpers.s3 as hs3

_LOG = logging.getLogger(__name__)


def add_experiment_arg(
    parser: argparse.ArgumentParser,
    dst_dir_required: bool,
    dst_dir_default: Optional[str] = None,
) -> argparse.ArgumentParser:
    """
    Add common command line options to run experiments and notebooks.

    :param dst_dir_required: whether the user must specify a destination directory
        or not. If not, a default value should be passed through `dst_dir_default`
    :param dst_dir_default: a default destination dir
    """
    parser = hparser.add_dst_dir_arg(
        parser, dst_dir_required=dst_dir_required, dst_dir_default=dst_dir_default
    )
    parser = hparser.add_parallel_processing_arg(parser)
    parser.add_argument(
        "--index",
        action="store",
        default=None,
        help="Run a single experiment corresponding to the i-th config",
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
        "--start_from_index",
        action="store",
        default=None,
        help="Run experiments starting from a specified index",
    )
    return parser  # type: ignore


# TODO(gp): Generalize this logic for `parallel_execute`.
#  Each task writes in a directory and if it terminates, the success.txt file is
#  written.
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
    hio.to_file(file_name, "success")


def setup_experiment_dir(config: cconfig.Config) -> None:
    """
    Set up the directory and the book-keeping artifacts for the experiment
    running `config`.

    :return: whether we need to run this config or not
    """
    hdbg.dassert_isinstance(config, cconfig.Config)
    # Create subdirectory structure for experiment results.
    experiment_result_dir = config[("meta", "experiment_result_dir")]
    _LOG.info("Creating experiment dir '%s'", experiment_result_dir)
    hio.create_dir(experiment_result_dir, incremental=True)
    # Prepare book-keeping files.
    file_name = os.path.join(experiment_result_dir, "config.pkl")
    _LOG.debug("Saving '%s'", file_name)
    hpickle.to_pickle(config, file_name)
    #
    file_name = os.path.join(experiment_result_dir, "config.txt")
    _LOG.debug("Saving '%s'", file_name)
    hio.to_file(file_name, str(config))


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
    hdbg.dassert_container_type(configs, List, cconfig.Config)
    hdbg.dassert_lte(1, len(configs))
    if index is not None:
        index = int(index)
        _LOG.warning("Only config %d will be executed because of --index", index)
        hdbg.dassert_lte(0, index)
        hdbg.dassert_lt(index, len(configs))
        configs = [configs[index]]
    elif start_from_index is not None:
        start_from_index = int(start_from_index)
        _LOG.warning(
            "Only configs >= %d will be executed because of --start_from_index",
            start_from_index,
        )
        hdbg.dassert_lte(0, start_from_index)
        hdbg.dassert_lt(start_from_index, len(configs))
        configs = [c for idx, c in enumerate(configs) if idx >= start_from_index]
    _LOG.info("Selected %s configs", len(configs))
    hdbg.dassert_container_type(configs, List, cconfig.Config)
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
            print(hprintin.frame("Config %d/%s" % (i + 1, len(configs))))
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
    # Get the experiment selected_idxs.
    experiment_ids = [int(config[("meta", "id")]) for config in configs]
    # Match experiment selected_idxs with their return codes.
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


# #############################################################################
# Load and save experiment `ResultBundle`.
# #############################################################################


def save_experiment_result_bundle(
    config: cconfig.Config,
    result_bundle: dtf.ResultBundle,
    file_name: str = "result_bundle.pkl",
) -> None:
    """
    Save the `ResultBundle` from running `Config`.
    """
    # TODO(Paul): Consider having the caller provide the dir instead.
    file_name = os.path.join(config["meta", "experiment_result_dir"], file_name)
    result_bundle.to_pickle(file_name, use_pq=True)


# #############################################################################


def _retrieve_archived_experiment_artifacts_from_S3(
    s3_file_name: str,
    dst_dir: str,
    aws_profile: str,
) -> str:
    """
    Retrieve a package containing experiment artifacts from S3.

    :param s3_file_name: S3 file name containing the archive.
        E.g., s3://alphamatic-data/experiments/experiment.RH1E.v1.20210726-20_09_53.5T.tgz
    :param dst_dir: where to save the data
    :param aws_profile: the AWS profile to use it to access the archive
    :return: path to local dir with the content of the decompressed archive
    """
    hs3.dassert_is_s3_path(s3_file_name)
    # Get the file name and the enclosing dir name.
    dir_name = os.path.basename(os.path.dirname(s3_file_name))
    if dir_name != "experiments":
        _LOG.warning(
            "The file name '%s' is not under `experiments` dir", s3_file_name
        )
    hdbg.dassert_file_extension(s3_file_name, "tgz")
    tgz_dst_dir = hs3.retrieve_archived_data_from_s3(
        s3_file_name, dst_dir, aws_profile
    )
    _LOG.info("Retrieved artifacts to '%s'", tgz_dst_dir)
    return tgz_dst_dir  # type: ignore[no-any-return]


def _get_experiment_subdirs(
    src_dir: str,
    selected_idxs: Optional[Iterable[int]] = None,
    aws_profile: Optional[str] = None,
) -> Tuple[str, Dict[int, str]]:
    """
    Get the subdirectories under `src_dir` with a format like `result_*`.

    This function works also for archived (S3 or local) tarballs of experiment results.

    :param src_dir: directory with results or S3 path to archive
    :param selected_idxs: config indices to consider. `None` means all available
        experiments
    :return: the dir used to retrieve the experiment subdirectory,
        dict `config idx` to `experiment subdirectory`
    """
    # Handle the situation where the file is an archived file.
    if src_dir.endswith(".tgz"):
        scratch_dir = "."
        if hs3.is_s3_path(src_dir):
            hdbg.dassert_is_not(aws_profile, None)
            aws_profile = cast(str, aws_profile)
            tgz_file = _retrieve_archived_experiment_artifacts_from_S3(
                src_dir, scratch_dir, aws_profile
            )
        else:
            tgz_file = src_dir
        # Expand.
        src_dir = hs3.expand_archived_data(tgz_file, scratch_dir)
        _LOG.debug("src_dir=%s", src_dir)
    # Retrieve all the subdirectories in `src_dir` that store results.
    hdbg.dassert_dir_exists(src_dir)
    subdirs = [d for d in glob.glob(f"{src_dir}/result_*") if os.path.isdir(d)]
    _LOG.info("Found %d experiment subdirs in '%s'", len(subdirs), src_dir)
    # Build a mapping from `config_idx` to `experiment_dir`.
    config_idx_to_dir: Dict[int, str] = {}
    for subdir in subdirs:
        _LOG.debug("subdir='%s'", subdir)
        # E.g., `result_123"
        m = re.match(r"^result_(\d+)$", os.path.basename(subdir))
        hdbg.dassert(m)
        m = cast(Match[str], m)
        key = int(m.group(1))
        hdbg.dassert_not_in(key, config_idx_to_dir)
        config_idx_to_dir[key] = subdir
    # Select the indices of files to load.
    config_idxs = config_idx_to_dir.keys()
    if selected_idxs is None:
        # All indices.
        selected_keys = sorted(config_idxs)
    else:
        idxs_l = set(selected_idxs)
        hdbg.dassert_is_subset(idxs_l, set(config_idxs))
        selected_keys = [key for key in sorted(config_idxs) if key in idxs_l]
    # Subset the experiment subdirs based on the selected keys.
    experiment_subdirs = {key: config_idx_to_dir[key] for key in selected_keys}
    hdbg.dassert_lte(
        1,
        len(experiment_subdirs),
        "Can't find subdirs in '%s'",
        experiment_subdirs,
    )
    return src_dir, experiment_subdirs


def _load_experiment_artifact(
    file_name: str,
    load_rb_kwargs: Optional[Dict[str, Any]] = None,
) -> Any:
    """
    Load an experiment artifact whose format is encoded in name and extension.

    The content of a `result` dir looks like:
        config.pkl
        config.txt
        result_bundle_v1.0.pkl
        result_bundle_v2.0.pkl
        result_bundle_v2.0.pkl
        run_experiment.0.log

    - `result_bundle.v2_0.*`: a `ResultBundle` split between a pickle and Parquet
    - `result_bundle.v1_0.pkl`: a pickle file containing an entire `ResultBundle`
    - `config.pkl`: a pickle file containing a `Config`

    :param load_rb_kwargs: parameters passed to `ResultBundle.from_pickle`
    """
    base_name = os.path.basename(file_name)
    if base_name == "result_bundle.v2_0.pkl":
        # Load a `ResultBundle` stored in `rb` format.
        res = dtf.ResultBundle.from_pickle(
            file_name, use_pq=True, **load_rb_kwargs
        )
    elif base_name == "result_bundle.v1_0.pkl":
        # Load `ResultBundle` stored as a single pickle.
        res = dtf.ResultBundle.from_pickle(
            file_name, use_pq=False, **load_rb_kwargs
        )
    elif base_name == "config.pkl":
        # Load a `Config` stored as a pickle file.
        res = hpickle.from_pickle(file_name, log_level=logging.DEBUG)
        # TODO(gp): We should convert it to a `Config`.
    elif base_name.endswith(".json"):
        # Load JSON files.
        # TODO(gp): Use hpickle.to_json
        with open(file_name, "r") as file:
            res = json.load(file)
    elif base_name.endswith(".txt"):
        # Load txt files.
        res = hio.from_file(file_name)
    else:
        raise ValueError(f"Invalid file_name='{file_name}'")
    return res


def yield_experiment_artifacts(
    src_dir: str,
    file_name: str,
    load_rb_kwargs: Dict[str, Any],
    selected_idxs: Optional[Iterable[int]] = None,
    aws_profile: Optional[str] = None,
) -> Iterable[Tuple[str, Any]]:
    """
    Create an iterator returning the key of the experiment and an artifact.

    Same inputs as `load_experiment_artifacts()`.
    """
    _LOG.info("# Load artifacts '%s' from '%s'", file_name, src_dir)
    # Get the experiment subdirs.
    src_dir, experiment_subdirs = _get_experiment_subdirs(
        src_dir, selected_idxs, aws_profile=aws_profile
    )
    # Iterate over experiment directories.
    for key, subdir in tqdm(experiment_subdirs.items(), desc="Loading artifacts"):
        # Build the name of the file.
        hdbg.dassert_dir_exists(subdir)
        file_name_tmp = os.path.join(subdir, file_name)
        _LOG.debug("Loading '%s'", file_name_tmp)
        if not os.path.exists(file_name_tmp):
            _LOG.warning("Can't find '%s': skipping", file_name_tmp)
            continue
        obj = _load_experiment_artifact(file_name_tmp, load_rb_kwargs)
        yield key, obj


def _yield_rolling_experiment_out_of_sample_df(
    src_dir: str,
    file_name_prefix: str,
    load_rb_kwargs: Dict[str, Any],
    selected_idxs: Optional[Iterable[int]] = None,
    aws_profile: Optional[str] = None,
) -> Iterable[Tuple[str, pd.DataFrame]]:
    """
    Return in experiment dirs under `src_dir` matching `file_name_prefix*`.

    This function stitches together out-of-sample predictions from consecutive runs
    to form a single out-of-sample dataframe.

    Same inputs as `load_experiment_artifacts()`.
    """
    # TODO(gp): Not sure what are the acceptable prefixes.
    hdbg.dassert_in(
        file_name_prefix, ("result_bundle.v1_0.pkl", "result_bundle.v2_0.pkl")
    )
    # TODO(Paul): Generalize to loading fit `ResultBundle`s.
    _LOG.info("# Load artifacts '%s' from '%s'", file_name_prefix, src_dir)
    # Get the experiment subdirs.
    src_dir, experiment_subdirs = _get_experiment_subdirs(
        src_dir, selected_idxs, aws_profile=aws_profile
    )
    # Iterate over experiment directories.
    for key, subdir in tqdm(experiment_subdirs.items(), desc="Loading artifacts"):
        hdbg.dassert_dir_exists(subdir)
        # TODO(Paul): Sort these explicitly. Currently we rely on an implicit
        #  order.
        files = glob.glob(os.path.join(subdir, file_name_prefix) + "*")
        dfs = []
        # Iterate over OOS chunks.
        for file_name_tmp in files:
            _LOG.debug("Loading '%s'", file_name_tmp)
            if not os.path.exists(file_name_tmp):
                _LOG.warning("Can't find '%s': skipping", file_name_tmp)
                continue
            hdbg.dassert(os.path.basename(file_name_tmp))
            rb = _load_experiment_artifact(file_name_tmp, load_rb_kwargs)
            dfs.append(rb["result_df"])
        if dfs:
            df = pd.concat(dfs, axis=0)
            hdbg.dassert_strictly_increasing_index(df)
            df = csipro.resample(df, rule=dfs[0].index.freq).sum(min_count=1)
            yield key, df


def load_experiment_artifacts(
    src_dir: str,
    file_name: str,
    experiment_type: str,
    load_rb_kwargs: Optional[Dict[str, Any]] = None,
    selected_idxs: Optional[Iterable[int]] = None,
    aws_profile: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Load the results of an experiment.

    The function returns the contents of the files, indexed by the key extracted
    from the subdirectory index name.

    This function assumes subdirectories under `src_dir` have the following
    structure:
        ```
        {src_dir}/result_{idx}/{file_name}
        ```
    where `idx` denotes an integer encoded in the subdirectory name, representing
    the key of the experiment.

    :param src_dir: directory containing subdirectories of experiment results
        It is the directory that was specified as `--dst_dir` in `run_experiment.py`
        and `run_notebook.py`
    :param file_name: the file name within each run results subdirectory to load
        E.g., `result_bundle.v1_0.pkl` or `result_bundle.v2_0.pkl`
    :param load_rb_kwargs: parameters for loading a `ResultBundle` (see
    :param selected_idxs: specific experiment indices to load. `None` (default)
        loads all available indices
    """
    _LOG.info(
        "Before load_experiment_artifacts: memory_usage=%s",
        hdbg.get_memory_usage_as_str(None),
    )
    if experiment_type == "ins_oos":
        iterator = yield_experiment_artifacts
    elif experiment_type == "rolling_oos":
        iterator = _yield_rolling_experiment_out_of_sample_df
    else:
        raise ValueError("Invalid experiment_type='%s'", experiment_type)
    iter = iterator(
        src_dir,
        file_name,
        load_rb_kwargs=load_rb_kwargs,
        selected_idxs=selected_idxs,
        aws_profile=aws_profile,
    )
    # TODO(gp): We might want also to compare to the original experiments Configs.
    artifacts = collections.OrderedDict()
    for key, artifact in iter:
        _LOG.info(
            "load_experiment_artifacts: memory_usage=%s",
            hdbg.get_memory_usage_as_str(None),
        )
        artifacts[key] = artifact
    hdbg.dassert(artifacts, "No data read from '%s'", src_dir)
    _LOG.info(
        "After load_experiment_artifacts: memory_usage=%s",
        hdbg.get_memory_usage_as_str(None),
    )
    return artifacts
