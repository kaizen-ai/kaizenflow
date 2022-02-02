#!/usr/bin/env python
"""
Convert a directory storing Parquet files organized by dates into a Parquet
dataset organized by assets.

A Parquet file organized by dates looks like:
```
src_dir/
    date1/
        data.parquet
    date2/
        data.parquet
```

A Parquet file organized by assets looks like:
```
dst_dir/
    year1/
        month1/
            day1/
                asset1/
                    data.parquet
                asset2/
                    data.parquet
...
    year2/
        month2/
            day2/
                asset1/
                    data.parquet
                asset2/
                    data.parquet
```

# Example:
> transform_pq_by_date_to_by_asset.py \
    --src_dir im_v2/common/data/transform/test_data_by_date \
    --dst_dir im_v2/common/data/transform/test_data_by_asset \
    --num_threads 2

# To process Parquet data for LimeTask317:
> transform_pq_by_date_to_by_asset.py \
    --src_dir $DST_DIR \
    --dst_dir ${DST_DIR}_out \
    --no_incremental --force \
    --num_threads serial \
    --transform_func_name _LimeTask317 \
    -v DEBUG

Import as:

import im_v2.common.data.transform.transform_pq_by_date_to_by_asset as imvcdtcpbdtba
"""

import argparse
import logging
import os
from typing import Any, Callable, Dict, List, Tuple

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hintrospection as hintros
import helpers.hio as hio
import helpers.hjoblib as hjoblib
import helpers.hparser as hparser
import helpers.hprint as hprint

_LOG = logging.getLogger(__name__)


# Since different Parquet files require different transformations, we package all
# the specifics in custom functions to prepare the tasks and do perform the tasks
# See example for LimeTask317.

# #############################################################################
# Generic processing of files.
# #############################################################################


def _prepare_tasks(src_file_names: List[str], args: Any) -> List[hjoblib.Task]:
    # Build the src -> dst file mapping.
    src_dst_file_name_map = []
    for src_file_name in src_file_names:
        # ./tmp.s3/20220111/data.parquet
        # ->
        # ./tmp.s3_out/data.parquet
        dst_file_name = args.dst_dir
        _LOG.debug("%s -> %s", src_file_name, dst_file_name)
        src_dst_file_name_map.append((src_file_name, dst_file_name))
    # Remove the tasks already processed, if in incremental mode.
    incremental = not args.no_incremental
    if incremental:
        src_dst_file_name_map = hjoblib.apply_incremental_mode(
            src_dst_file_name_map
        )
    # Prepare the tasks from the src -> dst mapping.
    num_threads = hjoblib.get_num_executing_threads(args.num_threads)
    _LOG.info(
        "Number of executing threads=%s (%s)", num_threads, args.num_threads
    )
    chunked_srd_dst_file_name_map = hjoblib.split_list_in_tasks(
        src_dst_file_name_map,
        num_threads,
        keep_order=not args.no_keep_order,
        num_elems_per_task=args.num_func_per_task,
    )
    _LOG.info("Prepared %s tasks", len(chunked_srd_dst_file_name_map))
    tasks = []
    for src_dst_file_names in chunked_srd_dst_file_name_map:
        _LOG.debug(hprint.to_str("src_dst_file_names"))
        # The interface for the function called is:
        # def _process_chunk(src_dst_file_names: List[Tuple[str, str]])
        task: hjoblib.Task = (
            # args.
            (src_dst_file_names,),
            # kwargs.
            {},
        )
        tasks.append(task)
    return tasks


# #############################################################################


# TODO(gp): Move to hparquet.py
def _get_parquet_filenames(src_dir: str) -> List[str]:
    """
    Generate a list of all the Parquet files in a given dir.
    """
    hdbg.dassert_dir_exists(src_dir)
    # Find all the files with extension `.parquet` or `.pq`.
    src_pq_files = hio.find_files(src_dir, "*.parquet")
    if not src_pq_files:
        src_pq_files = hio.find_files(src_dir, "*.pq")
    hdbg.dassert_lte(1, len(src_pq_files))
    return src_pq_files


def _run(args: argparse.Namespace) -> None:
    incremental = not args.no_incremental
    # Prepare the destination dir.
    hparser.create_incremental_dir(args.dst_dir, args)
    # Get the input files to process.
    src_file_names = _get_parquet_filenames(args.src_dir)
    _LOG.info("Found %s Parquet files in '%s'", len(src_file_names), args.src_dir)
    # Prepare the tasks.
    func = hintros.get_function_from_string(args.prepare_tasks_func_name)
    hdbg.dassert_isinstance(func, Callable)
    tasks = func(src_file_names, args)
    # Prepare the workload.
    func = hintros.get_function_from_string(args.execute_task_func_name)
    hdbg.dassert_isinstance(func, Callable)
    func_name = func.__name__
    workload = (func, func_name, tasks)
    hjoblib.validate_workload(workload)
    # Prepare the log file.
    timestamp = hdateti.get_current_timestamp_as_string("ET")
    # TODO(Nikola): Change directory.
    log_dir = os.getcwd()
    log_file = os.path.join(log_dir, f"log.{timestamp}.txt")
    _LOG.info("log_file='%s'", log_file)
    # Execute the workload using command lines.
    abort_on_error = not args.skip_on_error
    hjoblib.parallel_execute(
        workload,
        args.dry_run,
        args.num_threads,
        incremental,
        abort_on_error,
        args.num_attempts,
        log_file,
    )


# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--src_dir",
        action="store",
        type=str,
        required=True,
        help="Source directory where original Parquet files are stored",
    )
    parser.add_argument(
        "--dst_dir",
        action="store",
        type=str,
        required=True,
        help="Destination directory where transformed Parquet files will be stored",
    )
    hparser.add_parallel_processing_arg(parser)
    hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    _run(args)


if __name__ == "__main__":
    _main(_parse())
