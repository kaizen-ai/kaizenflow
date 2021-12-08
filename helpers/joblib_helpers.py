"""
Import as:

import helpers.joblib_helpers as hjoblib
"""

import concurrent.futures
import logging
import os
import pprint
import random
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import joblib
from joblib._store_backends import StoreBackendBase, StoreBackendMixin
from tqdm.autonotebook import tqdm

import helpers.datetime_ as hdateti
import helpers.dbg as hdbg
import helpers.htqdm as htqdm
import helpers.io_ as hio
import helpers.printing as hprint
import helpers.timer as htimer

_LOG = logging.getLogger(__name__)

# #############################################################################
# Task
# #############################################################################

# A `Task` contains the parameters to pass to the function, in the forms of a
# tuple of `*args` and `**kwargs`.
Task = Tuple[Tuple[Any], Dict[str, Any]]


def validate_task(task: Task) -> bool:
    """
    Assert if the task is malformed, otherwise return True.
    """
    hdbg.dassert_isinstance(task, tuple)
    hdbg.dassert_eq(len(task), 2)
    args, kwargs = task
    _LOG.debug("task[0]=%s", str(args))
    hdbg.dassert_isinstance(args, tuple)
    _LOG.debug("task[1]=%s", str(kwargs))
    hdbg.dassert_isinstance(kwargs, dict)
    return True


def task_to_string(task: Task) -> str:
    hdbg.dassert(validate_task(task))
    args, kwargs = task
    txt = []
    txt.append("args=%s" % pprint.pformat(args))
    txt.append("kwargs=%s" % pprint.pformat(kwargs))
    txt = "\n".join(txt)
    return txt


# #############################################################################
# Workload
# #############################################################################

# A `Workload` represents multiple executions of a function with different
# parameters.
Workload = Tuple[
    # `func`: the function representing the workload to execute
    Callable,
    # `func_name`: the mnemonic name of the function, which is used for debugging info
    # and for naming the directory storing the cache
    # - E.g., `vltbut.get_bar_data_for_date_interval`
    # - Note that the `func_name` can be different than the name of `func`
    #   - E.g., we can call
    #     `vltbut.get_bar_data_for_date_interval_for_interval` inside `func`,
    #     in order to create a cache for
    #     `vltbut.get_bar_data_for_date_interval`, so the cache name should be
    #     for `vltbut.get_bar_data_for_date_interval`
    str,
    # `tasks`: a list of (*args, **kwargs) to pass to `func`
    List[Task],
]


def validate_workload(workload: Workload) -> bool:
    """
    Assert if the workload is malformed, otherwise return True.
    """
    hdbg.dassert_isinstance(workload, tuple)
    hdbg.dassert_eq(len(workload), 3)
    # Parse workload.
    workload_func, func_name, tasks = workload
    # Check each component.
    hdbg.dassert_isinstance(workload_func, Callable)
    hdbg.dassert_isinstance(func_name, str)
    hdbg.dassert_container_type(tasks, List, tuple)
    hdbg.dassert(all(validate_task(task) for task in tasks))
    return True


def randomize_workload(
    workload: Workload, seed: Optional[int] = None
) -> Workload:
    validate_workload(workload)
    workload_func, func_name, tasks = workload
    seed = seed or 42
    random.seed(seed)
    random.shuffle(tasks)
    workload = (workload_func, func_name, tasks)
    validate_workload(workload)
    return workload


def workload_to_string(workload: Workload) -> str:
    validate_workload(workload)
    workload_func, func_name, tasks = workload
    txt = []
    txt.append("workload_func=%s" % workload_func.__name__)
    txt.append("func_name=%s" % func_name)
    for i, task in enumerate(tasks):
        txt.append("\n" + hprint.frame("Task %s / %s" % (i + 1, len(tasks))))
        txt.append(task_to_string(task))
    txt = "\n".join(txt)
    return txt


# #############################################################################
# Template for functions to execute in parallel.
# #############################################################################

# NOTE: the workload function:
# - asserts if there is an error, since the return value is a string with a summary
#   of the execution
# - doesn't have to be the function that we intend to cache


def _workload_function(*args: Any, **kwargs: Any) -> str:
    """
    Execute the function task.

    :raises: in case of error
    :return: string representing information about the cached function execution.
    """
    _ = args
    incremental = kwargs.pop("incremental")
    num_attempts = kwargs.pop("num_attempts")
    _ = incremental, num_attempts
    func_output: List[str] = []
    func_output = "\n".join(func_output)
    return func_output


def _get_workload(
    # args: argparse.Namespace
) -> Workload:
    """
    Prepare the workload using the parameters from command line.
    """
    # _ = args


# #############################################################################
# Layer passing information from `parallel_execute` to the function to execute
# in parallel.
# #############################################################################


def _parallel_execute_decorator(
    task_idx: int,
    task_len: int,
    incremental: bool,
    abort_on_error: bool,
    num_attempts: int,
    log_file: str,
    #
    workload_func: Callable,
    func_name: str,
    task: Task,
) -> Any:
    """
    :param abort_on_error: control whether to abort on `workload_func` function
        that is failing and asserting
        - If `workload_func` fails:
            - if `abort_on_error=True` the exception from `workload_func` is
              propagated and the return value is `None`
            - if `abort_on_error=False` the exception is not propagated, but the
              return value is the string representation of the exception

    :return: the return value of the workload function or the exception string
    """
    # Validate very carefully all the parameters.
    hdbg.dassert_lte(0, task_idx)
    hdbg.dassert_lt(task_idx, task_len)
    hdbg.dassert_isinstance(incremental, bool)
    hdbg.dassert_isinstance(abort_on_error, bool)
    hdbg.dassert_lte(1, num_attempts)
    hdbg.dassert_isinstance(log_file, str)
    hdbg.dassert_isinstance(workload_func, Callable)
    hdbg.dassert_isinstance(func_name, str)
    hdbg.dassert(validate_task(task))
    # Redirect the logging output of each task to a different file.
    # TODO(gp): This file should go in the `task_dst_dir`.
    # log_to_file = True
    log_to_file = False
    if log_to_file:
        dst_dir = os.path.dirname(os.path.abspath(log_file))
        print(dst_dir)
        hio.create_dir(dst_dir, incremental=True)
        file_name = os.path.join(
            dst_dir, f"{func_name}.{task_idx + 1}_{task_len}.log"
        )
        _LOG.warning("Logging to %s", file_name)
        file_handler = logging.FileHandler(file_name)
        root_logger = logging.getLogger()
        root_logger.addHandler(file_handler)

    # Save some information about the function execution.
    txt = []
    # `start_ts` needs to be before running the function.
    start_ts = hdateti.get_timestamp("naive_ET")
    tag = "%s/%s (%s)" % (task_idx + 1, task_len, start_ts)
    txt.append("\n" + hprint.frame(tag) + "\n")
    txt.append("tag=%s" % tag)
    txt.append("workload_func=%s" % workload_func.__name__)
    txt.append("func_name=%s" % func_name)
    txt.append(task_to_string(task))
    args, kwargs = task
    kwargs.update({"incremental": incremental, "num_attempts": num_attempts})
    with htimer.TimedScope(
        logging.DEBUG, "Execute '%s'" % workload_func.__name__
    ) as ts:
        try:
            res = workload_func(*args, **kwargs)
            error = False
        except Exception as e:  # pylint: disable=broad-except
            exception = e
            txt.append("exception='%s'" % str(e))
            res = None
            error = True
            _LOG.error("Execution failed")
    elapsed_time = ts.elapsed_time
    txt.append("func_res=\n%s" % hprint.indent(str(res)))
    txt.append("elapsed_time_in_secs=%s" % elapsed_time)
    txt.append("start_ts=%s" % start_ts)
    end_ts = hdateti.get_timestamp("naive_ET")
    txt.append("end_ts=%s" % end_ts)
    txt.append("error=%s" % error)
    # Update log file.
    txt = "\n".join(txt)
    _LOG.debug("txt=\n%s", hprint.indent(txt))
    hio.to_file(log_file, txt, mode="a")
    if error:
        # The execution wasn't successful.
        _LOG.error(txt)
        if abort_on_error:
            _LOG.error("Aborting since abort_on_error=%s", abort_on_error)
            raise exception  # noqa: F821
        _LOG.error("Continuing execution since abort_on_error=%s", abort_on_error)
        res = str(exception)
    else:
        # The execution was successful.
        pass
    return res


# TODO(gp): Pass a `task_dst_dir` to each task so it can write there.
#  This is a generalization of `experiment_result_dir` for `run_experiment` and
#  `run_notebook`.
def parallel_execute(
    workload: Workload,
    # Options for the `parallel_execute` framework.
    dry_run: bool,
    num_threads: Union[str, int],
    incremental: bool,
    abort_on_error: bool,
    num_attempts: int,
    log_file: str,
    *,
    backend: str = "loky",
) -> Optional[List[Any]]:
    """
    Run a workload in parallel.

    :param workload: the workload to execute

    :param dry_run: if True, print the workload and exit without executing it
    :param num_threads: joblib parameter to control how many threads to use
    :param incremental: parameter passed to the function to execute, to control if
        we want to re-execute workload already executed or not
    :param abort_on_error: if True, if one task asserts stop executing the workload
        and return the exception of the failing task
        - If False, the execution continues
    :param num_attempts: number of times to attempt running a function before
        declaring an error
    :param log_file: file used to log information about the execution
    :param backend: specify the backend type (e.g., joblib `loky` or
        `asyncio_process_executor`)

    :return: list with the results from executing `func` or the exception of the
        failing function
        - NOTE: if `abort_on_error=True` and one task fails, `joblib` doesn't return
          the output of the already executed tasks. In this case, the best we can do
          is to return the exception of the failing task
    """
    validate_workload(workload)
    workload_func, func_name, tasks = workload
    #
    _LOG.info(
        hprint.to_str(
            "dry_run num_threads incremental num_attempts abort_on_error"
        )
    )
    if dry_run:
        print(workload_to_string(workload))
        _LOG.warning("Exiting without executing, as per user request")
        return None
    _LOG.info("Saving log info in '%s'", log_file)
    _LOG.info("Number of tasks=%s", len(tasks))
    # Run.
    task_len = len(tasks)
    tqdm_out = htqdm.TqdmToLogger(_LOG, level=logging.INFO)
    tqdm_iter = tqdm(
        enumerate(tasks),
        total=task_len,
        file=tqdm_out,
        desc=f"num_threads={num_threads} backend={backend}",
    )
    if num_threads == "serial":
        res = []
        for task_idx, task in tqdm_iter:
            _LOG.debug(
                "\n%s", hprint.frame("Task %s / %s" % (task_idx + 1, task_len))
            )
            # Execute.
            res_tmp = _parallel_execute_decorator(
                task_idx,
                task_len,
                incremental,
                abort_on_error,
                num_attempts,
                log_file,
                #
                workload_func,
                func_name,
                task,
            )
            res.append(res_tmp)
    else:
        # -1 is interpreted by joblib like for all cores.
        num_threads = int(num_threads)
        _LOG.info("Using %d threads, backend='%s'", num_threads, backend)
        if backend in ("loky", "threading", "multiprocessing"):
            # from joblib.externals.loky import set_loky_pickler
            # set_loky_pickler('cloudpickle')
            # backend = "threading"
            # backend = "multiprocessing"
            res = joblib.Parallel(
                n_jobs=num_threads, backend=backend, verbose=200
            )(
                joblib.delayed(_parallel_execute_decorator)(
                    task_idx,
                    task_len,
                    incremental,
                    abort_on_error,
                    num_attempts,
                    log_file,
                    #
                    workload_func,
                    func_name,
                    task,
                )
                # We can't use `tqdm_iter` since this only shows the submission of
                # the jobs but not their completion.
                for task_idx, task in enumerate(tasks)
            )
        elif backend in ("asyncio_threading", "asyncio_multiprocessing"):
            if backend == "asyncio_threading":
                executor = concurrent.futures.ThreadPoolExecutor
            elif backend == "asyncio_multiprocessing":
                executor = concurrent.futures.ProcessPoolExecutor
            else:
                raise ValueError("Invalid backend='%s'" % backend)
            func = lambda args_: _parallel_execute_decorator(
                args_[0],
                task_len,
                incremental,
                abort_on_error,
                num_attempts,
                log_file,
                #
                workload_func,
                func_name,
                args_[1],
            )
            args = list(enumerate(tasks))
            use_progress_bar = True
            if not use_progress_bar:
                # Implementation without progress bar.
                with executor(max_workers=num_threads) as executor_:
                    res = list(executor_.map(func, args))
            else:
                # Implementation with progress bar.
                res = []
                with tqdm_iter as pbar:
                    with executor(max_workers=num_threads) as executor_:
                        futures = {
                            executor_.submit(func, arg): arg for arg in args
                        }
                        _LOG.debug("done submitting")
                        for future in concurrent.futures.as_completed(futures):
                            res_tmp = future.result()
                            res.append(res_tmp)
                            pbar.update(1)
        else:
            raise ValueError("Invalid backend='%s'" % backend)
    _LOG.info("Saved log info in '%s'", log_file)
    return res


# #############################################################################
# joblib storage backend for S3.
# #############################################################################

# Adapted from https://github.com/aabadie/joblib-s3


class _S3FSStoreBackend(StoreBackendBase, StoreBackendMixin):
    """
    A StoreBackend for S3 cloud storage file system.
    """

    def __init__(self) -> None:
        super().__init__()
        self._objs: List[Any] = []

    def clear_location(self, location: str) -> None:
        """
        Check if object exists in store.
        """
        if self.storage.exists(location):
            self._flush()
            self.storage.rm(location, recursive=True)

    def create_location(self, location: str) -> None:
        """
        Create object location on store.
        """
        self._mkdirp(location)

    def get_items(self) -> List[Any]:
        """
        Return the whole list of items available in cache.
        """
        _ = self
        return []

    def configure(
        self,
        location: str,
        backend_options: Dict[str, Any],
        verbose: int = 0,
    ) -> None:
        """
        Configure the store backend.
        """
        options = backend_options
        hdbg.dassert_in("s3fs", options)
        self.storage = options["s3fs"]
        hdbg.dassert_in("bucket", options)
        bucket = options["bucket"]
        # Ensure the given bucket exists.
        root_bucket = os.path.join("s3://", bucket)
        if not self.storage.exists(root_bucket):
            self.storage.mkdir(root_bucket)
        if location.startswith("/"):
            location.replace("/", "")
        self.location = os.path.join(root_bucket, location)
        if not self.storage.exists(self.location):
            self.storage.mkdir(self.location)
        # Computation results can be stored compressed for faster I/O.
        self.compress = backend_options["compress"]
        # Memory map mode is not supported.
        self.mmap_mode = None

    def _flush(self) -> None:
        _ = self
        # TODO(gp): No need to flush for now.
        # for fd in self._objs:
        #    fd.flush(force=True)

    def _open_item(self, fd: Any, mode: str) -> Any:
        self._objs.append(fd)
        return self.storage.open(fd, mode)

    def _item_exists(self, path: str) -> bool:
        self._flush()
        ret: bool = self.storage.exists(path)
        return ret

    def _move_item(self, src: str, dst: str) -> None:
        self.storage.mv(src, dst)

    def _mkdirp(self, directory: str) -> None:
        """
        Create recursively a directory on the S3 store.
        """
        # Remove root cachedir from input directory to create as it should
        # have already been created in the configure function.
        if directory.startswith(self.location):
            directory = directory.replace(self.location + "/", "")
        current_path = self.location
        for sub_dir in directory.split("/"):
            current_path = os.path.join(current_path, sub_dir)
            self.storage.mkdir(current_path)


_REGISTER_S3FS_STORE = False


def register_s3fs_store_backend() -> None:
    """
    Register the S3 store backend for joblib memory caching.
    """
    global _REGISTER_S3FS_STORE
    if not _REGISTER_S3FS_STORE:
        joblib.register_store_backend("s3", _S3FSStoreBackend)
        _REGISTER_S3FS_STORE = True
