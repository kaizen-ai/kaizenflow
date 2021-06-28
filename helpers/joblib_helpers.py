"""
Import as:

import helpers.joblib_helpers as hjoblib
"""

import logging
import pprint
import random
import functools
from typing import Any, Callable, Dict, List, Optional, Tuple

import joblib
from tqdm.autonotebook import tqdm

import helpers.dbg as dbg
import helpers.datetime_ as hdatetime
import helpers.io_ as hio
import helpers.timer as htimer
import helpers.printing as hprint

_LOG = logging.getLogger(__name__)

# #############################################################################

# A task is composed by the parameters (e.g., *args and **kwargs) to call the
# function.
TASK = Tuple[Tuple[Any], Dict[str, Any]]

WORKLOAD = Tuple[Callable, str, TASK]


def _file_logging_decorator(func: Callable) -> Callable:
    """
    Decorator to handle execution logging of the function and the abort_on_error behavior.
    """

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        # Extract parameters for the wrapper.
        task_idx = kwargs.pop("pe_task_idx")
        task_len = kwargs.pop("pe_task_len")
        func_name = kwargs.pop("pe_func_name")
        abort_on_error = kwargs.pop("pe_abort_on_error")
        log_file = kwargs.pop("pe_log_file")
        # Save some information about the function execution.
        txt = []
        memento = htimer.dtimer_start(logging.DEBUG, "Execute %s" % func.__name__)
        txt.append("# task=%s/%s" % (task_idx + 1, task_len))
        txt.append("func=%s" % func.__name__)
        txt.append("func_name=%s" % func_name)
        txt.append("args=\n%s" % hprint.indent(str(args)))
        txt.append("kwargs=\n%s" % hprint.indent(str(kwargs)))
        # `start_ts` needs to be before running the function.
        start_ts = hdatetime.get_timestamp()
        try:
            res = func(*args, **kwargs)
            error = False
        except RuntimeError as e:
            txt.append("exception=\n%s" % str(e))
            res = None
            error = True
            _LOG.error("Execution failed:\n%s", txt)
        _LOG.debug("error=%s", error)
        msg, elapsed_time = htimer.dtimer_stop(memento)
        _ = msg
        txt.append("elapsed_time_in_secs=%s" % elapsed_time)
        txt.append("start_ts=%s" % start_ts)
        end_ts = hdatetime.get_timestamp()
        txt.append("end_ts=%s" % end_ts)
        txt.append("error=%s" % error)
        # Update log file.
        txt = "\n" + hprint.frame(start_ts) + "\n" + "\n".join(txt)
        _LOG.debug("txt=\n%s" % hprint.indent(txt))
        hio.to_file(log_file, txt, mode="a")
        if error:
            _LOG.error(msg)
            # The execution wasn't successful.
            if abort_on_error:
                _LOG.error("Aborting since abort_on_error=%s", abort_on_error)
                raise e
            else:
                _LOG.error("Continuing execution since abort_on_error=%s",
                           abort_on_error)
        else:
            # The execution was successful.
            pass
        return res

    # TODO(gp): For some reason `@functools.wraps` reports an error like:
    #   File "/app/amp/helpers/joblib_helpers.py", line 136, in parallel_execute
    #     res_tmp = wrapped_func(
    #   TypeError: update_wrapper() got multiple values for argument 'wrapped'
    wrapper.__name__ = func.__name__
    wrapper.__doc__ = func.__doc__
    return wrapper


def _decorate_kwargs(
        kwargs: Dict, task_idx: int, task_len: int, func_name: str,
        incremental: bool, abort_on_error: bool,
        log_file: str,
) -> Dict:
    """
    Pass the book-keeping parameters from `parallel_execute()` to the wrapped func.
    """
    kwargs = kwargs.copy()
    # We prepend `pe_` (as in parallel_execution) to avoid collisions.
    kwargs.update({
        "pe_task_idx": task_idx,
        "pe_task_len": task_len,
        "pe_func_name": func_name,
        # This is a parameter for the function, so we don't prepend `pe_`.
        "incremental": incremental,
        "pe_abort_on_error": abort_on_error,
        "pe_log_file": log_file
    })
    return kwargs


# def abort_on_error_handler(func: Callable) -> Callable:
#     """
#     Decorator to handle execution logging of the function and the abort_on_error behavior.
#     """
#
#     def wrapper(*args: Any, **kwargs: Any) -> Any:
#         abort_on_error = kwargs.pop("abort_on_error")
#         try:
#             ret = func(*args, **kwargs)
#         except RuntimeError as e:
#
#
#     wrapper.__name__ = func.__name__
#     wrapper.__doc__ = func.__doc__
#     return wrapper


def randomize_workload(
    workload: WORKLOAD, seed: Optional[int] = None
) -> WORKLOAD:
    tasks = workload[1]
    seed = seed or 42
    random.seed(seed)
    random.shuffle(tasks)
    return (workload[0], tasks)


def tasks_to_string(func: Callable, func_name: str, tasks: List[TASK]) ->str:
    dbg.dassert_isinstance(func, Callable)
    dbg.dassert_isinstance(func_name, str)
    dbg.dassert_container_type(tasks, List, tuple)
    txt = []
    txt.append("func=%s" % func.__name__)
    txt.append("func_name=%s" % func_name)
    for i, task in tqdm(enumerate(tasks)):
        txt.append("\n" + hprint.frame("Task %s / %s" % (i + 1, len(tasks))))
        txt.append("task=%s" % pprint.pformat(task))
    txt = "\n".join(txt)
    return txt


def parallel_execute(
        func: Callable,
        func_name: str,
        tasks: List[TASK],
        dry_run: bool,
        num_threads: str,
        incremental: bool,
        abort_on_error: bool,
        log_file: str,
) -> Optional[List[Any]]:
    """
    Run a workload in parallel.

    :param func: function to call in each thread
    :param func_name: string invocation of the function for logging purposes
        (e.g., vltbut.get_bar_data_for_date_interval)
    :param tasks: list of args and kwargs to use when calling `func`
    :param dry_run: if True, print the workload and exit without executing it
    :param num_threads: joblib parameter to control how many threads to use
    :param incremental: parameter passed to the function to execute, to control if
        we want to re-execute workload already executed or not
    :param abort_on_error: if True, stop executing the workload if one task asserts,
        or continue
    :param log_file: file used to log information about the execution

    :return: list with the results from executing `func`
    """
    dbg.dassert_isinstance(func, Callable)
    dbg.dassert_isinstance(func_name, str)
    dbg.dassert_container_type(tasks, List, tuple)
    _LOG.info(hprint.to_str("dry_run num_threads incremental abort_on_error"))
    _LOG.info("Saving log info in '%s'", log_file)
    _LOG.info("Number of tasks=%s", len(tasks))
    if dry_run:
        print(tasks_to_string(func, func_name, tasks))
        _LOG.warning("Exiting without executing, as per user request")
        return None
    # Apply the wrapper to handle `abort_on_error`.
    wrapped_func = _file_logging_decorator(func)
    # Run.
    task_len = len(tasks)
    if num_threads == "serial":
        res = []
        for i, task in tqdm(enumerate(tasks), total=len(tasks),
                            desc="Serial tasks"):
            _LOG.debug("\n%s", hprint.frame("Task %s / %s" % (i + 1, len(tasks))))
            _LOG.debug("task=%s", pprint.pformat(task))
            # Execute.
            res_tmp = wrapped_func(
                *task[0],
                **_decorate_kwargs(task[1], i, task_len, func_name, incremental,
                                   abort_on_error, log_file)
            )
            res.append(res_tmp)
    else:
        num_threads = int(num_threads)  # type: ignore[assignment]
        # -1 is interpreted by joblib like for all cores.
        _LOG.info("Using %d threads", num_threads)
        # From https://stackoverflow.com/questions/24983493
        tqdm_ = tqdm(enumerate(tasks), total=len(tasks), desc="Parallel tasks")
        res = joblib.Parallel(n_jobs=num_threads, verbose=100)(
            joblib.delayed(wrapped_func)(
                *task[0],
                **_decorate_kwargs(task[1], i, task_len, func_name, incremental,
                                   abort_on_error, log_file)
            )
            for i, task in tqdm_
        )
    _LOG.info("Saved log info in '%s'", log_file)
    return res
