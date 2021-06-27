"""
Import as:

import helpers.joblib_helpers as hjoblib
"""

import logging
import pprint
import functools
from typing import Any, Callable, Dict, List, Optional, Tuple

import joblib
from tqdm.autonotebook import tqdm

import helpers.datetime_
import helpers.io_ as hio
import helpers.timer as htimer
import helpers.printing as hprint

_LOG = logging.getLogger(__name__)


# #############################################################################

# A task is composed by the parameters (e.g., *args and **kwargs) to call the function.
TASK = Tuple[Tuple[Any], Dict[str, Any]]

WORKLOAD = Tuple[Callable, TASK]


def _file_logging_decorator(func: Callable) -> Callable:
    """
    Decorator to handle execution logging of the function and the abort_on_error behavior.
    """

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        # Extract parameters for the wrapper.
        task_idx = kwargs.pop("pe_task_idx")
        task_len = kwargs.pop("pe_task_len")
        abort_on_error = kwargs.pop("pe_abort_on_error")
        log_file = kwargs.pop("pe_log_file")
        # Save some information about the function execution.
        txt = []
        memento = htimer.dtimer_start(logging.DEBUG, "Execute %s" % func.__name__)
        txt.append("# task=%s/%s" % (task_idx + 1, task_len))
        txt.append("func=%s" % func.__name__)
        txt.append("args=\n%s" % hprint.indent(str(args)))
        txt.append("kwargs=\n%s" % hprint.indent(str(kwargs)))
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
        txt.append("elapsed_time=%s" % elapsed_time)
        start_ts = helpers.datetime_.get_timestamp()
        txt.append("start_ts=%s" % start_ts)
        end_ts = helpers.datetime_.get_timestamp()
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
    kwargs: Dict, task_idx: int, task_len: int, incremental: bool, abort_on_error: bool,
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


def parallel_execute(
    func: Callable,
    tasks: List[TASK],
    dry_run: bool,
    num_threads: str,
    incremental: bool,
    abort_on_error: bool,
    log_file: str,
) -> Optional[List[Any]]:
    _LOG.info(hprint.to_str("dry_run num_threads incremental abort_on_error"))
    _LOG.info("Saving log info in '%s'", log_file)
    if dry_run:
        for i, task in tqdm(enumerate(tasks)):
            print("\n" + hprint.frame("Task %s / %s" % (i + 1, len(tasks))))
            print("task=%s" % pprint.pformat(task))
        _LOG.warning("Exiting without executing, as per user request")
        return None
    # Apply the wrapper to handle `abort_on_error`.
    wrapped_func = _file_logging_decorator(func)
    # Run.
    task_len = len(tasks)
    if num_threads == "serial":
        res = []
        for i, task in tqdm(enumerate(tasks)):
            _LOG.debug("\n%s", hprint.frame("Task %s/%s" % (i + 1, task_len)))
            _LOG.debug("task=%s", pprint.pformat(task))
            # Execute.
            res_tmp = wrapped_func(
                *task[0], **_decorate_kwargs(task[1], i, task_len, incremental, abort_on_error, log_file)
            )
            res.append(res_tmp)
    else:
        num_threads = int(num_threads)  # type: ignore[assignment]
        # -1 is interpreted by joblib like for all cores.
        _LOG.info("Using %d threads", num_threads)
        res = joblib.Parallel(n_jobs=num_threads, verbose=50)(
            joblib.delayed(wrapped_func)(
                *task[0], **_decorate_kwargs(task[1], i, task_len, incremental, abort_on_error, log_file)
            )
            for task in tasks
        )
    _LOG.info("Saved log info in '%s'", log_file)
    return res
