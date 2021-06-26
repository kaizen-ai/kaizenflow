"""
Import as:

import helpers.joblib_helpers as hjoblib
"""

import logging
import pprint
from typing import Any, Callable, Dict, List, Optional, Tuple

import joblib
from tqdm.autonotebook import tqdm

import helpers.printing as hprint

# import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)


# #############################################################################

# A task is composed by the parameters (e.g., *args and **kwargs) to call the function.
TASK = Tuple[Tuple[Any], Dict[str, Any]]

WORKLOAD = Tuple[Callable, TASK]


def _decorate_kwargs(
    kwargs: Dict, incremental: bool, abort_on_error: bool
) -> Dict:
    kwargs = kwargs.copy()
    kwargs.update({"incremental": incremental, "abort_on_error": abort_on_error})
    return kwargs


def parallel_execute(
    func: Callable,
    tasks: List[TASK],
    dry_run: bool,
    num_threads: str,
    incremental: bool,
    abort_on_error: bool,
) -> Optional[List[Any]]:
    _LOG.info(hprint.to_str("dry_run num_threads incremental abort_on_error"))
    if dry_run:
        for i, task in tqdm(enumerate(tasks)):
            print("\n" + hprint.frame("Task %s / %s" % (i + 1, len(tasks))))
            print("task=%s" % pprint.pformat(task))
        _LOG.warning("Exiting without executing, as per user request")
        return None
    # Run.
    if num_threads == "serial":
        res = []
        for i, task in tqdm(enumerate(tasks)):
            _LOG.debug("\n%s", hprint.frame("Task %s / %s" % (i + 1, len(tasks))))
            _LOG.debug("task=%s", pprint.pformat(task))
            #
            res_tmp = func(
                *task[0], **_decorate_kwargs(task[1], incremental, abort_on_error)
            )
            res.append(res_tmp)
    else:
        num_threads = int(num_threads)  # type: ignore[assignment]
        # -1 is interpreted by joblib like for all cores.
        _LOG.info("Using %d threads", num_threads)
        res = joblib.Parallel(n_jobs=num_threads, verbose=50)(
            joblib.delayed(func)(
                *task[0], **_decorate_kwargs(task[1], incremental, abort_on_error)
            )
            for task in tasks
        )
    return res
