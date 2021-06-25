import logging
import os
import re
import random
import time
from typing import Any, Callable, Dict, List, Tuple

import helpers.dbg as dbg
import helpers.system_interaction as hsyste
import helpers.unit_test as hut
import helpers.joblib_helpers as hjoblib

_LOG = logging.getLogger(__name__)

# #############################################################################


def _func(val1: int, val2: str,
          #
          incremental: bool, abort_on_error: bool,
          #
          **kwargs: Any,
          ) -> str:
    res = f"val1={val1} val2={val2} kwargs={kwargs} incremental={incremental} abort_on_error={abort_on_error}"
    _LOG.debug("res=%s", res)
    time.sleep(0.1)
    if val1 == -1:
        if abort_on_error:
            raise ValueError(f"Error: {res}")
    return res


def _randomize_workload(workload: hjoblib.WORKLOAD, seed: int = 1) -> hjoblib.WORKLOAD:
    tasks = workload[1]
    random.seed(seed)
    random.shuffle(tasks)
    return (workload[0], tasks)


def _get_workload1() -> hjoblib.WORKLOAD:
    """
    Return a workload for `_func()` that succeds.
    """
    tasks = []
    for i in range(5):
        # val1, val2
        task = ((i, 2 * i), {f"hello{i}": f"world{2 * i}", "good": "bye"})
        tasks.append(task)
    workload = (_func, tasks)
    # Randomize workload.
    workload = _randomize_workload(workload)
    return workload


def _get_workload2() -> Tuple[Callable, List[hjoblib.TASK]]:
    """
    Return a workload for `_func()` that fails.
    """
    workload = _get_workload1()
    #
    task = ((-1, 7), {"hello2": "world2", "good2": "bye2"})
    workload[1].append(task)
    # Randomize workload.
    workload = _randomize_workload(workload)
    return workload


class Test_parallel_execute1(hut.TestCase):

    def test_dry_run1(self) -> None:
        func, tasks = _get_workload1()
        dry_run = True
        num_threads = "serial"
        incremental = True
        abort_on_error = True
        res = hjoblib.parallel_execute(func, tasks, dry_run, num_threads, incremental, abort_on_error)
        _LOG.debug("res=%s", str(res))
        self.assertIs(res, None)

    def test_serial1(self) -> None:
        """
        Execute serially a workload that succeeds.
        """
        func, tasks = _get_workload1()
        dry_run = False
        num_threads = "serial"
        incremental = True
        abort_on_error = True
        #
        self._helper_success(func, tasks, dry_run, num_threads, incremental, abort_on_error)

    def test_parallel1(self) -> None:
        """
        Execute with 1 thread a workload that succeeds.
        """
        func, tasks = _get_workload1()
        dry_run = False
        num_threads = 1
        incremental = True
        abort_on_error = True
        #
        self._helper_success(func, tasks, dry_run, num_threads, incremental, abort_on_error)

    def test_parallel2(self) -> None:
        """
        Execute with 3 threads a workload that succeeds.
        """
        func, tasks = _get_workload1()
        dry_run = False
        num_threads = 3
        incremental = True
        abort_on_error = True
        #
        self._helper_success(func, tasks, dry_run, num_threads, incremental, abort_on_error)

    def test_parallel3(self) -> None:
        """
        Execute with 3 threads a workload that asserts, but using not aborting.
        """
        func, tasks = _get_workload1()
        dry_run = False
        num_threads = 3
        incremental = True
        abort_on_error = False
        #
        res = hjoblib.parallel_execute(func, tasks, dry_run, num_threads, incremental, abort_on_error)
        _LOG.debug("res=%s", str(res))
        act = str(sorted(res))
        # pylint: disable=line-too-long
        exp = [
            "val1=0 val2=0 kwargs={'hello0': 'world0', 'good': 'bye'} incremental=True abort_on_error=False",
             "val1=1 val2=2 kwargs={'hello1': 'world2', 'good': 'bye'} incremental=True abort_on_error=False",
             "val1=2 val2=4 kwargs={'hello2': 'world4', 'good': 'bye'} incremental=True abort_on_error=False",
             "val1=3 val2=6 kwargs={'hello3': 'world6', 'good': 'bye'} incremental=True abort_on_error=False",
             "val1=4 val2=8 kwargs={'hello4': 'world8', 'good': 'bye'} incremental=True abort_on_error=False"]
        # pylint: enable=line-too-long
        exp = str(exp)
        self.assert_equal(act, exp)

    def test_serial_fail1(self) -> None:
        """
        Execute serially a workload that asserts.
        """
        func, tasks = _get_workload2()
        dry_run = False
        num_threads = "serial"
        incremental = True
        abort_on_error = True
        #
        self._helper_fail(func, tasks, dry_run, num_threads, incremental, abort_on_error)

    def test_parallel_fail1(self) -> None:
        """
        Execute with 1 thread a workload that asserts.
        """
        func, tasks = _get_workload2()
        dry_run = False
        num_threads = 1
        incremental = True
        abort_on_error = True
        #
        self._helper_fail(func, tasks, dry_run, num_threads, incremental, abort_on_error)

    def test_parallel_fail2(self) -> None:
        """
        Execute with 3 threads a workload that asserts.
        """
        func, tasks = _get_workload2()
        dry_run = False
        num_threads = 3
        incremental = True
        abort_on_error = True
        #
        self._helper_fail(func, tasks, dry_run, num_threads, incremental, abort_on_error)

    def _helper_success(self, *args: Any, **kwargs: Any) -> None:
        res = hjoblib.parallel_execute(*args, **kwargs)
        _LOG.debug("res=%s", str(res))
        act = str(sorted(res))
        # pylint: disable=line-too-long
        exp = ["val1=0 val2=0 kwargs={'hello0': 'world0', 'good': 'bye'} incremental=True abort_on_error=True",
               "val1=1 val2=2 kwargs={'hello1': 'world2', 'good': 'bye'} incremental=True abort_on_error=True",
               "val1=2 val2=4 kwargs={'hello2': 'world4', 'good': 'bye'} incremental=True abort_on_error=True",
               "val1=3 val2=6 kwargs={'hello3': 'world6', 'good': 'bye'} incremental=True abort_on_error=True",
               "val1=4 val2=8 kwargs={'hello4': 'world8', 'good': 'bye'} incremental=True abort_on_error=True"]
        # pylint: enable=line-too-long
        exp = str(exp)
        self.assert_equal(act, exp)

    def _helper_fail(self, *args: Any, **kwargs: Any) -> None:
        with self.assertRaises(ValueError) as cm:
            res = hjoblib.parallel_execute(*args, **kwargs)
            _LOG.debug("res=%s", str(res))
        act = str(cm.exception)
        # pylint: disable=line-too-long
        exp = r"""Error: val1=-1 val2=7 kwargs={'hello2': 'world2', 'good2': 'bye2'} incremental=True abort_on_error=True"""
        # pylint: enable=line-too-long
        self.assert_equal(act, exp)
