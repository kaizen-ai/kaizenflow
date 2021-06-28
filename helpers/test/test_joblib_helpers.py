import logging
import os
import time
from typing import Any, Callable, List, Optional

import helpers.joblib_helpers as hjoblib
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)

# #############################################################################


# TODO(gp): -> function
def func(
    val1: int,
    val2: str,
    #
    incremental: bool,
    #
    **kwargs: Any,
) -> str:
    res = (
        f"val1={val1} val2={val2} kwargs={kwargs} incremental={incremental}"
    )
    _LOG.debug("res=%s", res)
    time.sleep(0.1)
    if val1 == -1:
        raise ValueError(f"Error: {res}")
    return res


def get_workload1(randomize: bool, seed: Optional[int]=None) -> hjoblib.WORKLOAD:
    """
    Return a workload for `func()` that succeeds.
    """
    tasks = []
    for i in range(5):
        # val1, val2
        task = ((i, 2 * i), {f"hello{i}": f"world{2 * i}", "good": "bye"})
        tasks.append(task)
    workload = (func, tasks)
    if randomize:
        # Randomize workload.
        workload: hjoblib.WORKLOAD = hjoblib.randomize_workload(workload, seed=seed)
    return workload


def get_workload2(randomize: bool, seed: Optional[int]=None) -> hjoblib.WORKLOAD:
    """
    Return a workload for `func()` that fails.
    """
    workload = get_workload1(randomize=True)
    #
    task = ((-1, 7), {"hello2": "world2", "good2": "bye2"})
    workload[1].append(task)
    if randomize:
        # Randomize workload.
        workload: hjoblib.WORKLOAD = hjoblib.randomize_workload(workload, seed=seed)
    return workload


class Test_parallel_execute1(hut.TestCase):
    def test_dry_run1(self) -> None:
        """
        Dry-run a workload.
        """
        func, tasks = get_workload1(randomize=True)
        func_name = "func"
        dry_run = True
        num_threads = "serial"
        incremental = True
        abort_on_error = True
        log_file = os.path.join(self.get_scratch_space(), "log.txt")
        res = hjoblib.parallel_execute(
            func, func_name, tasks, dry_run, num_threads, incremental, abort_on_error,
            log_file
        )
        _LOG.debug("res=%s", str(res))
        self.assertIs(res, None)

    # pylint: disable=line-too-long
    exp_workload1 = [
        "val1=0 val2=0 kwargs={'hello0': 'world0', 'good': 'bye'} incremental=True",
        "val1=1 val2=2 kwargs={'hello1': 'world2', 'good': 'bye'} incremental=True",
        "val1=2 val2=4 kwargs={'hello2': 'world4', 'good': 'bye'} incremental=True",
        "val1=3 val2=6 kwargs={'hello3': 'world6', 'good': 'bye'} incremental=True",
        "val1=4 val2=8 kwargs={'hello4': 'world8', 'good': 'bye'} incremental=True",
    ]
    # pylint: enable=line-too-long

    def test_serial1(self) -> None:
        """
        Execute serially a workload that succeeds.
        """
        func, tasks = get_workload1(randomize=True)
        num_threads = "serial"
        abort_on_error = True
        exp = self.exp_workload1
        #
        self._helper_success(
            func, tasks, num_threads, abort_on_error, exp
        )

    def test_parallel1(self) -> None:
        """
        Execute with 1 thread a workload that succeeds.
        """
        func, tasks = get_workload1(randomize=True)
        num_threads = "1"
        abort_on_error = True
        exp = self.exp_workload1
        #
        self._helper_success(
            func, tasks, num_threads, abort_on_error, exp
        )

    def test_parallel2(self) -> None:
        """
        Execute with 3 threads a workload that succeeds.
        """
        func, tasks = get_workload1(randomize=True)
        num_threads = "3"
        abort_on_error = True
        exp = self.exp_workload1
        #
        self._helper_success(
            func, tasks, num_threads, abort_on_error, exp
        )

    # pylint: disable=line-too-long
    exp_workload2 = r"""Error: val1=-1 val2=7 kwargs={'hello2': 'world2', 'good2': 'bye2'} incremental=True"""
    # pylint: enable=line-too-long

    def test_serial_fail1(self) -> None:
        """
        Execute serially a workload that fails.
        """
        func, tasks = get_workload2(randomize=False)
        num_threads = "serial"
        abort_on_error = True
        exp = self.exp_workload2
        #
        self._helper_fail(
            func, tasks, num_threads, abort_on_error, exp
        )

    def test_parallel_fail1(self) -> None:
        """
        Execute with 1 thread a workload that fails.
        """
        func, tasks = get_workload2(randomize=False)
        num_threads = "1"
        abort_on_error = False
        exp = self.exp_workload2
        #
        self._helper_fail(
            func, tasks, num_threads, abort_on_error, exp
        )

    def test_parallel_fail2(self) -> None:
        """
        Execute with 3 threads a workload that fails.
        """
        func, tasks = get_workload2(randomize=False)
        num_threads = "3"
        abort_on_error = False
        exp = self.exp_workload2
        #
        self._helper_fail(
            func, tasks, num_threads, abort_on_error, exp
        )

    def test_parallel_fail3(self) -> None:
        """
        Execute with 2 threads a workload that fails, but without
        """
        func, tasks = get_workload2(randomize=False)
        num_threads = "3"
        abort_on_error = False
        exp = self.exp_workload2
        #
        self._helper_fail(
            func, tasks, num_threads, abort_on_error, exp
        )

    # def test_serial_fail2(self) -> None:
    #     """
    #     Execute serially a workload that fails, but with abort_on_error=False.
    #     """
    #     func, tasks = get_workload2(randomize=False)
    #     num_threads = "serial"
    #     abort_on_error = True
    #     exp = self.exp_workload2
    #     #
    #     self._helper_fail(
    #         func, tasks, num_threads, abort_on_error, exp
    #     )

    @staticmethod
    def _outcome_to_string(outcome: List[str]) -> str:
        outcome = "\n".join(sorted(map(str, outcome)))
        return outcome

    def _helper_fail(self,
            func: Callable,
            tasks: List[hjoblib.TASK],
            num_threads: str,
            abort_on_error: bool,
            exp: str,
            ) -> None:
        func_name = "func"
        dry_run = False
        incremental = True
        log_file = os.path.join(self.get_scratch_space(), "log.txt")
        #
        with self.assertRaises(ValueError) as cm:
            res = hjoblib.parallel_execute(
                func, func_name, tasks, dry_run, num_threads, incremental, abort_on_error, log_file)
            _LOG.debug("res=%s", str(res))
        act = str(cm.exception)
        self.assert_equal(act, exp)

    def _helper_success(self,
            func: Callable,
            tasks: List[hjoblib.TASK],
            num_threads: str,
            abort_on_error: bool,
            exp: List[str],
            ) -> None:
        func_name = "func"
        dry_run = False
        incremental = True
        log_file = os.path.join(self.get_scratch_space(), "log.txt")
        #
        res = hjoblib.parallel_execute(func, func_name, tasks, dry_run, num_threads, incremental, abort_on_error, log_file)
        _LOG.debug("res=%s", str(res))
        act = self._outcome_to_string(res)
        exp = self._outcome_to_string(exp)
        self.assert_equal(act, exp)