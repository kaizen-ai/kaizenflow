import logging
import os
import time
from typing import Any, List, Optional, Union

import pytest

import helpers.joblib_helpers as hjoblib
import helpers.printing as hprint
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)

# #############################################################################


def workload_function(
    val1: int,
    val2: str,
    #
    **kwargs: Any,
) -> str:
    """
    Function executing the test workload.
    """
    incremental = kwargs.pop("incremental")
    num_attempts = kwargs.pop("num_attempts")
    _ = val1, val2, incremental, num_attempts
    res: str = hprint.to_str("val1 val2 incremental num_attempts kwargs")
    _LOG.debug("res=%s", res)
    time.sleep(0.01)
    if val1 == -1:
        raise ValueError(f"Error: {res}")
    return res


def get_workload1(
    randomize: bool, seed: Optional[int] = None
) -> hjoblib.Workload:
    """
    Return a workload for `workload_function()` with 5 tasks that succeeds.
    """
    tasks = []
    for i in range(5):
        # val1, val2
        task = ((i, 2 * i), {f"hello{i}": f"world{2 * i}", "good": "bye"})
        tasks.append(task)
    workload: hjoblib.Workload = (workload_function, "workload_function", tasks)
    if randomize:
        # Randomize workload.
        workload: hjoblib.Workload = hjoblib.randomize_workload(
            workload, seed=seed
        )
    return workload


def get_workload2() -> hjoblib.Workload:
    """
    Return a workload for `workload_function()` with 1 task that fails.
    """
    task = ((-1, 7), {"hello2": "world2", "good2": "bye2"})
    tasks = [task]
    workload: hjoblib.Workload = (workload_function, "workload_function", tasks)
    return workload


def get_workload3(
    randomize: bool, seed: Optional[int] = None
) -> hjoblib.Workload:
    """
    Return a workload for `workload_function()` with 5 tasks succeeding and one
    task failing.
    """
    workload = get_workload1(randomize=True)
    # Modify the workflow in place.
    (workload_func, func_name, tasks) = workload
    _ = workload_func, func_name
    task = ((-1, 7), {"hello2": "world2", "good2": "bye2"})
    tasks.append(task)
    if randomize:
        # Randomize workload.
        workload: hjoblib.Workload = hjoblib.randomize_workload(
            workload, seed=seed
        )
    return workload


# #############################################################################


class Test_parallel_execute1(hut.TestCase):
    def test_dry_run1(self) -> None:
        """
        Dry-run a workload.
        """
        workload = get_workload1(randomize=True)
        dry_run = True
        num_threads = "serial"
        incremental = True
        num_attempts = 1
        abort_on_error = True
        log_file = os.path.join(self.get_scratch_space(), "log.txt")
        res = hjoblib.parallel_execute(
            workload,
            dry_run,
            num_threads,
            incremental,
            abort_on_error,
            num_attempts,
            log_file,
        )
        _LOG.debug("res=%s", str(res))
        self.assertIs(res, None)

    # pylint: disable=line-too-long
    EXPECTED_RETURN = r"""val1=0, val2=0, incremental=True, num_attempts=1, kwargs={'hello0': 'world0', 'good': 'bye'}
val1=1, val2=2, incremental=True, num_attempts=1, kwargs={'hello1': 'world2', 'good': 'bye'}
val1=2, val2=4, incremental=True, num_attempts=1, kwargs={'hello2': 'world4', 'good': 'bye'}
val1=3, val2=6, incremental=True, num_attempts=1, kwargs={'hello3': 'world6', 'good': 'bye'}
val1=4, val2=8, incremental=True, num_attempts=1, kwargs={'hello4': 'world8', 'good': 'bye'}"""
    # pylint: enable=line-too-long

    def test_serial1(self) -> None:
        """
        Execute:
        - a workload of 5 tasks that succeeds
        - serially
        """
        workload = get_workload1(randomize=True)
        num_threads = "serial"
        abort_on_error = True
        #
        expected_return = self.EXPECTED_RETURN
        _helper_success(
            self, workload, num_threads, abort_on_error, expected_return
        )

    def test_parallel1(self) -> None:
        """
        Execute:
        - a workload of 5 tasks that succeeds
        - with 1 thread
        """
        workload = get_workload1(randomize=True)
        num_threads = "1"
        abort_on_error = True
        #
        expected_return = self.EXPECTED_RETURN
        _helper_success(
            self, workload, num_threads, abort_on_error, expected_return
        )

    def test_parallel2(self) -> None:
        """
        Execute.

        - a workload of 5 tasks that succeeds
        - with 3 threads
        """
        workload = get_workload1(randomize=True)
        num_threads = "3"
        abort_on_error = True
        expected_return = self.EXPECTED_RETURN
        #
        _helper_success(
            self, workload, num_threads, abort_on_error, expected_return
        )


# #############################################################################


class Test_parallel_execute2(hut.TestCase):

    # pylint: disable=line-too-long
    EXPECTED_STRING = r"""Error: val1=-1, val2=7, incremental=True, num_attempts=1, kwargs={'hello2': 'world2', 'good2': 'bye2'}"""
    # pylint: enable=line-too-long

    def test_serial1(self) -> None:
        """
        Execute:
        - a workload of 1 task that fails
        - serially
        """
        workload = get_workload2()
        num_threads = "serial"
        abort_on_error = True
        #
        expected_assertion = self.EXPECTED_STRING
        _helper_fail(
            self, workload, num_threads, abort_on_error, expected_assertion
        )

    def test_serial2(self) -> None:
        """
        Execute:
        - a workload of 1 task that fails
        - serially
        - don't abort because abort_on_error=False
        """
        workload = get_workload2()
        num_threads = "serial"
        abort_on_error = False
        #
        expected_return = self.EXPECTED_STRING
        _helper_success(
            self, workload, num_threads, abort_on_error, expected_return
        )

    def test_parallel1(self) -> None:
        """
        Execute:
        - a workload of 1 task that fails
        - serially
        """
        workload = get_workload2()
        num_threads = 2
        abort_on_error = True
        #
        expected_assertion = self.EXPECTED_STRING
        _helper_fail(
            self, workload, num_threads, abort_on_error, expected_assertion
        )

    def test_parallel2(self) -> None:
        """
        Execute:
        - a workload of 1 task that fails
        - serially
        - don't abort because abort_on_error=False
        """
        workload = get_workload2()
        num_threads = 2
        abort_on_error = False
        #
        expected_return = self.EXPECTED_STRING
        _helper_success(
            self, workload, num_threads, abort_on_error, expected_return
        )


# #############################################################################


class Test_parallel_execute3(hut.TestCase):

    # pylint: disable=line-too-long
    EXPECTED_STRING1 = r"""Error: val1=-1, val2=7, incremental=True, num_attempts=1, kwargs={'hello2': 'world2', 'good2': 'bye2'}"""

    EXPECTED_STRING2 = r"""Error: val1=-1, val2=7, incremental=True, num_attempts=1, kwargs={'hello2': 'world2', 'good2': 'bye2'}
val1=0, val2=0, incremental=True, num_attempts=1, kwargs={'hello0': 'world0', 'good': 'bye'}
val1=1, val2=2, incremental=True, num_attempts=1, kwargs={'hello1': 'world2', 'good': 'bye'}
val1=2, val2=4, incremental=True, num_attempts=1, kwargs={'hello2': 'world4', 'good': 'bye'}
val1=3, val2=6, incremental=True, num_attempts=1, kwargs={'hello3': 'world6', 'good': 'bye'}
val1=4, val2=8, incremental=True, num_attempts=1, kwargs={'hello4': 'world8', 'good': 'bye'}"""
    # pylint: enable=line-too-long

    def test_serial1(self) -> None:
        """
        Execute:
        - a workload with 5 tasks that succeed and 1 task that fails
        - serially
        """
        workload = get_workload3(randomize=False)
        num_threads = "serial"
        abort_on_error = True
        # Since there is an error and `abort_on_error=True` we only get information
        # about the failed task.
        expected_exception = self.EXPECTED_STRING1
        _helper_fail(
            self, workload, num_threads, abort_on_error, expected_exception
        )

    def test_serial2(self) -> None:
        """
        Execute:
        - a workload with 5 tasks that succeed and 1 task that fails
        - serially
        - don't abort because abort_on_error=False
        """
        workload = get_workload3(randomize=False)
        num_threads = "serial"
        abort_on_error = False
        #
        expected_return = self.EXPECTED_STRING2
        _helper_success(
            self, workload, num_threads, abort_on_error, expected_return
        )

    def test_parallel1(self) -> None:
        """
        Execute:
        - a workload with 5 tasks that succeed and 1 task that fails
        - with 1 thread

        The outcome should be the same as `test_serial1`.
        """
        workload = get_workload3(randomize=False)
        num_threads = "1"
        abort_on_error = True
        #
        expected_exception = self.EXPECTED_STRING1
        _helper_fail(
            self, workload, num_threads, abort_on_error, expected_exception
        )

    def test_parallel2(self) -> None:
        """
        Execute:
        - a workload with 5 tasks that succeed and 1 task that fails
        - with 3 threads

        The outcome should be the same as `test_serial1`.
        """
        workload = get_workload3(randomize=False)
        num_threads = "3"
        abort_on_error = True
        #
        expected_exception = self.EXPECTED_STRING1
        _helper_fail(
            self, workload, num_threads, abort_on_error, expected_exception
        )

    def test_parallel3(self) -> None:
        """
        Execute:
        - a workload with 5 tasks that succeed and 1 task that fails
        - with 1 thread
        - don't abort because abort_on_error=False

        The outcome should be the same as `test_serial2`.
        """
        workload = get_workload3(randomize=False)
        num_threads = "1"
        abort_on_error = False
        #
        expected_return = self.EXPECTED_STRING2
        _helper_success(
            self, workload, num_threads, abort_on_error, expected_return
        )

    def test_parallel4(self) -> None:
        """
        Execute:
        - a workload with 5 tasks that succeed and 1 task that fails
        - with 3 thread
        - don't abort because abort_on_error=False

        The outcome should be the same as `test_serial2`.
        """
        workload = get_workload3(randomize=False)
        num_threads = "3"
        abort_on_error = False
        #
        expected_return = self.EXPECTED_STRING2
        _helper_success(
            self, workload, num_threads, abort_on_error, expected_return
        )


@pytest.mark.skip(reason="Just for experimenting with joblib")
class Test_joblib_example1(hut.TestCase):
    @staticmethod
    def func(val: int) -> int:
        print("val=%s" % val)
        if val == -1:
            raise ValueError(f"val={val}")
        print("  out=%s" % val)
        return val

    def test1(self) -> None:
        """
        Show that when a job fails the entire `joblib.Parallel` fails without
        returning anything, but just propagating the exception.
        """
        # num_threads = 5
        num_threads = 1
        vals = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        # vals[1] = -1
        vals[5] = -1
        import joblib

        backend = "loky"
        res = joblib.Parallel(n_jobs=num_threads, backend=backend, verbose=200)(
            joblib.delayed(Test_joblib_example.func)(val) for val in vals
        )
        print("res=%s" % str(res))


# #############################################################################


def _outcome_to_string(outcome: List[str]) -> str:
    outcome = "\n".join(sorted(map(str, outcome)))
    return outcome


def _helper_success(
    self_: Any,
    workload: hjoblib.Workload,
    num_threads: Union[str, int],
    abort_on_error: bool,
    expected_return: str,
) -> None:
    dry_run = False
    incremental = True
    num_attempts = 1
    log_file = os.path.join(self_.get_scratch_space(), "log.txt")
    #
    res = hjoblib.parallel_execute(
        workload,
        dry_run,
        num_threads,
        incremental,
        abort_on_error,
        num_attempts,
        log_file,
    )
    _LOG.debug("res=%s", str(res))
    act = _outcome_to_string(res)
    self_.assert_equal(act, expected_return)


def _helper_fail(
    self_: Any,
    workload: hjoblib.Workload,
    num_threads: Union[str, int],
    abort_on_error: bool,
    expected_assertion: str,
) -> None:
    dry_run = False
    incremental = True
    num_attempts = 1
    log_file = os.path.join(self_.get_scratch_space(), "log.txt")
    #
    with self_.assertRaises(ValueError) as cm:
        res = hjoblib.parallel_execute(
            workload,
            dry_run,
            num_threads,
            incremental,
            abort_on_error,
            num_attempts,
            log_file,
        )
        _LOG.debug("res=%s", str(res))
    act = str(cm.exception)
    self_.assert_equal(act, expected_assertion)
