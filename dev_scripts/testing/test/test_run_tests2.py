"""
Test all methods in run_tests.py script. In particular,
tests correctness of a pytest command construction
"""
import argparse
import logging
from typing import List

import helpers.unit_test as ut

import dev_scripts.testing.run_tests2 as rt

_LOG = logging.getLogger(__name__)


class Test_get_test_suite_property1(ut.TestCase):
    """Test run_tests._get_test_suite_property method"""

    # Define available suites.
    suites = rt._get_available_test_suites()

    def test_existing_property1(self) -> None:
        """Test get timeout property from existing suite"""
        # Get property.
        timeout = rt._get_test_suite_property("fast", "timeout")
        # Find etalon value.
        etalon = self.suites["fast"].timeout
        # Check that getting property is correct.
        self.assertEqual(timeout, etalon)

    def test_existing_property2(self) -> None:
        """Test get marks property from existing suite"""
        # Get property.
        marks = rt._get_test_suite_property("slow", "marks")
        # Find etalon value.
        etalon = self.suites["slow"].marks
        # Check that getting property is correct.
        self.assertEqual(marks, etalon)

    def test_unexisting_property1(self) -> None:
        """Test get unexisting property from existing suite"""
        # Get property.
        with self.assertRaises(AttributeError) as cm:
            rt._get_test_suite_property("fast", "property123")
        # Check that property is None.
        self.assertIn("property123", str(cm.exception))

    def test_unexisting_test_suite1(self) -> None:
        """Test get a property from unexisting suite"""
        # Get property.
        with self.assertRaises(AssertionError) as cm:
            rt._get_test_suite_property("suite123", "timeout")
        # Check that exception raised by method.
        self.assertIn("Invalid _build_pytest_optsuite", str(cm.exception))


class Test_get_marker_options1(ut.TestCase):
    """Test run_tests._get_marker_options method"""

    def test_empty_marker1(self) -> None:
        """Test get marker options from fast test suite"""
        # Get marker.
        marker = rt._get_marker_options("fast")
        # Check that marker is empty.
        self.assertListEqual(marker, [])

    def test_non_empty_marker1(self) -> None:
        """Test get marker options from slow test suite"""
        # Get marker.
        marker = rt._get_marker_options("slow")
        # Check that marker is correct.
        self.assertListEqual(marker, ['-m "slow"'])


class Test_get_timeout_options1(ut.TestCase):
    """Test run_tests._get_timeout_options method"""

    def test_non_empty_timeout1(self) -> None:
        """Test get timeout from superslow test suite"""
        # Get timeout.
        timeout = rt._get_timeout_options("superslow")
        # Check that timeout is correct.
        self.assertListEqual(timeout, ["--timeout=1800"])


class Test_get_parallel_options1(ut.TestCase):
    """Test run_tests._get_parallel_options method"""

    def test1(self) -> None:
        """Test all available cores usage"""
        # Look to log.
        with self.assertLogs(rt._LOG, level="WARNING") as cm:
            # Get number of parallel jobs.
            n_jobs = rt._get_parallel_options(-1)
            # Check that all cores will be used.
            self.assertListEqual(n_jobs, ["-n %d" % self._num_cores()])
        # Check that log understands parallel mode correctly.
        self._check_log(parallel=True, log_strings=cm.output)

    def test2(self) -> None:
        """Test 1 core usage"""
        # Look to log.
        with self.assertLogs(rt._LOG, level="WARNING") as cm:
            # Get number of parallel jobs.
            n_jobs = rt._get_parallel_options(1)
            # Check that 1 core will be used.
            self.assertListEqual(n_jobs, [])
        # Check that log understands serial mode correctly.
        self._check_log(parallel=False, log_strings=cm.output)

    def test3(self) -> None:
        """Test 10 core usage"""
        # Look to log.
        with self.assertLogs(rt._LOG, level="WARNING") as cm:
            # Get number of parallel jobs.
            n_jobs = rt._get_parallel_options(10)
            # Check that all cores will be used.
            self.assertListEqual(n_jobs, ["-n 10"])
        # Check that log understads parallel mode correctly.
        self._check_log(parallel=True, log_strings=cm.output)

    def _num_cores(self) -> int:
        import joblib  # type: ignore

        return int(joblib.cpu_count())

    def _check_log(self, parallel: bool, log_strings: List[str]) -> None:
        """Check assert that expected mode message is present in the log"""
        parallel_msg = "Parallel mode selected"
        serial_msg = "Serial mode selected"
        # Define what is expected depending on mode.
        if parallel:
            expected = parallel_msg
            unexpected = serial_msg
        else:
            expected = serial_msg
            unexpected = parallel_msg
        # Find message in logs.
        log_text = "\n".join(log_strings)
        self.assertIn(expected, log_text)
        # Check that not needed message is not presented in logs.
        self.assertNotIn(unexpected, log_text)


class Test_build_pytest_opts1(ut.TestCase):
    """Test run_tests._build_pytest_opts method"""

    def test_all_args1(self) -> None:
        """
        Test options building if all args except override_pytest_arg
        are presented and not default
        """
        # Define incoming args.
        args = argparse.Namespace(
            test_suite="slow",
            num_cpus=8,
            coverage=True,
            ci=True,
            extra_pytest_arg="--extra -e 1",
            override_pytest_arg=None,
        )
        collect_opts, run_opts = rt._build_pytest_opts(args)
        # Check options for collect command.
        self.assertCountEqual(
            collect_opts,
            [
                "--collect-only",
                '-m "slow"',
                "-q",
            ],
        )
        # Check options for run command.
        self.assertCountEqual(
            run_opts,
            [
                '-m "slow"',
                "-n 8",
                "--timeout=120",
                "--cov",
                "--cov-branch",
                "--cov-report term-missing",
                "--cov-report html",
                "--cov-report annotate",
                "--extra -e 1",
                "-vv",
                "-rpa",
            ],
        )

    def test_all_args2(self) -> None:
        """
        Test options building if all args except override_pytest_arg
        are presented and default
        """
        # Define incoming args.
        args = argparse.Namespace(
            test_suite="fast",
            num_cpus=1,
            coverage=False,
            ci=False,
            extra_pytest_arg=None,
            override_pytest_arg=None,
        )
        collect_opts, run_opts = rt._build_pytest_opts(args)
        # Check options for collect command.
        self.assertCountEqual(
            collect_opts,
            [
                "--collect-only",
                "-q",
            ],
        )
        # Check options for run command.
        self.assertCountEqual(
            run_opts,
            [
                "-vv",
                "-rpa",
            ],
        )

    def test_override_args1(self) -> None:
        """Test options building if override_pytest_arg is presented"""
        # Define incoming args.
        args = argparse.Namespace(
            test_suite="slow",
            num_cpus=8,
            coverage=True,
            ci=True,
            extra_pytest_arg="--extra -e 1",
            override_pytest_arg="--override=me",
        )
        collect_opts, run_opts = rt._build_pytest_opts(args)
        # Check options for collect command.
        self.assertCountEqual(
            collect_opts,
            [
                "--collect-only",
                '-m "slow"',
                "-q",
            ],
        )
        # Check options for run command.
        self.assertCountEqual(
            run_opts,
            [
                "--override=me",
            ],
        )


class Test_build_pytest_cmd1(ut.TestCase):
    """Test run_tests._build_pytest_cmd method"""

    def test_empty_args1(self) -> None:
        """
        Test command building if args are options are empty and
        test is specified.
        """
        cmd = rt._build_pytest_cmd("test_me.py", [])
        self.assertEqual(cmd, "pytest test_me.py")

    def test_empty_args2(self) -> None:
        """
        Test command building if args are options are empty and
        test is not specified.
        """
        cmd = rt._build_pytest_cmd("", [])
        self.assertEqual(cmd, "pytest")

    def test_non_empty_args1(self) -> None:
        """
        Test command building if args are options are presented and
        test is specified.
        """
        cmd = rt._build_pytest_cmd("test_me.py", ["--opt1", "--opt2=3"])
        self.assertEqual(cmd, "pytest --opt1 --opt2=3 test_me.py")

    def test_non_empty_args2(self) -> None:
        """
        Test command building if args are options are presented and
        test is not specified.
        """
        cmd = rt._build_pytest_cmd("", ["--opt1", "--opt2=3"])
        self.assertEqual(cmd, "pytest --opt1 --opt2=3")
