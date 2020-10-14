"""
Test all methods in run_tests.py script. In particular,
tests correctness of a pytest command construction
"""
import argparse
import logging
from typing import List

import dev_scripts.testing.run_tests2 as rt
import helpers.unit_test as ut

_LOG = logging.getLogger(__name__)


class Test_PytestSuiteOptsBuilder1(ut.TestCase):
    """Test methods inside run_tests._PytestSuiteOptionsBuilder class"""

    def test_build1(self) -> None:
        """Test build fast suite options for non-ci mode"""
        # Get builder.
        opts_builder = rt._PytestSuiteOptionsBuilder("fast")
        # Build options.
        opts = opts_builder.build(False)
        # Check that opts correct.
        self.assertCountEqual(opts, [])

    def test_build2(self) -> None:
        """Test build slow suite options for non-ci mode"""
        # Get builder.
        opts_builder = rt._PytestSuiteOptionsBuilder("slow")
        # Build options.
        opts = opts_builder.build(False)
        # Check that opts correct.
        self.assertCountEqual(opts, ['-m "slow"'])

    def test_build_ci1(self) -> None:
        """Test build fast suite options for ci mode"""
        # Get builder.
        opts_builder = rt._PytestSuiteOptionsBuilder("fast")
        # Build options.
        opts = opts_builder.build(True)
        # Check that opts correct.
        self.assertCountEqual(opts, ["--timeout=5"])

    def test_build_ci2(self) -> None:
        """Test build superslow suite options for ci mode"""
        # Get builder.
        opts_builder = rt._PytestSuiteOptionsBuilder("superslow")
        # Build options.
        opts = opts_builder.build(True)
        # Check that opts correct.
        self.assertCountEqual(opts, ["--timeout=1800", '-m "superslow"'])

    def test_init_unexisting1(self) -> None:
        """Test build unexisting suite options"""
        with self.assertRaises(AssertionError) as cm:
            # Get builder.
            rt._PytestSuiteOptionsBuilder("suite123")
        self.assertIn("suite123", str(cm.exception))


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
        import joblib

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
