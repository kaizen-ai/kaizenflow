#!/usr/bin/env python

"""
# To run the tests
> run_tests2.py
> run_tests2.py --test_suite slow

# To dry run
> run_tests2.py --dry_run -v DEBUG

# To run coverage
> run_tests2.py --test datetime_utils_test.py --coverage -v DEBUG

# To run in ci mode
> run_tests2.py --test_suite superslow --ci

# To clean pytest artifacts
> run_tests2.py --cleanup
"""

import argparse
import dataclasses
import logging
import sys
from typing import List, Tuple

import helpers.dbg as dbg
import helpers.parser as prsr
import helpers.printing as pri
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# #############################################################################

# TODO(gp): Add unit tests freezing the command to be executed given the args.
# TODO(gp): Add --approx_trans_closure that
#  - get all the files modified (-b, -c)
#  - find all the largest subset of including dirs
#  - run all the tests there


@dataclasses.dataclass
class _TestSuite:
    """Class represents a test suite"""

    timeout: int
    marks: List[str] = dataclasses.field(default_factory=list)


class _PytestSuiteOptionsBuilder:
    """Represent methods to build pytest options depending on test_suite"""

    _available_suites = {
        "fast": _TestSuite(timeout=5),
        "slow": _TestSuite(timeout=120, marks=["slow"]),
        "superslow": _TestSuite(timeout=1800, marks=["superslow"]),
    }

    def __init__(self, test_suite_name: str) -> None:
        self._check(test_suite_name)
        self._test_suite = self._available_suites[test_suite_name]

    def build(self, ci: bool) -> List[str]:
        """Build list of pytest options"""
        opts = self._marker_options()
        if ci:
            opts.extend(self._timeout_options())
        return opts

    def _check(self, test_suite_name: str) -> None:
        """Check if test_suite is available"""
        dbg.dassert_in(
            test_suite_name,
            self._available_suites,
            "Invalid _build_pytest_optsuite='%s'" % test_suite_name,
        )

    def _marker_options(self) -> List[str]:
        """
        Convert test_suite's marks to marker option for pytest.

        :return: list of options related to marker.
        """
        opts = []
        if self._test_suite.marks:
            # > pytest -m "mark1 and mark2"
            opts.append('-m "' + " and ".join(self._test_suite.marks) + '"')
        return opts

    def _timeout_options(self) -> List[str]:
        """
        Convert test_suite's timeout to timeout option for pytest.

        :return: list of options related to timeout.
        """
        opts = []
        if self._test_suite.timeout:
            # --timeout=0 equals disabling timeout
            # > pytest --timeout=100
            opts.append("--timeout=%d" % self._test_suite.timeout)
        return opts


def _get_coverage_options() -> List[str]:
    opts = [
        "--cov",
        "--cov-branch",
        "--cov-report term-missing",
        "--cov-report html",
        "--cov-report annotate",
    ]
    return opts


def _get_real_number_of_cores_to_use(num_cpus: int) -> int:
    if num_cpus == -1:
        # -1 means all available
        import joblib

        return int(joblib.cpu_count())
    return num_cpus


def _get_parallel_options(num_cpus: int) -> List[str]:
    """
    Find num cores on which pytest will be executed

    :param num_cpus: num CPUs to use, if specified as -1 - will use all CPUs
    :return: list of options related to parallelizing
    """
    opts: List[str] = []
    # Get number of parallel jobs.
    n_jobs = _get_real_number_of_cores_to_use(num_cpus)
    # Serial mode
    if n_jobs == 1:
        _LOG.warning("Serial mode selected")
        return opts
    # Parallel mode.
    _LOG.warning("Parallel mode selected: running on %s CPUs", n_jobs)
    dbg.dassert_lte(1, n_jobs)
    opts.append("-n %s" % n_jobs)
    return opts


def _get_output_options() -> List[str]:
    opts = []
    # Nice verbose mode.
    opts.append("-vv")
    # Report the results.
    opts.append("-rpa")
    return opts


def _run_cleanup(cleanup: bool) -> None:
    if cleanup:
        si.pytest_clean_artifacts(".")


def _build_pytest_opts(args: argparse.Namespace) -> Tuple[List[str], List[str]]:
    """
    Build the command options for pytest from the command line.
    Following args are used to build a list of options:
        - ci
        - coverage
        - extra_pytest_arg
        - num_cpus
        - override_pytest_arg
        - test_suite

    :return:
        - options for pytest collect step
        - options for pytest
    """
    # Options for pytest.
    # -> opts
    opts = []
    collect_opts = []
    # Add marker and timeout.
    test_suite_opts_builder = _PytestSuiteOptionsBuilder(args.test_suite)
    opts.extend(test_suite_opts_builder.build(args.ci))
    collect_opts.extend(test_suite_opts_builder.build(False))
    # Save options for collect step.
    collect_opts.extend(["--collect-only", "-q"])
    # Return if pytest arguments specified directly.
    if args.override_pytest_arg is not None:
        _LOG.warning("Overriding the pytest args")
        return collect_opts, [args.override_pytest_arg]
    # Add coverage, if needed.
    if args.coverage:
        opts.extend(_get_coverage_options())
    # Add parallelize options.
    opts.extend(_get_parallel_options(args.num_cpus))
    # Add default options.
    opts.extend(_get_output_options())
    # Add extra pytest args
    if args.extra_pytest_arg is not None:
        opts.append(args.extra_pytest_arg)

    return collect_opts, opts


def _build_pytest_cmd(target: str, opts: List[str]) -> str:
    _LOG.info("pytest_opts=%s", " ".join(opts))
    _LOG.info("pytest_target=%s", target)
    # Construct command parts.
    cmd_parts = ["pytest"]
    cmd_parts.extend(opts)
    if target:
        cmd_parts.append(target)
    # Union parts to a final command
    cmd = " ".join(cmd_parts)
    return cmd


def _info_about_coverage() -> None:
    """Print instruction how to use coverage info and how to clean it up"""
    message = (
        "Use https://coverage.readthedocs.io/en/stable/"
        "cmd.html#cmd-report to get custom reports. Some examples:\n"
        " > coverage report\n"
        " > coverage report --include=*core/dataflow/* --show_missing\n"
        "\n"
        "Go to your browser for the file `htmlcov/index.html`\n"
        " > open htmlcov/index.html"
        "\n"
        "To remove all the coverage info:\n"
        " > make coverage_clean\n"
        " > find . -name '*,cover' | xargs rm -rf\n"
        " > rm -rf ./htmlcov\n"
        "\n"
        "Compare to master.\n"
    )
    print(pri.frame(message))


def _system(cmd: str, dry_run: bool) -> None:
    print(pri.frame("> " + cmd))
    wrapper = None
    si.system(
        cmd,
        wrapper=wrapper,
        suppress_output=False,
        dry_run=dry_run,
        log_level="echo",
    )


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--test_suite",
        action="store",
        default="fast",
        type=str,
        help="Run a given set of tests (e.g., fast)",
    )
    parser.add_argument(
        "--test",
        action="store",
        default="",
        type=str,
        help="Tests location: dir, module or selected test in module",
    )
    parser.add_argument(
        "--num_cpus",
        action="store",
        type=int,
        default=-1,
        help="Use up to a certain number of CPUs"
        " (1=serial, -1=all available CPUs)",
    )
    parser.add_argument(
        "--coverage",
        action="store_true",
        help="Collect and report coverage information",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Clean pytest artifacts after testing",
    )
    parser.add_argument(
        "--extra_pytest_arg",
        action="store",
        help="Options to pass to pytest",
    )
    parser.add_argument(
        "--collect_only", action="store_true", help="Perform only collection step"
    )
    parser.add_argument(
        "--skip_collect", action="store_true", help="Skip the collection step"
    )
    parser.add_argument("--ci", action="store_true", help="Run tests in CI mode")
    # Debug.
    parser.add_argument(
        "--override_pytest_arg",
        action="store",
        help="Override standard pytest command line",
    )
    parser.add_argument("--dry_run", action="store_true")
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    #
    log_level = logging.getLevelName(args.log_level)
    _LOG.debug("%s -> %s", args.log_level, log_level)
    #
    # Build pytest options.
    #
    pytest_collect_opts, pytest_opts = _build_pytest_opts(args)
    #
    # Preview tests.
    #
    if not args.skip_collect:
        cmd = _build_pytest_cmd(args.test, pytest_collect_opts)
        _system(cmd, dry_run=False)
    #
    # Exit if run is not needed.
    #
    if args.collect_only:
        _LOG.warning("Not running tests as per user request")
        sys.exit(0)
    #
    # Run tests.
    #
    cmd = _build_pytest_cmd(args.test, pytest_opts)
    print("> %s" % cmd)
    # This is the only system that should be not execute to dry run.
    _system(cmd, dry_run=args.dry_run)
    #
    # Cleanup.
    #
    _run_cleanup(args.cleanup)
    #
    # Show info how to use coverage
    #
    if not args.dry_run and args.coverage:
        _info_about_coverage()
        # https://github.com/marketplace/codecov
        # https://stackoverflow.com/questions/10252010/serializing-class-instance-to-json
        # https://github.com/jsonpickle/jsonpickle


if __name__ == "__main__":
    _main(_parse())
