#!/usr/bin/env python

"""
# To run the tests
> run_tests.py

# To dry run
> run_tests.py --dry_run -v DEBUG

# To run coverage
> run_tests.py --test datetime_utils_test.py --coverage -v DEBUG
"""

import argparse
import logging
import sys
from typing import Tuple

import helpers.dbg as dbg
import helpers.parser as prsr
import helpers.printing as pri
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# #############################################################################

# TODO(gp): Add si.pytest_clean_artifacts()
# TODO(gp): Add unit tests freezing the command to be executed given the args.
# TODO(gp): Add --approx_trans_closure that
#  - get all the files modified (-b, -c)
#  - find all the largest subset of including dirs
#  - run all the tests there

def _build_pytest_opts(
    args: argparse.Namespace
) -> Tuple[str, str]:
    """
    Build the command options for pytest from the command line.

    :return:
        - options for pytest collect step
        - options for pytest
    """
    #
    # Build the marker.
    #
    test = args.test
    # Options for pytest.
    # -> opts
    opts = []
    marker = []
    if test == "fast":
        # `> pytest`
        if args.ci:
            # Report all the tests.
            # opts += " --durations=0"
            # 5 secs limit.
            opts.append(" --durations=5")
    elif test == "slow":
        # `> pytest -m "slow"`
        marker.append("slow")
        if args.ci:
            # 2 mins limit.
            opts.append(" --durations=120")
    elif test == "superslow":
        # `> pytest -m "superslow"`
        marker.append("superslow")
        if args.ci:
            # 30 mins limit.
            opts.append(" --durations=1800")
    else:
        dbg.dfatal("Invalid test='%s'" % test)
    collect_opts = '-m "' + " and ".join(marker) + '"'
    opts.append(collect_opts)
    # Add coverage, if needed.
    if args.coverage:
        opts.extend([
            "--cov",
            "--cov-branch",
            "--cov-report term-missing",
            "--cov-report html",
            "--cov-report annotate",
            ])
    # Nice verbose mode.
    opts.append("-vv")
    # Report the results.
    opts.append("-rpa")
    # Add extra options from the user.
    if args.extra_pytest_arg:
        opts.append(args.extra_pytest_arg)
    opts = " ".join(opts)
    return collect_opts, opts


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
        "--test",
        action="store",
        default="fast",
        type=str,
        help="Run a given set of tests (e.g., fast)"
    )
    parser.add_argument(
        "--num_cpus",
        action="store",
        type=int,
        default=-1,
        help="Use up to a certain number of CPUs (1=serial, -1=all available CPUs)",
    )
    parser.add_argument("--coverage", action="store_true")
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
    parser.add_argument(
        "--ci", action="store_true", help="Run tests in CI mode"
    )
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
    pytest_collect_opts, pytest_opts, pytest_target = _build_pytest_opts(
        args
    )
    #
    # Preview tests.
    #
    if not args.skip_collect:
        cmd = "pytest --collect-only -q %s %s" % (
            pytest_collect_opts,
            pytest_target,
        )
        _system(cmd, dry_run=False)
    if args.collect_only:
        _LOG.warning("Not running tests as per user request")
        sys.exit(0)
    #
    # Run tests.
    #
    if not args.override_pytest_arg:
        if args.num_cpus == 1:
            _LOG.warning("Serial mode selected")
        else:
            # Parallel mode.
            if args.num_cpus == -1:
                import joblib

                n_jobs = joblib.cpu_count()
            else:
                n_jobs = args.num_cpus
            _LOG.warning("Parallel mode selected: running on %s CPUs", n_jobs)
            dbg.dassert_lte(1, n_jobs)
            pytest_opts += " -n %s" % n_jobs
    else:
        _LOG.warning("Overriding the pytest args")
        pytest_opts = args.override_pytest_arg
    #
    _LOG.info("pytest_opts=%s", pytest_opts)
    _LOG.info("pytest_target=%s", pytest_target)
    cmd = "pytest %s %s" % (pytest_opts, pytest_target)
    print("> %s" % cmd)
    # This is the only system that should be not execute to dry run.
    _system(cmd, dry_run=args.dry_run)
    #
    if not args.dry_run and args.coverage:
        # run_tests.py --coverage --test /Users/saggese/src/commodity_research4/amp/core/dataflow/test/test_nodes.py
        # coverage report
        # coverage report --show_missing
        # coverage report --include=*core/dataflow/*
        # coverage report --include=*core/dataflow/* --omit=*/test/test_*
        #
        # To remove all the coverage info:
        # > make coverage_clean
        # > find . -name "*,cover" | xargs rm -rf
        # > rm -rf ./htmlcov
        #
        # Compare to master.
        # https://github.com/marketplace/codecov
        #
        # Go to your browser for the file `htmlcov/index.html`
        # > `open htmlcov/index.html`
        # https://stackoverflow.com/questions/10252010/serializing-class-instance-to-json
        # https://github.com/jsonpickle/jsonpickle
        pass


if __name__ == "__main__":
    _main(_parse())
