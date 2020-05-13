#!/usr/bin/env python

"""
- To run the tests
> run_tests.py

- To dry run
> run_tests.py --dry_run -v DEBUG

- To run coverage
> run_tests.py --test datetime_utils_test.py --coverage -v DEBUG
"""

import argparse
import logging
import sys

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


def _build_pytest_opts(args, test: str):
    pytest_mark_opts = []
    pytest_target = ""
    if test in ("fast", "slow"):
        if test == "fast":
            mark_opts = "not slow"
        elif test == "slow":
            mark_opts = ""
        else:
            raise ValueError("Invalid '%s'" % test)
        if mark_opts:
            mark_opts += " and"
        # Skip certain tests, if needed.
        mark_opts += " not broken_deps"
        if si.get_server_name() in ("gpmac",):
            mark_opts += " and not aws_deps"
        pytest_mark_opts.append(mark_opts)
    else:
        # Pick a specific test.
        if test:
            pytest_target = test
        else:
            pytest_target = "."
    # Build the options.
    pytest_opts = ""
    if pytest_mark_opts:
        pytest_opts += '-m "' + " and ".join(pytest_mark_opts) + '"'
    #
    pytest_collect_opts = pytest_opts
    #
    if args.jenkins:
        # Report the tests with a certain duration.
        # pytest_opts += " --durations=0"
        pytest_opts += " --durations=20"
    # Add coverage, if needed.
    if args.coverage:
        pytest_opts += (
            " --cov"
            " --cov-branch"
            " --cov-report term-missing"
            " --cov-report html"
            " --cov-report annotate"
        )
    # Nice verbose mode.
    pytest_opts += " -vv"
    # Report the results.
    pytest_opts += " -rpa"
    if args.extra_pytest_arg:
        pytest_opts += " " + args.extra_pytest_arg
    return pytest_collect_opts, pytest_opts, pytest_target


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--test",
        action="store",
        default="fast",
        type=str,
        help="Run a given set of tests (e.g., fast) or a given test using pytest"
        " specification style",
    )
    parser.add_argument(
        "--num_cpus",
        action="store",
        type=int,
        default=1,
        help="Use up to a certain number of CPUs (1=serial, -1=all available CPUs)",
    )
    parser.add_argument("--coverage", action="store_true")
    parser.add_argument(
        "--extra_pytest_arg",
        action="store",
        help="Options to add to the standard pytest command line",
    )
    parser.add_argument(
        "--override_pytest_arg",
        action="store",
        help="Options to override to the standard pytest command line",
    )
    parser.add_argument(
        "--collect_only", action="store_true", help="Only collection step"
    )
    parser.add_argument(
        "--skip_collect", action="store_true", help="Skip the collection step"
    )
    parser.add_argument(
        "--jenkins", action="store_true", help="Run tests as Jenkins"
    )
    #
    parser.add_argument("--dry_run", action="store_true")
    prsr.add_verbosity_arg(parser)
    return parser


# TODO(gp): Refactor this function in smaller pieces.
def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    #
    test = args.test
    log_level = logging.getLevelName(args.log_level)
    _LOG.debug("%s -> %s", args.log_level, log_level)
    #
    # Report current setup.
    #
    wrapper = []
    if wrapper:
        wrapper = " && ".join(wrapper)
        if args.log_level >= logging.DEBUG:
            # Silence the wrapper.
            wrapper = "((" + wrapper + ") 2>%1 >/dev/null)"
    else:
        wrapper = None
    _LOG.debug("wrapper=%s", wrapper)

    def _system(cmd, dry_run):
        print(pri.frame("> " + cmd))
        si.system(
            cmd,
            wrapper=wrapper,
            suppress_output=False,
            dry_run=dry_run,
            log_level=logging.DEBUG,
        )
    #
    if args.jenkins:
        cmds = [
            # TODO(gp): For some reason `system("conda")` doesn't work from
            #  this script. Maybe we need to use the hco.conda_system().
            # "conda list",
            "git log -5",
            "whoami",
            "printenv",
            "pwd",
            "date",
        ]
        for cmd in cmds:
            _system(cmd, dry_run=False)
    #
    # Build pytest options.
    #
    pytest_collect_opts, pytest_opts, pytest_target = _build_pytest_opts(args, test)
    #
    # Preview tests.
    #
    if not args.skip_collect:
        cmd = "pytest --collect-only -q %s %s" % (pytest_collect_opts, pytest_target)
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
        # coverage report --include=core/dataflow/*
        # coverage report --include=core/dataflow/* --omit=*/test/test_*
        # Compare to master.
        # Go to your browser at open htmlcov/index.html
        # https://stackoverflow.com/questions/10252010/serializing-class-instance-to-json
        # https://github.com/jsonpickle/jsonpickle
        pass


if __name__ == "__main__":
    _main(_parse())
