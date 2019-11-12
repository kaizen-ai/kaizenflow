#!/usr/bin/env python
"""
- To run the tests
> run_tests.py

- To dry run
> run_tests.py --dry_run -v DEBUG

- To test coverage
> run_tests.py --test datetime_utils_test.py --coverage -v DEBUG
"""

import argparse
import logging
import sys

import helpers.dbg as dbg
import helpers.printing as pri
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# ##############################################################################


def _parse():
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
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )
    return parser


# TODO(gp): Refactor this function in smaller pieces.
def _main(parser):
    args = parser.parse_args()
    dbg.init_logger(verb=args.log_level, use_exec_path=True)
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
            # this script. Maybe we need to use the hco.conda_system().
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
    pytest_mark_opts = []
    pytest_test = ""
    if test in ("fast", "slow"):
        if test == "fast":
            market_opts = "not slow"
        elif test == "slow":
            market_opts = ""
        else:
            raise ValueError("Invalid '%s'" % test)
        if market_opts:
            market_opts += " and"
        # Skip certain tests, if needed.
        market_opts += " not broken_deps"
        if si.get_server_name() in ("gpmac",):
            market_opts += " and not aws_deps"
        pytest_mark_opts.append(market_opts)
    else:
        # Pick a specific test.
        if test:
            pytest_test = test
        else:
            pytest_test = "."
    # Build the options.
    pytest_opts = ""
    if pytest_mark_opts:
        pytest_opts += '-m "' + " and ".join(pytest_mark_opts) + '"'
    if args.jenkins:
        # Report all the tests.
        # pytest_opts += " --durations=0"
        pytest_opts += " --durations=20"
    if pytest_test:
        pytest_opts += " " + pytest_test
    #
    # Preview tests.
    #
    if not args.skip_collect:
        cmd = "pytest --collect-only -q %s %s" % (pytest_opts, pytest_test)
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
    else:
        _LOG.warning("Overriding the pytest args")
        pytest_opts = args.override_pytest_arg
    #
    _LOG.info("pytest_opts=%s", pytest_opts)
    _LOG.info("pytest_test=%s", pytest_test)
    cmd = "pytest %s %s" % (pytest_opts, pytest_test)
    print("> %s" % cmd)
    # This is the only system that should be not execute to dry run.
    _system(cmd, dry_run=args.dry_run)


if __name__ == "__main__":
    _main(_parse())
