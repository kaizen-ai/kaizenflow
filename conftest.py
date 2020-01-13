import os
from typing import Any, Generator

import helpers.dbg as dbg
import helpers.unit_test as hut

# Add custom options.

# Hack to workaround pytest not happy with multiple redundant conftest.py
# (bug #34).
# TODO(gp): -> _CONFTEST_ALREADY_PARSED
if not hasattr(hut, "conftest_already_parsed"):

    # Store whether we are running unit test through pytest.
    # From https://docs.pytest.org/en/latest/example/simple.html#detect-if-running-from-within-a-pytest-run
    def pytest_configure(config: Any) -> None:
        _ = config
        # pylint: disable=protected-access
        hut._CONFTEST_IN_PYTEST = True

    def pytest_unconfigure(config: Any) -> None:
        _ = config
        # pylint: disable=protected-access
        hut._CONFTEST_IN_PYTEST = False

    hut.conftest_already_parsed = True

    def pytest_addoption(parser: Any) -> None:
        parser.addoption(
            "--update_outcomes",
            action="store_true",
            default=False,
            help="Update golden outcomes of test",
        )
        parser.addoption(
            "--incremental",
            action="store_true",
            default=False,
            help="Reuse and not clean up test artifacts",
        )
        parser.addoption(
            "--dbg_verbosity",
            dest="log_level",
            default="INFO",
            choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
            help="Set the logging level",
        )

    def pytest_collection_modifyitems(config: Any, items: Any) -> None:
        _ = items
        if config.getoption("--update_outcomes"):
            print("\nWARNING: Updating test outcomes")
            hut.set_update_tests(True)
        if config.getoption("--incremental"):
            print("\nWARNING: Using incremental test mode")
            hut.set_incremental_tests(True)
        if config.getoption("--dbg_verbosity"):
            print("\nWARNING: Setting verbosity level")
            dbg.init_logger(config.getoption("--dbg_verbosity"))

    if "PYANNOTATE" in os.environ:
        print("\nWARNING: Collecting information about types through pyannotate")
        # From https://github.com/dropbox/pyannotate/blob/master/example/example_conftest.py
        import pytest

        def pytest_collection_finish(session: Any) -> None:
            """
            Handle the pytest collection finish hook: configure pyannotate.
            Explicitly delay importing `collect_types` until all tests have
            been collected.  This gives gevent a chance to monkey patch the
            world before importing pyannotate.
            """
            # mypy: Cannot find module named 'pyannotate_runtime'
            import pyannotate_runtime  # type: ignore

            _ = session
            pyannotate_runtime.collect_types.init_types_collection()

        @pytest.fixture(autouse=True)
        def collect_types_fixture() -> Generator:
            import pyannotate_runtime

            pyannotate_runtime.collect_types.start()
            yield
            pyannotate_runtime.collect_types.stop()

        def pytest_sessionfinish(session: Any, exitstatus: Any) -> None:
            import pyannotate_runtime

            _ = session, exitstatus
            pyannotate_runtime.collect_types.dump_stats("type_info.json")
            print("\n*** Collected types ***")
