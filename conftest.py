import os

import helpers.dbg as dbg
import helpers.unit_test as hut

# Add custom options.

# Hack to workaround pytest not happy with multiple redundant conftest.py
# (bug #34).
if not hasattr(hut, "conftest_already_parsed"):
    # mypy: Module has no attribute "conftest_already_parsed"
    hut.conftest_already_parsed = True  # type: ignore

    def pytest_addoption(parser):
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

    def pytest_collection_modifyitems(config, items):
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

    if os.environ["PYANNOTATE"]:
        print("\nWARNING: Collecting information about types through pyannotate")
        # From https://github.com/dropbox/pyannotate/blob/master/example/example_conftest.py
        import pytest

        def pytest_collection_finish(session):
            """
            Handle the pytest collection finish hook: configure pyannotate.
            Explicitly delay importing `collect_types` until all tests have
            been collected.  This gives gevent a chance to monkey patch the
            world before importing pyannotate.
            """
            # mypy: Cannot find module named 'pyannotate_runtime'
            from pyannotate_runtime import collect_types  # type: ignore

            _ = session
            collect_types.init_types_collection()

        @pytest.fixture(autouse=True)
        def collect_types_fixture():
            from pyannotate_runtime import collect_types

            collect_types.start()
            yield
            collect_types.stop()

        def pytest_sessionfinish(session, exitstatus):
            from pyannotate_runtime import collect_types

            _ = session, exitstatus
            collect_types.dump_stats("type_info.json")
            print("\n*** Collected types ***")
