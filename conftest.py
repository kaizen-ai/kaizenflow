import helpers.dbg as dbg
import helpers.unit_test as hut

# Add custom options.

# Hack to workaround pytest not happy with multiple redundant conftest.py
# (bug #34).
if not hasattr(hut, "conftest_already_parsed"):
    hut.conftest_already_parsed = True

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
            print("\nWARNING: Setting verb level")
            dbg.init_logger(config.getoption("--dbg_verbosity"))
