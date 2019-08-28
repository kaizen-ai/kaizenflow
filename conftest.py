import helpers.dbg as dbg
import helpers.unit_test as hut

# Add option to update golden outcome.


def pytest_addoption(parser):
    parser.addoption(
        "--update_outcomes",
        action="store_true",
        default=False,
        help="Update golden outcomes of test")
    parser.addoption(
        "--dbg_verbosity",
        dest="log_level",
        default="INFO",
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help="Set the logging level")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--update_outcomes"):
        print("\nWARNING: Updating test outcomes")
        hut.set_update_tests(True)
    if config.getoption("--dbg_verbosity"):
        print("\nWARNING: Setting verb level")
        dbg.init_logger(config.getoption("--dbg_verbosity"))
