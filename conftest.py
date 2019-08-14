import helpers.unit_test as hut

# Add option to update golden outcome.


def pytest_addoption(parser):
    parser.addoption(
        "--update_outcomes",
        action="store_true",
        default=False,
        help="Update golden outcomes of test")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--update_outcomes"):
        print("\nWARNING: Updating test outcomes")
        hut.set_update_tests(True)
