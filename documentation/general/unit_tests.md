<!--ts-->
   * [Running unit tests](#running-unit-tests)
      * [Using run_tests.py](#using-run_testspy)
         * [Run fast tests](#run-fast-tests)
         * [Run slow tests](#run-slow-tests)
         * [Run parallel tests](#run-parallel-tests)
      * [Using pytest directly](#using-pytest-directly)
         * [Usage and Invocations reference](#usage-and-invocations-reference)
         * [Stop at first failure](#stop-at-first-failure)
         * [Run a single class](#run-a-single-class)
         * [Run a single test method](#run-a-single-test-method)
         * [Remove cache artifacts](#remove-cache-artifacts)
         * [Run with a clear cache](#run-with-a-clear-cache)
         * [Run the tests that last failed](#run-the-tests-that-last-failed)
      * [Custom pytest options behaviors](#custom-pytest-options-behaviors)
         * [Enable debug info](#enable-debug-info)
         * [Update golden outcomes](#update-golden-outcomes)
         * [Incremental test mode (advanced users)](#incremental-test-mode-advanced-users)



<!--te-->

# Running unit tests

-   Before any PR (and ideally after every commit) we want to run all the unit
    tests to make sure we didn't introduce no new bugs
-   We use `pytest` and `unittest` as testing framework

-   We have different test sets:

    -   `fast`: tests that are quick to execute (typically < 5 secs per test
        class)
    -   `slow`: tests that we don't want to run all the times because they are:
        -   slow
        -   related to pieces of code that don't change often
        -   external APIs we don't want to hit completely

-   `fast` tests are a subset of `slow` tests

## Using `run_tests.py`

-   `dev_scripts/testing/run_tests.py` is a wrapper around `pytest` to implement
    some typical workflows

### Run fast tests

-   Run only fast tests:
    ```bash
    > run_tests.py
    > run_tests.py --test fast
    ```

### Run slow tests

-   Run all tests:
    ```bash
    > run_tests.py --test slow
    ```

### Run parallel tests

-   You can use the switch `--num_cpus -1` to use all the available CPUs:
    ```bash
    > run_tests.py --test fast --num_cpus -1
    > run_tests.py --test slow --num_cpus -1
    ```

## Using `pytest` directly

### Usage and Invocations reference

-   See [pytest documentation](http://doc.pytest.org/en/latest/usage.html)

### Stop at first failure

    ```bash
    > pytest -x
    ```

### Run a single class

    ```bash
    > pytest -k TestPcaFactorComputer1
    ```

### Run a single test method

    ```bash
    > pytest core/test/test_core.py::TestPcaFactorComputer1::test_linearize_eigval_eigvec
    ```

### Remove cache artifacts

    ```bash
    > find . -name "__pycache__" -o -name ".pytest_cache"
    ./.pytest_cache
    ./dev_scripts/test/Test_linter_py1.test_linter1/tmp.scratch/__pycache__
    ./dev_scripts/test/__pycache__
    ./dev_scripts/__pycache__

    > find . -name "__pycache__" -o -name ".pytest_cache" | xargs rm -rf
    ```

### Run with a clear cache

    ```bash
    > pytest --cache-clear
    ```

### Run the tests that last failed

    ```bash
    > pytest --last-failed
    ```

-   This data is stored in `.pytest_cache/v/cache/lastfailed`

## Custom pytest options behaviors

### Enable debug info

    ```bash
    > pytest --dbg_verbosity DEBUG
    ```

### Update golden outcomes

-   This switch allows to overwrite the golden outcomes that are used as
    reference in the unit tests to detect failures
    ```bash
    > pytest --update_outcomes
    ```

### Incremental test mode (advanced users)

-   This switch allows to reuse artifacts in the test directory and to skip the
    clean up phase
-   It is used to re-run tests from the middle when they are very long and one
    wants to debug them
    ```bash
    > pytest --incremental
    ```
