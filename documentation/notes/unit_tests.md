<!--ts-->
   * [Running unit tests](documentation/notes/unit_tests.md#running-unit-tests)
      * [Using the run_tests.py wrapper](documentation/notes/unit_tests.md#using-the-run_testspy-wrapper)
         * [Run fast tests](documentation/notes/unit_tests.md#run-fast-tests)
         * [Run slow tests](documentation/notes/unit_tests.md#run-slow-tests)
         * [Run parallel tests](documentation/notes/unit_tests.md#run-parallel-tests)
      * [Using pytest directly](documentation/notes/unit_tests.md#using-pytest-directly)
         * [Usage and Invocations reference](documentation/notes/unit_tests.md#usage-and-invocations-reference)
         * [Stop at first failure](documentation/notes/unit_tests.md#stop-at-first-failure)
         * [Run a single class](documentation/notes/unit_tests.md#run-a-single-class)
         * [Run a single test method](documentation/notes/unit_tests.md#run-a-single-test-method)
         * [Remove cache artifacts](documentation/notes/unit_tests.md#remove-cache-artifacts)
         * [Run with a clear cache](documentation/notes/unit_tests.md#run-with-a-clear-cache)
      * [Custom pytest options behaviors](documentation/notes/unit_tests.md#custom-pytest-options-behaviors)
         * [Enable debug info](documentation/notes/unit_tests.md#enable-debug-info)
         * [Update golden outcomes](documentation/notes/unit_tests.md#update-golden-outcomes)
         * [Incremental test mode (advanced users)](documentation/notes/unit_tests.md#incremental-test-mode-advanced-users)

<!-- Added by: saggese, at: Sat Oct 19 19:38:40 EDT 2019 -->

<!--te-->

# Running unit tests

## Using the `run_tests.py` wrapper

### Run fast tests
    ```bash
    > run_tests.py
    > run_tests.py --test fast
    ```

### Run slow tests
    ```bash
    > run_tests.py --test slow
    ```

### Run parallel tests
- You can use the switch `--num_cpus -1` to use all the available CPUs
    ```bash
    > run_tests.py --test slow --num_cpus -1
    ```

## Using `pytest` directly

### Usage and Invocations reference

- See [pytest documentation](http://doc.pytest.org/en/latest/usage.html)

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

## Custom pytest options behaviors

### Enable debug info
    ```bash
    > pytest --dbg_verbosity DEBUG
    ```

### Update golden outcomes
- This switch allows to overwrite the golden outcomes that are used as reference
  in the unit tests to detect failures
    ```bash
    > pytest --update_outcomes
    ```

### Incremental test mode (advanced users)
- This switch allows to reuse artifacts in the test directory and to skip the
  clean up phase
- It is used to re-run tests from the middle when they are very long and one
  wants to debug them
    ```bash
    > pytest --incremental
    ```
