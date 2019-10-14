# Running unit tests

## Using the `run_tests.py` wrapper

### Run fast tests
    ```
    > run_tests.py
    > run_tests.py --test fast
    ```

### Run parallel tests
- You can use the switch `--num_cpus -1` to use all the available CPUs

### Run slow tests
    ```
    > run_tests.py --test slow
    ```

## Using `pytest` directly

### Usage and Invocations reference
- See [documentation](http://doc.pytest.org/en/latest/usage.html)

### Enable debug info
    ```bash
    > pytest --dbg_verbosity DEBUG
    ```

### Update golden outcomes
    ```bash
    > pytest --update_outcomes
    ```

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
