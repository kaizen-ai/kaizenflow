<!--ts-->
   * [Running unit tests](#running-unit-tests)
      * [Using run_tests.py](#using-run_testspy)
         * [Run fast tests](#run-fast-tests)
         * [Run slow tests](#run-slow-tests)
         * [Run parallel tests](#run-parallel-tests)
         * [Compute tests coverage](#compute-tests-coverage)
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
   * [Implementing unit tests](#implementing-unit-tests)
      * [General info](#general-info)
         * [What is a unit test?](#what-is-a-unit-test)
         * [Why is unit testing important?](#why-is-unit-testing-important)
      * [Unit testing tips](#unit-testing-tips)
         * [Unit Testing Tip #1: Test one thing](#unit-testing-tip-1-test-one-thing)
         * [Unit Testing Tip #2: Keep tests self-contained](#unit-testing-tip-2-keep-tests-self-contained)
         * [Unit Testing Tip #3: Only specify data related to what is being tested](#unit-testing-tip-3-only-specify-data-related-to-what-is-being-tested)
         * [Unit Testing Tip #4: Test realistic corner cases](#unit-testing-tip-4-test-realistic-corner-cases)
         * [Unit Testing Tip #5: Test a typical scenario](#unit-testing-tip-5-test-a-typical-scenario)
      * [Conventions](#conventions)
         * [Naming and placement conventions](#naming-and-placement-conventions)
         * [Our framework to test using input / output data](#our-framework-to-test-using-input--output-data)
         * [Use text and not pickle files as input](#use-text-and-not-pickle-files-as-input)
         * [check_string vs self.assertEqual](#check_string-vs-selfassertequal)
         * [Smoke test vs "frozen" behavior](#smoke-test-vs-frozen-behavior)
         * [Prefer integration tests](#prefer-integration-tests)
         * [Use self.assert_equal](#use-selfassert_equal)
         * [How to split unit test code in files](#how-to-split-unit-test-code-in-files)
         * [Skeleton for unit test](#skeleton-for-unit-test)
         * [Use the appropriate self.assert*](#use-the-appropriate-selfassert)
         * [Do not use dbg.dassert](#do-not-use-dbgdassert)
         * [Interesting testing functions](#interesting-testing-functions)
      * [Update test tags](#update-test-tags)



<!--te-->

# Running unit tests

- Before any PR (and ideally after every commit) we want to run all the unit
  tests to make sure we didn't introduce no new bugs
- We use `pytest` and `unittest` as testing framework

- We have different test sets:
  - `fast`
    - Tests that are quick to execute (typically < 5 secs per test class)
    - We want to run these tests after every commit / PR to make sure things are
      not horrible broken
  - `slow`
    - Tests that we don't want to run all the times because they are:
      - Slow (typically < 2 minutes per test)
      - Related to pieces of code that don't change often
      - External APIs we don't want to hit continuously
  - `superslow`
    - Tests that run long workload, e.g., running a production model

- `fast` tests are a subset of `slow` tests

## Using `run_tests.py`

- `dev_scripts/testing/run_tests.py` is a wrapper around `pytest` to implement
  some typical workflows

### Run fast tests

- Run only fast tests:
  ```bash
  > run_tests.py
  > run_tests.py --test fast
  ```

### Run slow tests

- Run all tests:
  ```bash
  > run_tests.py --test slow
  ```

### Run parallel tests

- You can use the switch `--num_cpus -1` to use all the available CPUs:
  ```bash
  > run_tests.py --test fast --num_cpus -1
  > run_tests.py --test slow --num_cpus -1
  ```

### Compute tests coverage

- You can use `run_tests2.py` instead of `run_tests.py` to compute the coverage:
  ```bash
  > run_tests2.py --test_suite fast --test ./test --coverage
  ```
- It will create a coverage report. If you want to customize your report:
  ```bash
  > coverage report --include=*.py --omit=test_*.py -m
  ```
  [Here](https://coverage.readthedocs.io/en/latest/cmd.html#reporting) is an
  official documentation about reporting.
- It will also create `htmlcov/` folder, where coverage results are stored as
  `html` files. You can easy share them or review yourself with:
  ```bash
  > cd htmlcov; python -m http.server 33333
  ```
  After that you will be able to go `http://research.p1:33333` or
  `http://localhost:33333` depending where do you start your server and review
  results.
- To find an original command was called by `run_test2.py` you can:
  ```bash
  > run_tests2.py --coverage --dry_run
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

### Run the tests that last failed

    ```bash
    > pytest --last-failed
    ```

- This data is stored in `.pytest_cache/v/cache/lastfailed`

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

# Implementing unit tests

## General info

### What is a unit test?

- A unit test is a small, self-contained test of a public function or public
  method of a library
- The test specifies the given inputs, any necessary state, and the expected
  output
- Running the test ensures that the actual output agrees with the expected
  output

### Why is unit testing important?

- Unit testing is an integral part of Pragmatic programming approach
- Some of the tips that relate to it are:
  - Design with Contracts
  - Refactor Early, Refactor Often
  - Test Your Software, or Your Users Will
  - Coding Ain't Done Till All the Tests Run
  - Test State Coverage, Not Code Coverage
  - You Can't Write Perfect Software
  - Crash Early
  - Design to Test
  - Test Early. Test Often. Test Automatically.
  - Use Saboteurs to Test Your Testing
  - Find Bugs Once.

- Good unit testing improves software quality. It does this in part by
  1. Eliminating bugs (obvious)
  2. Clarifying code design and interfaces ("Design to Test")
  3. Making refactoring safer and easier ("Refactor Early, Refactor Often")
  4. Documenting expected behavior and usage

## Unit testing tips

### Unit Testing Tip #1: Test one thing

- A good unit test tests only one thing
- Testing one thing keeps the unit test simple, relatively easy to understand,
  and helps isolate the root cause when the test fails
- How do we test more than one thing? By having more than one unit test!

### Unit Testing Tip #2: Keep tests self-contained

- A unit test should be independent of all other unit tests
- Each test should be self-sufficient
- Additionally, one should never assume that unit tests will be executed in a
  particular order
- A corollary of keeping tests self-contained is to keep all information needed
  to understand the test within the test itself
- In other words, when possible, avoid calling helper functions to load data or
  state to initialize the test; instead, specify the data explicitly in the test
  where it is used
  - This makes the test easier to understand and easier to debug when it fails
  - If multiple unit tests use or can use the same initialization data, do not
    hesitate repeating it in each test (or consider using parameterized testing)

### Unit Testing Tip #3: Only specify data related to what is being tested

- If a function that is being tested supports optional arguments, but those
  optional arguments are not needed for a particular unit test, then do not
  specify them in the test
- Specify the minimum of what is required to test what is being tested.

### Unit Testing Tip #4: Test realistic corner cases

- Can your function receive a list that is empty?
- Can it return an empty Series?
- What happens if it receives a numerical value outside of an expected range?
- How should the function behave in those cases? Should it crash? Should it
  return a reasonable default value?
- Expect these questions to come up in practice and think through what the
  appropriate behavior should be. Then, test for it.

### Unit Testing Tip #5: Test a `typical` scenario

- In ensuring that corner cases are covered, do not overlook testing basic
  functionality for typical cases
- This is useful for verifying current behavior and to support refactoring.

## Conventions

### Naming and placement conventions

- We follow the convention (that happen to be mostly the default to `pytest`):

- A directory `test` that contains all the test code and artifacts
  - The `test` directory contains all the`test_*.py` files and all inputs and
    outputs for the tests.
  - A unit test file should be close to the library / code it tests

- The test class should make clear reference to the code that it is tested.
  - To test a class `FooBar` the corresponding test class is named `TestFooBar`
  - You can add a number, e.g.,`TestFooBar1()`, if there are multiple test
    classes that are testing the code in different ways (e.g.,`setUp()`
    `tearDown()` are different)
  - To test a function `generate_html_tables()` the corresponding test class is
    `Test_generate_html_tables`
    - This is ok even if it's not a PEP8 compatible class name (i.e., CamelCase)

- It's ok to have multiple test methods, e.g., for `FooBar.method_a()` and
  `FooBar.method_b()`, the test method is:

  ```python
  class TestFooBar1(unittest2.TestCase):

     def test_method_a(self):
         ...

     def test_method_b(self):
         ...
  ```

- Split test classes and methods in a reasonable way, so each one tests one
  single thing in the simplest possible way.

- Remember that test code is not second class citizen, although it's auxiliary
  to the code.
  - Add comments and docstring explaining what the code is doing.
  - If you change the name of a class, also the test should be changed.
  - If you change the name of a file also the name of the file with the testing
    code should be changed

### Our framework to test using input / output data

- `helpers/unit_test.py` has some utilities to easily create input and output
  dirs storing data for unit tests.

- `ut.TestCase` has various methods to help you create
  - `get_input_dir`: return the name of the dir used to store the inputs
  - `get_scratch_space`: return the name of a scratch dir to keep artifacts of
    the test
  - `get_output_dir`: probably not interesting for the user

- The directory structure enforced by the out `TestCase` is like

  ```bash
  > tree -d edg/form_8/test/
  edg/form_8/test/
  └── TestExtractTables1.test1
      ├── input
      └── output
  ```

- The layout of test dir
  ```bash
  > ls -1 helpers/test/
  Test_dassert1.test2
  Test_dassert1.test3
  Test_dassert1.test4
  ...
  Test_dassert_misc1.test6
  Test_dassert_misc1.test8
  Test_system1.test7
  test_dbg.py
  test_helpers.py
  test_system_interaction.py
  ```

### Use text and not pickle files as input

- The problem with pickle files are the usual ones
  - They are not stable across different version of libraries
  - They are not human readable

- Prefer to use text file
  - E.g., use a CSV file

- If the data used for testing is generated in a non-complicated way
  - Document how it was generated
  - Even better add a test that generates the data

- Use a subset of the input data
  - The smaller the better for everybody
    - Fast tests
    - Easier to debug
    - More targeted unit test
  - Do not check in 1 megabyte of test data!

### `check_string` vs `self.assertEqual`

- TODO(gp): Add

### Smoke test vs "frozen" behavior

### Prefer integration tests

### Use `self.assert_equal`

- This is a function that helps you understand what are the mismatches
- It works on `str`

### How to split unit test code in files

- The two extreme approaches are:

  1. All the test code for a directory goes in one file
     `foo/bar/test/test_$DIRNAME.py` (or `foo/bar/test/test_all.py`)
  2. Each file `foo/bar/$FILENAME` with code gets its corresponding
     `foo/bar/test/test_$FILENAME.py`
     - It should also be named according to the library it tests
     - For example, if the library to test is called `pnl.py`, then the
       corresponding unit test should be called`test_pnl.py`

- Pros of 1) vs 2)
  - Less maintenance churn
    - It takes work to keep the code and the test files in sync, e.g.,
      - If you change the name of the code file you don't have to change other
        file names
      - If you move one class from one file to another, you might not need to
        move test code
  - Fewer files opened in your editor
  - Avoid many files with a lot of boilerplate code
- Cons of 1) vs 2)
  - The single file can become huge!

- Compromise solution:
  - Start with a single file `test_$DIRNAME.py` (or `test_dir_name.py`)
  - In the large file add a framed comment like:
    ```python
    # #################
    # Unit tests for file: ...
    # #################
    ```
    - So it's easy to find which file is tested were using grep
  - Then split when it becomes too big using `test_$FILENAME.py`

### Skeleton for unit test

- Interesting unit tests are in `helpers/test`

- A unit test looks like:

  ```python
  import helpers.unit_test as hut

  class Test...(hut.TestCase):

      def test...(self):
            ...
  ```

- `pytest` will take care of running the code so you don't need:
  ```python
  if __name__ == '__main__':
      unittest.main()
  ```

### Use the appropriate `self.assert*`

- When you get a failure you don't want to get something like "True is not
  False", rather an informative message like "5 is not < 4"

- **Bad**

  ```python
  self.assertTrue(a < b)
  ```

- **Good**
  ```python
  self.assertLess(a, b)
  ```

### Do not use `dbg.dassert`

- `dassert` are for checking self-consistency of the code
- The invariant is that you can remove `dbg.dassert` without changing the
  behavior of the code. Of course you can't remove the assertion and get unit
  tests to work

### Interesting testing functions

- List of useful testing functions are:
  - [General python](https://docs.python.org/2/library/unittest.html#test-cases)
  - [Numpy](https://docs.scipy.org/doc/numpy-1.15.0/reference/routines.testing.html)
  - [Pandas](https://pandas.pydata.org/pandas-docs/version/0.21/api.html#testing-functions)

## Update test tags

- There are 2 files with the list of tests' tags:
  - `amp/pytest.ini`
  - `commodity_research/pytest.ini`

- In order to update the tags (do it in the both files):
  - In the `markers` section add a name of a new tag
  - Afther a `:` add a short description
  - Keep tags in the alpabetical order
  <!-- #endregion -->
