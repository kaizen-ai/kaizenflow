# General info on unit test

## Types of tests

### Unit test
- A unit test is a small, self-contained test of a public function or public
  method of a library
- The test specifies the given inputs, any necessary state, and the expected
  output
- Running the test ensures that the actual output agrees with the expected
  output
    - Check internal consistency

- Unit tests should have no dependencies from anything (e.g., outside code is
  mocked or stubbed out)

### Integration tests
- To verify that different pieces of the system work together
- Might require resources (e.g., database, hardware)

### End-to-end tests
- To verify that the entire application behaves as expected

## Why is unit testing important?

- Good unit testing improves software quality, by:
    1) Eliminating bugs (obvious)
    2) Clarifying code design and interfaces ("Design to Test")
    3) Making refactoring safer and easier ("Refactor Early, Refactor Often")
    4) Documenting expected behavior and usage

## Pragmatic programming and unit tests

- Unit testing is an integral part of pragmatic programming philosophy:
    - Some of the tips that relate to it are:
        - Design with Contracts
        - Refactor Early, Refactor Often
        - Test Your Software, or Your Users Will
        - Coding Ain’t Done Till All the Tests Run
        - Test State Coverage, Not Code Coverage
        - You Can’t Write Perfect Software
        - Crash Early
        - Design to Test
        - Test Early. Test Often. Test Automatically.
        - Use Saboteurs to Test Your Testing
        - Find Bugs Once.

## Unit testing tips

### Unit Testing Tip #1: Test one thing

- A good unit test tests only one thing
- Testing one thing keeps the unit test simple, relatively easy to understand,
  and helps isolate the root cause when the test fails
- How do we test more than one thing?
    - By having more than one unit test! 

### Unit Testing Tip #2: Keep tests self-contained

- A unit test should be independent of all other unit tests
    - Each test should be self-sufficient
    - One should never assume that unit tests will be executed in a particular
      order
- A corollary of keeping tests self-contained is to keep all information needed
  to understand the test within the test itself
    - In other words, when possible, avoid calling helper functions to load data
      or state to initialize the test; instead, specify the data explicitly in
      the test where it is used
    - This makes the test easier to understand and easier to debug when it fails
    - If multiple unit tests use or can use the same initialization data, do not
      hesitate repeating it in each test (or consider using parameterized
      testing)

### Unit Testing Tip #3: Only specify data related to what is being tested

- If a function that is being tested supports optional arguments, but those
  optional arguments are not needed for a particular unit test, then do not
  specify them in the test
- Specify the minimum of what is required to test what is being tested

### 8.2.4. Unit Testing Tip #4: Test realistic corner cases

- Can your function receive a list that is empty?
- Can it return an empty Series?
- What happens if it receives a numerical value outside of an expected range?
- How should the function behave in those cases?
    - Should it crash?
    - Should it return a reasonable default value?
- Expect these questions to come up in practice and think through what the
  appropriate behavior should be
    - Then, test for it

### 8.2.5. Unit Testing Tip #5: Test a `typical` scenario

- In ensuring that corner cases are covered, do not overlook testing basic
  functionality for typical cases
- This is useful for verifying current behavior and to support refactoring

## 8.3. Conventions

### 8.3.1. Naming and placement conventions

- We follow the convention that is the default to pytest

1. A directory `test` contains all the test code and artifacts
    - A unit test file should be close to the library / code it tests
    - The `test` directory contains all the `test_*.py` python files and all
      inputs and outputs for the tests

2. File names are like `test_*.py`
    - It should also be named according to the library it tests
    - E.g., if the library to test is called `pnl.py`, then the corresponding
      unit test should be called `test_pnl.py`

3. The test class should make clear reference to the code that it is tested.
    - To test a function `generate_early8k_html_tables()` the corresponding test
      class is `Test_generate_early8k_html_tables`. This is ok even if it’s not a
      PEP8 compatible class name (i.e., CamelCase)
    - To test a class `FooBar` the corresponding test class is named `TestFooBar`
    - You can add a number, e.g.,` TestFooBar1()`, if there are multiple test
      classes that are testing the code in different ways (e.g.,
      `setUp(), tearDown()` are different)

- It’s ok to have multiple test methods, e.g., for `FooBar.method_a()` and`
  .method_b()`, the test method is:
    ```python
    class TestFooBar1(ut.TestCase):

       def test_method_a(self):
           ...

       def test_method_b(self): 
           ...
    ```

- Split test classes and methods in a reasonable way, so each one tests one
  single thing in the simplest possible way

- Remember that test code is not second class citizen, although it’s auxiliary to
  the code
    - Add comments and docstring explaining what the code is doing
    - If you change the name of a class, also the test should be changed
    - If you change the name of a file, also the name of the file with the
      testing code should be changed

### Framework to test using input / output data

- helpers/unit_test.py has some utilities to easily create input and output dirs storing data for unit tests.

- The directory structure enforced by the out TestCase is like
    ```bash
    > tree -d edg/form_8/test/
    edg/form_8/test/
    └── TestExtractTables1.test1
        ├── input
        └── output
    ```




### 8.3.3. Skeleton for unit test {#8-3-3-skeleton-for-unit-test}

The most updated unit tests are in edg/form_8/test/test_table_normalizer.py

A unit test looks like:


```
import helpers.unit_test as hut

class Test...(hut.TestCase):

    def test...(self):
          ...
```


You don’t need


```
if __name__ == '__main__':
    unittest.main()
```


since pytest will take care of that.


### Use the appropriate `self.assert*`

- Use the proper `self.assert*()` to get as much information as possible from a
  failing test
- **Bad**
    ```python
    self.assertTrue(a < b)
    ```
- **Good**
    ```python
    self.assertLess(a, b)
    ```

- When you get a failure you don’t get something like "True is not
  False", rather an informative message like "5 is not < 4"
- The same reasoning holds also for `dbg.dassert*()` which should be modeled
  after the unit test routines

- List of useful testing functions are:
    * [General python](https://docs.python.org/2/library/unittest.html#test-cases)
    * [Numpy](https://docs.scipy.org/doc/numpy-1.15.0/reference/routines.testing.html)
    * [Pandas](https://pandas.pydata.org/pandas-docs/version/0.21/api.html#testing-functions)

### Ordering of actual, expected
- We prefer to use the following ordering of actual and expected value
    ```python
    self.assertEqual(actual, expected)
    ```
- In fact our own function for
    ```python
    self.assert_equal(self, actual, expected)
    ```

### Our unit testing framework
- TODO(gp): Complete this
- We use `pytest` as test harness
- `unittest` as unit testing
- `helpers/unittest.py` has some supporting functions to make testing even easier
    - setUp that sets the random number generator to avoid unexpected changes
    - assert_equal
    - fuzzy assert

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

### Tweaking the logging behavior
- To tweak the logging behavior you can add `--log-level=DEBUG -s`

- Even better is the option --log-cli-level=DEBUG aka "live log" which writes the
  log on the fly

### Updating the outcomes
- If you want to update the expected values you can do
    ```bash
    > pytest XYZ.py --update_outcomes
    ```
