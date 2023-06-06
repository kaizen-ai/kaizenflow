# Running unit tests

- Before any PR (and ideally after every commit) we want to run all the unit
  tests to make sure we didn't introduce no new bugs
- We use `pytest` and `unittest` as testing framework
- We have different test set lists:
  - `fast`
    - Tests that are quick to execute (typically &lt; 5 secs per test class)
    - We want to run these tests before / after every commit / PR to make sure
      things are not horrible broken
  - `slow`
    - Tests that we don't want to run all the times because they are:
      - Slow (typically &lt; 20 seconds per test)
      - Related to pieces of code that don't change often
      - External APIs we don't want to hit continuously
  - `superslow`
    - Tests that run long workload, e.g., running a production model

## Using invoke

[`invoke`](https://www.pyinvoke.org/) is a task execution which allows to
execute some typical workflows, e.g. run the tests

# Run only fast tests:

> i run_fast_tests

# Run only slow tests:

> i run_slow_tests

# Run only superslow tests:

> i run_superslow_tests

```
To see the options use `--help` option, e.g. `i --help run_fast_tests`:

Usage: inv[oke] [--core-opts] run_fast_tests [--options] [other tasks here ...]

Docstring:
  Run fast tests.
  :param stage: select a specific stage for the Docker image
  :param pytest_opts: option for pytest
  :param skip_submodules: ignore all the dir inside a submodule
  :param coverage: enable coverage computation
  :param collect_only: do not run tests but show what will be executed
  :param tee_to_file: save output of pytest in `tmp.pytest.log`
  :param kwargs: kwargs for `ctx.run`

Options:
  -c, --coverage
  -k, --skip-submodules
  -o, --collect-only
  -p STRING, --pytest-opts=STRING
  -s STRING, --stage=STRING
  -t, --tee-to-file
  -v STRING, --version=STRING
```

### Docker image stage and version

To select a specific stage for Docker image use the `--stage` option. E.g., this
might be useful when a user wants to run regressions on the local Docker image
to verify that nothing is broken before promoting it to `dev` image.

> i run_fast_tests --stage local

To run the tests on the specific version of a Docker image, use the `--version`
option. E.g., this might be useful when releasing a new version of an image.

> i run_fast_tests --stage local --version 1.0.4

### Specifying pytest options

With the option `--pytest-opts` it is possible to pass any `pytest` option to
`invoke`. E.g., if a user want to run the tests in the debug mode to show the
output

> i run_fast_tests -s --dbg

### Save test output to file

To save the output of `pytest` to `tmp.pytest.log` use the `--tee-to-file`
option.

> i run_fast_tests --tee-to-file

### Show the tests but do not run

To show (but not run) the tests that will be executed, use the `--collect-only`.

> i run_fast_test --collect-only

### Skip submodules

To skip running tests in submodules use the `--skip-submodules` option. This is
useful for repos with submodules, e.g., `dev_tools` where `cmamp` is a
submodule. Using this option, only tests in `dev_tools` but not in `cmamp` are
run

> cd dev_tools1

> i run_fast_tests --skip-submodules

### Compute tests coverage

To compute tests coverage use the `--coverage` option

> i run_fast_tests --coverage

## Timeout

- We use the [pytest-timeout](https://pypi.org/project/pytest-timeout/) package
  to limit durations of fast, slow, and superslow tests
- The timeout durations for each test type are listed
  [here](#running-unit-tests)
- The timeout restricts only running time of the test method, not `setUp` and
  `tearDown` time

## Rerunning timeout-ed tests

- Running tests can take different amount of time depending on workload and
  machine
- Because of this, we rerun failing tests using
  [pytest-rerunfailures](https://pypi.org/project/pytest-rerunfailures/)
- pytest-rerunfailures is not completely compatible with pytest-timeout. This is
  why we have to add the `-o timeout_func_only=true` flag to pytest-timeout. See
  [https://github.com/pytest-dev/pytest-rerunfailures/issues/99](https://github.com/pytest-dev/pytest-rerunfailures/issues/99)` `for
  more information
- We rerun timeouted fast tests twice and timeouted slow and superslow tests
  once
- There is a
  [way](https://pypi.org/project/pytest-rerunfailures/#re-run-individual-failures)
  to provide a rerun delay for individual tests. However, we can’t use it for
  now due to
  [#693 (comment)](https://github.com/cryptokaizen/cmamp/issues/693#issuecomment-989456031)

## Compute tests coverage

The documentation for `coverage` is
[here](https://coverage.readthedocs.io/en/latest/cmd.html#reporting).

Run a set of unit tests enabling coverage:

# Run the coverage for a single test:

> i run_fast_tests --coverage -p oms/test/test_broker.py::TestSimulatedBroker1

# Run coverage for an entire module like `oms`:

> i run_fast_tests --coverage -p oms

This generates and run a pytest command inside Docker like:

docker

> /venv/bin/pytest -m "not slow and not superslow"
> oms/test/test_broker.py::TestSimulatedBroker1 --cov=. --cov-branch
> --cov-report term-missing --cov-report html

which generates:

- a default coverage report
- a binary `.coverage` file that contains the coverage information
- an `htmlcov` dir with a browsable code output to inspect the coverage for the
  files

One can post-process the coverage report in different ways using the command
`coverage` inside a docker container, since the code was run (as always) inside
the Docker container that contains all the dependencies.

> coverage -h

Coverage.py, version 5.5 with C extension Measure, collect, and report on code
coverage in Python programs.

usage: coverage <command> [options] [args]

Commands: annotate Annotate source files with execution information. combine
Combine a number of data files. debug Display information about the internals of
coverage.py erase Erase previously collected coverage data. help Get help on
using coverage.py. html Create an HTML report. json Create a JSON report of
coverage results. report Report coverage stats on modules. run Run a Python
program and measure code execution. xml Create an XML report of coverage
results.

Use "coverage help <command>" for detailed help on any command. Full
documentation is at https://coverage.readthedocs.io

> coverage report -h

Usage: coverage report [options] [modules]

Report coverage statistics on modules.

```
Options:
  --contexts=REGEX1,REGEX2,...
                        Only display data from lines covered in the given
                        contexts. Accepts Python regexes, which must be
                        quoted.
  --fail-under=MIN      Exit with a status of 2 if the total coverage is less
                        than MIN.
  -i, --ignore-errors   Ignore errors while reading source files.
  --include=PAT1,PAT2,...
                        Include only files whose paths match one of these
                        patterns. Accepts shell-style wildcards, which must be
                        quoted.
  --omit=PAT1,PAT2,...  Omit files whose paths match one of these patterns.
                        Accepts shell-style wildcards, which must be quoted.
  --precision=N         Number of digits after the decimal point to display
                        for reported coverage percentages.
  --sort=COLUMN         Sort the report by the named column: name, stmts,
                        miss, branch, brpart, or cover. Default is name.
  -m, --show-missing    Show line numbers of statements in each module that
                        weren't executed.
  --skip-covered        Skip files with 100% coverage.
  --no-skip-covered     Disable --skip-covered.
  --skip-empty          Skip files with no code.
  --debug=OPTS          Debug options, separated by commas. [env:
                        COVERAGE_DEBUG]
  -h, --help            Get help on this command.
  --rcfile=RCFILE       Specify configuration file. By default '.coveragerc',
                        'setup.cfg', 'tox.ini', and 'pyproject.toml' are
                        tried. [env: COVERAGE_RCFILE]
```

> i docker_bash

```
# Report the coverage for all the files under `oms` using the workload above (i.e., the fast tests under `oms/test/test_broker.py::TestSimulatedBroker1`)
docker> coverage report --include="oms/*"

Name                                    Stmts   Miss Branch BrPart  Cover
-------------------------------------------------------------------------
oms/__init__.py                             0      0      0      0   100%
oms/api.py                                154     47     36      2    70%
oms/broker.py                             200     31     50      9    81%
oms/broker_example.py                      23      0      4      1    96%
oms/call_optimizer.py                      31      0      0      0   100%
oms/devops/__init__.py                      0      0      0      0   100%
oms/devops/docker_scripts/__init__.py       0      0      0      0   100%
oms/locates.py                              7      7      2      0     0%
oms/mr_market.py                           55      1     10      1    97%
oms/oms_db.py                              47      0     10      3    95%
oms/oms_lib_tasks.py                       64     39      2      0    38%
oms/oms_utils.py                           34     34      6      0     0%
oms/order.py                              101     30     22      0    64%
oms/order_example.py                       26      0      0      0   100%
oms/place_orders.py                       121      8     18      6    90%
oms/pnl_simulator.py                      326     42     68      8    83%
oms/portfolio.py                          309     21     22      0    92%
oms/portfolio_example.py                   32      0      0      0   100%
oms/tasks.py                                3      3      0      0     0%
oms/test/oms_db_helper.py                  29     11      2      0    65%
oms/test/test_api.py                      132     25     12      0    83%
oms/test/test_broker.py                    33      5      4      0    86%
oms/test/test_mocked_portfolio.py           0      0      0      0   100%
oms/test/test_mr_market.py                 46      0      2      0   100%
oms/test/test_oms_db.py                   114     75     14      0    38%
oms/test/test_order.py                     24      0      4      0   100%
oms/test/test_place_orders.py              77      0      4      0   100%
oms/test/test_pnl_simulator.py            235      6     16      0    98%
oms/test/test_portfolio.py                135      0      6      0   100%
-------------------------------------------------------------------------
TOTAL                                    2358    385    314     30    82%
```

To exclude the test files, which could inflate the coverage

> coverage report --include="oms/_" --omit="_/test\_\*.py"

```
Name                                    Stmts   Miss Branch BrPart  Cover
-------------------------------------------------------------------------
oms/__init__.py                             0      0      0      0   100%
oms/api.py                                154     47     36      2    70%
oms/broker.py                             200     31     50      9    81%
oms/broker_example.py                      23      0      4      1    96%
oms/call_optimizer.py                      31      0      0      0   100%
oms/devops/__init__.py                      0      0      0      0   100%
oms/devops/docker_scripts/__init__.py       0      0      0      0   100%
oms/locates.py                              7      7      2      0     0%
oms/mr_market.py                           55      1     10      1    97%
oms/oms_db.py                              47      0     10      3    95%
oms/oms_lib_tasks.py                       64     39      2      0    38%
oms/oms_utils.py                           34     34      6      0     0%
oms/order.py                              101     30     22      0    64%
oms/order_example.py                       26      0      0      0   100%
oms/place_orders.py                       121      8     18      6    90%
oms/pnl_simulator.py                      326     42     68      8    83%
oms/portfolio.py                          309     21     22      0    92%
oms/portfolio_example.py                   32      0      0      0   100%
oms/tasks.py                                3      3      0      0     0%
oms/test/oms_db_helper.py                  29     11      2      0    65%
-------------------------------------------------------------------------
TOTAL                                    1562    274    252     30    80%
```

To open the line coverage, from outside Docker go with your browser to
`htmlcov/index.html`. The `htmlcov` is re-written with every coverage run with
the `--cov-report html` option. If you move out `index.html` from `htmlcov` dir
some html features (e.g., filtering) will not work.

```
# On macOS:
> open htmlcov/index.html
```

![alt_text](python_unit_tests_figs/image_1.png)

By clicking on a file you can see which lines are not covered

![alt_text](python_unit_tests_figs/image_2.png)

### An example coverage session

We want to measure the unit test coverage of `oms` component from both fast and
slow tests

We start by running the fast tests:

```
# Run fast unit tests
> i run_fast_tests --coverage -p oms
collected 66 items / 7 deselected / 59 selected

# Compute the coverage for the module sorting by coverage
docker> coverage report --include="oms/*" --omit="*/test_*.py" --sort=Cover

Name                                    Stmts   Miss Branch BrPart  Cover
-------------------------------------------------------------------------
oms/locates.py                              7      7      2      0     0%
oms/oms_utils.py                           34     34      6      0     0%
oms/tasks.py                                3      3      0      0     0%
oms/oms_lib_tasks.py                       64     39      2      0    38%
oms/order.py                              101     30     22      0    64%
oms/test/oms_db_helper.py                  29     11      2      0    65%
oms/api.py                                154     47     36      2    70%
oms/broker.py                             200     31     50      9    81%
oms/pnl_simulator.py                      326     42     68      8    83%
oms/place_orders.py                       121      8     18      6    90%
oms/portfolio.py                          309     21     22      0    92%
oms/oms_db.py                              47      0     10      3    95%
oms/broker_example.py                      23      0      4      1    96%
oms/mr_market.py                           55      1     10      1    97%
oms/__init__.py                             0      0      0      0   100%
oms/call_optimizer.py                      31      0      0      0   100%
oms/devops/__init__.py                      0      0      0      0   100%
oms/devops/docker_scripts/__init__.py       0      0      0      0   100%
oms/order_example.py                       26      0      0      0   100%
oms/portfolio_example.py                   32      0      0      0   100%
-------------------------------------------------------------------------
TOTAL                                    1562    274    252     30    80%
```

We see that certain files have a low coverage, so we want to see what is not
covered.

Generate the same report in a browsable format

```
docker> rm -rf htmlcov; coverage html --include="oms/*" --omit="*/test_*.py"
# Wrote HTML report to `htmlcov/index.html`

> open htmlcov/index.html
```

The low coverage for `tasks.py` and `oms_lib_tasks.py` is due to the fact that
we are running code through invoke that doesn't allow `coverage` to track it.

Now we run the coverage for the slow tests

```
# Save the coverage from the fast tests run
> cp .coverage .coverage_fast_tests

> i run_slow_tests --coverage -p oms
collected 66 items / 59 deselected / 7 selected

> cp .coverage .coverage_slow_tests

> coverage report --include="oms/*" --omit="*/test_*.py" --sort=Cover

Name                                    Stmts   Miss Branch BrPart  Cover
-------------------------------------------------------------------------
oms/locates.py                              7      7      2      0     0%

oms/oms_utils.py                           34     34      6      0     0%

oms/tasks.py                                3      3      0      0     0%

oms/pnl_simulator.py                      326    280     68      1    13%

oms/place_orders.py                       121    100     18      0    15%

oms/mr_market.py                           55     44     10      0    17%

oms/portfolio.py                          309    256     22      0    18%

oms/call_optimizer.py                      31     25      0      0    19%

oms/broker.py                             200    159     50      0    20%

oms/order.py                              101     78     22      0    20%

oms/order_example.py                       26     19      0      0    27%

oms/broker_example.py                      23     14      4      0    33%

oms/portfolio_example.py                   32     21      0      0    34%

oms/api.py                                154    107     36      0    36%

oms/oms_lib_tasks.py                       64     39      2      0    38%

oms/oms_db.py                              47      5     10      2    84%

oms/__init__.py                             0      0      0      0   100%

oms/devops/__init__.py                      0      0      0      0   100%

oms/devops/docker_scripts/__init__.py       0      0      0      0   100%

oms/test/oms_db_helper.py                  29      0      2      0   100%

-------------------------------------------------------------------------

TOTAL                                    1562   1191    252      3    23%
```

We see that the coverage from the slow tests is only 23% for 7 tests

```
root@6faaa979072e:/app/amp# coverage combine .coverage_fast_tests .coverage_slow_tests

Combined data file .coverage_fast_tests

Combined data file .coverage_slow_tests
```

### An example with customized pytest-cov html run

We want to measure unit test coverage specifically for one test in
`im_v2/common/data/transform/` and to save generated htmlcov in the same
directory.

Run command below after `i docker_bash`:

```
pytest --cov-report term-missing
--cov=im_v2/common/data/transform/ im_v2/common/data/transform/test/test_transform_utils.py
--cov-report html:im_v2/common/data/transform/htmlcov \
```

Output sample:

```
---------- coverage: platform linux, python 3.8.10-final-0 ----------- Name Stmts Miss Cover Missing ----------------------------------------------------------------------------------------------- im_v2/common/data/transform/convert_csv_to_pq.py 55 55 0% 2-159 im_v2/common/data/transform/extract_data_from_db.py 55 55 0% 2-125 im_v2/common/data/transform/pq_convert.py 126 126 0% 3-248 im_v2/common/data/transform/transform_pq_by_date_to_by_asset.py 131 131 0% 2-437 im_v2/common/data/transform/transform_utils.py 22 0 100% ----------------------------------------------------------------------------------------------- TOTAL 389 367 6% Coverage HTML written to dir im_v2/common/data/transform/htmlcov \
```

### Generate coverage report with `invoke`

One can compute test coverage for a specified directory and generate text and
HTML reports automatically using `invoke task run_coverage_report`

```
> i --help run_coverage_report
INFO: > cmd='/data/grisha/src/venv/amp.client_venv/bin/invoke --help run_coverage_report'

Usage: inv[oke] [--core-opts] run_coverage_report [--options] [other tasks here ...]

Docstring:

  Compute test coverage stats.
```

The flow is:

- Run tests and compute coverage stats for each test type

- Combine coverage stats in a single file

- Generate a text report

- Generate a HTML report (optional)

- Post it on S3 (optional)

```
:param target_dir: directory to compute coverage stats for [here](#running-unit-tests)

:param generate_html_report: whether to generate HTML coverage report or not

:param publish_html_on_s3: whether to publish HTML coverage report or not

:param aws_profile: the AWS profile to use for publishing HTML report

Options:
  -a STRING, --aws-profile=STRING

  -g, --[no-]generate-html-report

  -p, --[no-]publish-html-on-s3

  -t STRING, --target-dir=STRING
```

### Common usage

Compute coverage for `market_data` dir, generate text and HTML reports and
publish HTML report on S3

> i run_coverage_report --target-dir market_data

```
Name                                   Stmts   Miss Branch BrPart  Cover
------------------------------------------------------------------------
market_data/real_time_market_data.py     100     81     32      0    16%

market_data/replayed_market_data.py      111     88     24      0    19%

market_data/abstract_market_data.py      177    141     24      0    19%

market_data/market_data_example.py       124     97     10      0    20%

market_data/market_data_im_client.py      66     50     18      0    21%

market_data/__init__.py                    5      0      0      0   100%

------------------------------------------------------------------------

TOTAL                                    583    457    108      0    19%
Wrote HTML report to htmlcov/index.html

20:08:53 - INFO  lib_tasks.py _publish_html_coverage_report_on_s3:3679  HTML coverage report is published on S3: path=`s3://cryptokaizen-html/html_coverage/grisha_CmTask1038_Tool_to_extract_the_dependency_from_a_project`
```

### Publishing HTML report on S3

- To make a dir with the report unique linux user and Git branch name are used,
  e.g.,
  `html_coverage/grisha_CmTask1038_Tool_to_extract_the_dependency_from_a_project`
  - `html_coverage` is the common dir on S3 for coverage reports
- After publishing the report, one can easily open it via a local web browser
  - See the details in
  - E.g.
    [http://172.30.2.44/html_coverage/grisha_CmTask1038_Tool_to_extract_the_dependency_from_a_project/](http://172.30.2.44/html_coverage/grisha_CmTask1038_Tool_to_extract_the_dependency_from_a_project/)

# Running `pytest` directly

## Usage and Invocations reference

See [pytest documentation](http://doc.pytest.org/en/latest/usage.html)

Some examples of useful command lines:

```
# Stop at first failure
> pytest -x

# Run a single class
> pytest -k TestPcaFactorComputer1

# Run a single test method
> pytest core/test/test_core.py::TestPcaFactorComputer1::test_linearize_eigval_eigvec

# Remove cache artifacts

> find . -name "__pycache__" -o -name ".pytest_cache"

./.pytest_cache

./dev_scripts/test/Test_linter_py1.test_linter1/tmp.scratch/__pycache__

./dev_scripts/test/__pycache__

./dev_scripts/__pycache__

> find . -name "__pycache__" -o -name ".pytest_cache" | xargs rm -rf

# Run with a clear cache

> pytest --cache-clear

# Run the tests that last failed (this data is stored in .pytest_cache/v/cache/lastfailed)

> pytest --last-failed
```

## Custom pytest options behaviors

### Enable logging

To enable logging of `_LOG.debug` for a single test run:

```
# Enable debug info

> pytest oms/test/test_broker.py::TestSimulatedBroker1 -s --dbg
```

### Update golden outcomes

This switch allows to overwrite the golden outcomes that are used as reference
in the unit tests to detect failures

```
> pytest --update_outcomes
```

### Incremental test mode

This switch allows to reuse artifacts in the test directory and to skip the
clean up phase

It is used to re-run tests from the middle when they are very long and one wants
to debug them

```
> pytest --incremental
```

# Running tests on GH Actions

The official documentation is
[https://docs.github.com/en/actions](https://docs.github.com/en/actions)

## How to run a single test on GH Action

Unfortunately there is no way to log in and run interactively on GH machines.
This is a feature requested but not implemented by GH yet.

All the code to run GH Actions is in the `.github` directory in `lemonade` and
`amp`.

E.g., to run a single test in the fast test target, instead of the entire
regression suite

1. you can modify `.github/workflows/fast_tests.yml`, by replacing

   ```
   # run: invoke run_fast_tests

   run: invoke run_fast_tests --pytest-opts="helpers/test/test_git.py::Test_git_modified_files1::test_get_modified_files_in_branch1 -s --dbg"
   ```

- Note that the indentation matters, since it's a YAML file

  ![alt_text](python_unit_tests_figs/image_3.png)

- The `-s --dbg` is to show `_LOG.debug` in case you care about that to get more
  information

2. Commit the code to your branch (not in master please) since GH runs each
   branch independently
3. Kick off manually the fast test through the GH interface
4. After debugging, you can revert the change from your branch to `master` and
   move along with the usual PR flow

# Guidelines about writing unit tests

### **What is a unit test?**

- A unit test is a small, self-contained test of a public function or public
  method of a library
- The test specifies the given inputs, any necessary state, and the expected
  output
- Running the test ensures that the actual output agrees with the expected
  output

### **Why is unit testing important?**

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
  - Find Bugs Once
- Good unit testing improves software quality. It does this in part by
  - Eliminating bugs (obvious)
  - Clarifying code design and interfaces ("Design to Test")
  - Making refactoring safer and easier ("Refactor Early, Refactor Often")
  - Documenting expected behavior and usage

## Unit testing tips

### Tip #1: Test one thing

- A good unit test tests only one thing
- Testing one thing keeps the unit test simple, relatively easy to understand,
  and helps isolate the root cause when the test fails
- How do we test more than one thing? By having more than one unit test!

### Tip #2: Keep tests self-contained

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

### Tip #3: Only specify data related to what is being tested

- If a function that is being tested supports optional arguments, but those
  optional arguments are not needed for a particular unit test, then do not
  specify them in the test
- Specify the minimum of what is required to test what is being tested.

### Tip #4: Test realistic corner cases

- Can your function receive a list that is empty?
- Can it return an empty Series?
- What happens if it receives a numerical value outside of an expected range?
- How should the function behave in those cases? Should it crash? Should it
  return a reasonable default value?
- Expect these questions to come up in practice and think through what the
  appropriate behavior should be. Then, test for it.

### Tip #5: Test a typical scenario

- In ensuring that corner cases are covered, do not overlook testing basic
  functionality for typical cases
- This is useful for verifying current behavior and to support refactoring.

### Tip #6: Test executable scripts end-to-end

- In some cases, like scripts, it is easy to get lost chasing the coverage %
  - e.g. covering every line of the original, including the parser
- This is not always necessary
  - if you are able to run a script with all arguments present, it means that
    the parser works correctly
  - so an end-to-end smoke test will also cover the parser
  - this saves a little time and reduces the bloat
- If you need to test the functionality, consider factoring out as much code as
  possible from `_main`
  - a good practice is to have a `_run` function that does all the job and
    `_main` only brings together the parser and the executable part

## Conventions

### Naming and placement conventions

- We follow the convention (that happen to be mostly the default to `pytest`):
- A directory `test` that contains all the test code and artifacts
  - The `test` directory contains all the `test_*.py` files and all inputs and
    outputs for the tests.
  - A unit test file should be close to the library / code it tests
- The test class should make clear reference to the code that is tested
  - To test a class `FooBar` the corresponding test class is named
    `TestFooBar,`we use the CamelCase for the test classes
  - You can add a number, e.g., `TestFooBar1()`, if there are multiple test
    classes that are testing the code in different ways (e.g.,`setUp()`
    `tearDown()` are different)
  - To test a function `generate_html_tables()` the corresponding test class is
    `TestGenerateHtmlTables`
- It's ok to have multiple test methods, e.g., for `FooBar.method_a()` and
  `FooBar.method_b()`, the test method is: \
  ```
  class TestFooBar1(unittest2.TestCase):
      def test_method_a(self):
          ...
      def test_method_b(self):
          ...
  ```
- Split test classes and methods in a reasonable way, so each one tests one
  single thing in the simplest possible way
- Remember that test code is not second class citizen, although it's auxiliary
  to the code
  - Add comments and docstring explaining what the code is doing
  - If you change the name of a class, also the test should be changed
  - If you change the name of a file also the name of the file with the testing
    code should be changed

### Our framework to test using input / output data

- `helpers/unit_test.py` has some utilities to easily create input and output
  dirs storing data for unit tests
- `hut.TestCase` has various methods to help you create
  - `get_input_dir`: return the name of the dir used to store the inputs
  - `get_scratch_space`: return the name of a scratch dir to keep artifacts of
    the test
  - `get_output_dir`: probably not interesting for the user
- The directory structure enforced by the out `TestCase` is like:

  ```
  > tree -d edg/form_8/test/
  edg/form_8/test/
  └── TestExtractTables1.test1
      ├── input
      └── output
  ```

- The layout of test dir:
  ```
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

### check_string vs self.assertEqual

- TODO(gp): Add

### Use self.assert_equal

- This is a function that helps you understand what are the mismatches
- It works on `str`

### How to split unit test code in files

- The two extreme approaches are:
  - All the test code for a directory goes in one file
    `foo/bar/test/test_$DIRNAME.py` (or `foo/bar/test/test_all.py`)
  - Each file `foo/bar/$FILENAME` with code gets its corresponding
    `foo/bar/test/test_$FILENAME.py`
    - It should also be named according to the library it tests
    - For example, if the library to test is called `pnl.py`, then the
      corresponding unit test should be called `test_pnl.py`
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
- Compromise solution: _ Start with a single file
  `test_$DIRNAME.py` (or
  `test_dir_name.py`) _ In the large file add a framed comment like: \
  `# ##################'\ '# Unit tests for file: … '\ '# ##################' * So it's easy to find which file is tested were using grep * Then split when it becomes too big using `test\_$FILENAME.py`

### Skeleton for unit test

- Interesting unit tests are in `helpers/test`
- A unit test looks like:
  ```
  import helpers.unit_test as hut
  class Test...(hut.TestCase):
      def test...(self):
          ...
  ```
- `pytest` will take care of running the code so you don't need: \
  ```
  if __name__ == '__main__':
  unittest.main()
  ```

### Hierarchical `TestCase` approach

- Whenever there is hierarchy in classes, we also create a hierarchy of test
  classes
- A parent test class looks like:
  ```
  import helpers.unit_test as hut \
  class SomeClientTestCase(hut.TestCase):
      def _test...1(self):
          ...
      def _test...2(self):
          ...
  ```
- While a child test class looks like this where test methods use the
  corresponding methods from the parent test class:
  ```
  class TestSomeClient(SomeClientTestCase):
      def test...1(self):
          ...
      def test...2(self):
          ...
  ```
- Each TestCase tests a "behavior" like a set of related methods
- Each TestCase is under the test dir
- Each derived class should use the proper `TestCase` classes to reach a decent
  coverage
- It is OK to use non-private methods in test classes to ensure that the code is
  in order of dependency, so that the reader doesn't have to jump back / forth
- We want to separate chunks of unit test code using:
  `# ////////////////////////////////////////////////////////////////////////`
  putting all the methods used by that chunk at the beginning and so on

- It is OK to skip a `TestCase` method if not meaningful, when coverage is
  enough
- As an example, see `im_v2/common/data/client/test/im_client_test_case.py` and
  `im_v2/ccxt/data/client/test/test_ccxt_clients.py`

### Use the appropriate `self.assert*`

- When you get a failure you don't want to get something like "True is not
  False", rather an informative message like "5 is not &lt; 4"
- Bad \
  `self.assertTrue(a &lt; b)`
- Good \
  `self.assertLess(a, b)`

### Do not use hdbg.dassert

- `dassert` are for checking self-consistency of the code
- The invariant is that you can remove `dbg.dassert` without changing the
  behavior of the code. Of course you can't remove the assertion and get unit
  tests to work

### Interesting testing functions

List of useful testing functions are:

- [General python](https://docs.python.org/2/library/unittest.html#test-cases)
- [Numpy](https://docs.scipy.org/doc/numpy-1.15.0/reference/routines.testing.html)
- [Pandas](https://pandas.pydata.org/pandas-docs/version/0.21/api.html#testing-functions)

### Use setUp/tearDown

- If you have a lot of repeated code in your tests, you can make them shorter by
  moving this code to `setUp/tearDown `methods: _ `setUp()` \
  Method called to prepare the test fixture. This is called immediately before calling
  the test method; other than <code>[AssertionError](https://docs.python.org/3/library/exceptions.html#AssertionError)</code>
  or <code>[SkipTest](https://docs.python.org/3/library/unittest.html#unittest.SkipTest)</code>,
  any exception raised by this method will be considered an error rather than a test
  failure. The default implementation does nothing. _ <code>tearDown()</code> \
  Method called immediately after the test method has been called and the result
  recorded. This is called even if the test method raised an exception, so the
  implementation in subclasses may need to be particularly careful about
  checking internal state. Any exception, other than
  <code>[AssertionError](https://docs.python.org/3/library/exceptions.html#AssertionError)</code>
  or
  <code>[SkipTest](https://docs.python.org/3/library/unittest.html#unittest.SkipTest)</code>,
  raised by this method will be considered an additional error rather than a
  test failure (thus increasing the total number of reported errors). This
  method will only be called if the
  <code>[setUp()](https://docs.python.org/3/library/unittest.html#unittest.TestCase.setUp)</code>
  succeeds, regardless of the outcome of the test method. The default
  implementation does nothing.
- If you need some expensive code parts to be done once for the whole test
  class, such as opening a database connection, opening a temporary file on the
  filesystem, loading a shared library for testing, etc., you can use
  <code>setUpClass/tearDownClass </code>methods: \* <code>setUpClass()</code> \
  A class method called before tests in an individual class are run. <code>setUpClass</code>
  is called with the class as the only argument and must be decorated as a [classmethod()](https://docs.python.org/3/library/functions.html#classmethod):

          ```
          @classmethod
          def setUpClass(cls):
              ...
          ```
      * `tearDownClass()`\

  A class method called after tests in an individual class have run.
  `tearDownClass` is called with the class as the only argument and must be
  decorated as a<span style="text-decoration:underline;">
  [classmethod()](https://docs.python.org/3/library/functions.html#classmethod)</span>:

          ```
          @classmethod
          def tearDownClass(cls):
              ...
          ```

- For more information see
  [official unittest docs](https://docs.python.org/3/library/unittest.html)

# Update test tags

- There are 2 files with the list of tests' tags:
  - `amp/pytest.ini`
  - `.../pytest.ini (if `amp` is a submodule)`
- In order to update the tags (do it in the both files):
  - In the `markers` section add a name of a new tag
  - After a `:` add a short description
  - Keep tags in the alphabetical order

# Mocking

## Refs

- Introductory article is
  [https://realpython.com/python-mock-library/ ](https://realpython.com/python-mock-library/)
- Official Python documentation for the mock package can be seen here
  [unit test mock](https://docs.python.org/3/library/unittest.mock.html)

## Common usage samples

Best to apply on any part that is deemed unnecessary for specific test

- complex functions
  - mocked functions can be tested separately
- 3rd party provider calls
  - CCXT
  - Talos
  - AWS
    - S3
      - see `helpers/hmoto.py` in `cmamp` repo
    - secrets
    - etc...
- DB calls

There are many more possible combinations that can be seen in official
documentation. \
Below are the most common ones for basic understanding.

## Philosophy about mocking

- We want to mock the minimal surface of a class
- E.g., assume there is a class that is interfacing with an external provider
  and our code places requests and get values back
- We want to replace the provider with an object that responds to the requests
  with the actual response of the provider
- In this way we can leave all the code of our class untouched and tested
- We want to test public methods of our class (and a few private methods)
- In other words, we want to test the end-to-end behavior and not how things are
  achieved
- Rationale: if we start testing "how" things are done and not "what" is done,
  we can't change how we do things (even if it doesn't affect the interface and
  its behavior), without updating tons of methods
- We want to test the minimal amount of behavior that enforces what we care
  about

## Some general suggestions about testing

1. Test from the outside-in
   - We want to start testing from the end-to-end methods towards the
     constructor of an object
   - Rationale: often we start testing very carefully the constructor and then
     we get tired / run out of time when we finally get to test the actual
     behavior
   - Also testing the important behavior automatically tests building the
     objects
   - Use the code coverage to see what's left to test once you have tested the
     "most external" code
2. We don't need to test all the assertions
   - E.g., testing carefully that we can't pass a value to a constructor doesn't
     really test much besides the fact that `dassert` works (which surprisingly
     works!)
   - We don't care about line coverage or checking boxes for the sake of
     checking boxes
3. Use strings to compare output instead of data structures

   - Often it's easier to do a check like:

     ```
     # Better:
     expected = str(...)
     expected = pprint.pformat(...)

     # Worse:
     expected = ["a", "b", { ... }]
     ```

     rather than building the data structure

   - Some purists might not like this, but
     - it's much faster to use a string (which is or should be one-to-one to the
       data structure), rather than the data structure itself
       - By extension, many of the more complex data structure have a built-in
         string representation
     - it is often more readable, easier to diff (e.g., self.assertEqual vs
       self.assert_equal)
     - in case of mismatch it's easier to update the string with copy-paste
       rather than creating a data structure that matches what was created

4. Use `self.check_string()` for things that we care about not changing (or are
   too big to have as strings in the code)
   - Use `self.assert_equal()` for things that should not change (e.g., 1 + 1
     = 2)
   - When using check_string still try to add invariants that force the code to
     be correct
   - E.g., if we want to check the PnL of a model we can freeze the output with
     `check_string()` but we want to add a constraint like there are more
     timestamps than 0 to avoid the situation where we update the string to
     something malformed
5. Each test method should test a single test case
   - Rationale: we want each test to be clear, simple, fast
   - If there is repeated code we should factor it out (e.g., builders for
     objects)
6. Each test should be crystal clear on how is different from the others
   - Often you can factor out all the common logic into an helper method
   - Copy-paste is not allowed in unit tests in the same way it's not allowed in
     production code
7. In general you want to budget the time to write unit tests

   - E.g., "I'm going to spend 3 hours writing unit tests". This is going to
     help you focus on what's important to test and force you to use an
     iterative approach rather than incremental (remember the Monalisa)

     ![alt_image](python_unit_tests_figs/image_4.png)

   - Write skeleton of unit tests and ask for a review if you are not sure how /
     what to test
     - Aka "testing plan"

## Object patch with return value

```
import unittest.mock as umock
import im_v2.ccxt.data.extract.extractor as ivcdexex

@umock.patch.object(ivcdexex.hsecret, "get_secret")
def test_function_call1(self, mock_get_secret: umock.MagicMock):
    mock_get_secret.return_value = "dummy"
```

- function `get_secret` in `helpers/hsecret.py` is mocked
  - pay attention on where is `get_secret` mocked:
    - It is mocked in im_v2.ccxt.data.extract.extractor as “get_secret” is
      called there in function that is being tested
  - @umock.patch.object(hsecret, "get_secret") will not work as mocks are
    applied after all modules are loaded, hence the reason for using exact
    location
    - If we import module in test itself it will work as mock is applied
    - for modules outside of test function it is too late as they are loaded
      before mocks for test are applied
- on every call it returns string "dummy"

## Path patch with multiple return values

```
import unittest.mock as umock

@umock.patch("helpers.hsecret.get_secret")
def test_function_call1(self, mock_get_secret: umock.MagicMock):

mock_get_secret.side_effect = ["dummy", Exception]
```

- on first call, string `dummy` is returned
- on second, Exception is raised

## Ways of calling `patch` and `patch.object`

- via decorator
  ```
  @umock.patch("helpers.hsecret.get_secret")
  def test_function_call1(self, mock_get_secret: umock.MagicMock):
      pass
  ```
- in actual function

  ```
  get_secret_patch = umock.patch("helpers.hsecret.get_secret")

  get_secret_mock = get_secret_patch.start()
  ```

- this is the only approach in which you need to start/stop patch!
  - the actual mock is returned as the return value of `start()` method!
- in other two approaches start/stop is handled under the hood and we are always
  interacting with `MagicMock` object
- via `with` statement (also in function)
  ```
  with umock.patch(""helpers.hsecret.get_secret"") as get_secret_mock:
      pass
  ```
- One of the use cases for this is if we are calling a different function inside
  a function that is being mocked
- Mostly because it is easy for an eye if there are to much patches via
  decorator and we do not need to worry about reverting the patch changes as
  that is automatically done at the end of with statement

## Mock object state after test run

```
@umock.patch.object(exchange_class._exchange, "fetch_ohlcv")

def test_function_call1(self, fetch_ohlcv_mock: umock.MagicMock):
    self.assertEqual(fetch_ohlcv_mock.call_count, 1)
    actual_args = tuple(fetch_ohlcv_mock.call_args)
    expected_args = (
            ("BTC/USDT",),
            {"limit": 2, "since": 1, "timeframe": "1m"},
    )
    self.assertEqual(actual_args, expected_args)
```

- after `fetch_ohlcv` is patched, `Mock` object is passed to test
  - in this case, it is `fetch_ohlcv_mock`
- from sample we can see that function is called once

  - first value in a tuple are positional args passed to `fetch_ohlcv` function
  - second value in a tuple are keyword args passed to `fetch_ohlcv` function
  - as an alternative, `fetch_ohlcv_mock.call_args.args` and
    `fetch_ohlcv_mock.call_args.kwargs` can be called for separate results of
    args/kwargs

    ```
    self.assertEqual(fetch_ohlcv_mock.call_count, 3)

            actual_args = str(fetch_ohlcv_mock.call_args_list)
            expected_args = r"""
            [call('BTC/USDT', since=1645660800000, bar_per_iteration=500),
            call('BTC/USDT', since=1645690800000, bar_per_iteration=500),
            call('BTC/USDT', since=1645720800000, bar_per_iteration=500)]
            """
    self.assert_equal(actual_args, expected_args, fuzzy_match=True)
    ```

- in sample above, that is continuation of previous sample,
  `fetch_ohlcv_mock.call_args_list` is called that returns all calls to mocked
  function regardless of how many times it is called
- useful for verifying that args passed are changing as expected

## Mock common external calls in `hunitest.TestCase` class

```
class TestCcxtExtractor1(hunitest.TestCase):
	# Mock calls to external providers.
	get_secret_patch = umock.patch.object(ivcdexex.hsecret, "get_secret")
	ccxt_patch = umock.patch.object(ivcdexex, "ccxt", spec=ivcdexex.ccxt)

	def setUp(self) -> None:
    	super().setUp()
    	#
    	self.get_secret_mock: umock.MagicMock = self.get_secret_patch.start()
    	self.ccxt_mock: umock.MagicMock = self.ccxt_patch.start()
    	# Set dummy credentials for all tests.
    	self.get_secret_mock.return_value = {"apiKey": "test", "secret": "test"}

	def tearDown(self) -> None:
    	self.get_secret_patch.stop()
    	self.ccxt_patch.stop()
    	# Deallocate in reverse order to avoid race conditions.
       	super().tearDown()
```

- for every unit test we want to isolate external calls and replace them with
  mocks
  - this way tests are much faster and not influenced by external factors we can
    not control
  - mocking them in `setUp` will make other tests using this class simpler and
    ready out of the box
- In current sample we are mocking AWS secrets and `ccxt` library
  - `umock.patch.object` is creating `patch` object that is not yet activated
    - `patch.start()/stop()` is activating/deactivating patch for each test run
      in `setUp/tearDown`
    - `patch.start()` is returning a standard `MagicMock` object we can use to
      check various states as mentioned in previous examples and control return
      values
      - call_args, call_count, return_value, side_effect, etc.
- Note: Although patch initialization in static variables belongs to `setUp`,
  when this code is moved there patch is created for each test separately. We
  want to avoid that and only start/stop same patch for each test.

## Mocks with specs

```
# Regular mock and external library `ccxt` is replaced with `MagicMock`
@umock.patch.object(ivcdexex, "ccxt")
# Only `ccxt` is spec'd, not actual components that are "deeper" in the `ccxt` library.
@umock.patch.object(ivcdexex, "ccxt", spec=ivcdexex.ccxt)
# Everything is spec'd recursively , including returning values/instances of `ccxt`
# functions and returned values/instances of returned values/instances, etc.
@umock.patch.object(ivcdexex, "ccxt", autospec=True)
```

- first mock is not tied to any spec and we can call any attribute/function
  against the mock and the call will be memorized for inspection and the return
  value is new `MagicMock`.
  - `ccxt_mock.test(123)` returns new `MagicMock` and raises no error
- in second mock `ccxt.test(123)` would fail as such function does not exists
  - we can only call valid exchange such as `ccxt_mock.binance()` that will
    return `MagicMock`, as exchange is not part of the spec
- in third mock everything needs to be properly called
  - `ccxt_mock.binance()` will return `MagicMock` with `ccxt.Exchange` spec_id
    (in mock instance as meta)
    - as newly `exchange` instance is with spec, we can only call real
      functions/attributes of `ccxt.Exchange` class

## Caveats

```
# `datetime.now` cannot be patched directly, as it is a built-in method.
# Error: "can't set attributes of built-in/extension type 'datetime.datetime'"

datetime_patch = umock.patch.object(imvcdeexut, "datetime", spec=imvcdeexut.datetime)
```

- python built-in methods can not be patched

```
class TestExtractor1(hunitest.TestCase):
	# Mock `Extractor`'s abstract functions.
	abstract_methods_patch = umock.patch.object(
    	    imvcdexex.Extractor, "__abstractmethods__", new=set()
	)
	ohlcv_patch = umock.patch.object(
    	    imvcdexex.Extractor,
    	    "_download_ohlcv",
    	    spec=imvcdexex.Extractor._download_ohlcv,
        	)
```

- patching `__abstractmethods__` function of an abstract class enables us to
  instantiate and test abstract class as any regular class
