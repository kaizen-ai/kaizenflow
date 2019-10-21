# Jenkins builds

## Naming conventions for Jenkins builds
- Builds are called in Jenkins as `<REPO>.<BRANCH>.<SCRIPT_NAME>`
    - E.g., `amp.master.run_fast_tests`, `p1.master.run_linter`

## Naming conventions for scripts
- The script name describes the action taken by the script and corresponds to
  scripts under `dev_scripts/jenkins`
    - E.g., `build_clean_env.run_slow_coverage_tests.sh` means that
        1) a clean conda environment is created
        2) tests in the `slow` test lists are run with coverage

## Brief description of the scripts
- The tests are under:
    ```bash
    > ls -1 dev_scripts/jenkins
    build_clean_env.amp_develop.sh
    build_clean_env.run_fast_coverage_tests.sh
    ...
    ```

- `build_clean_env.amp_develop.sh`
    - What it does
        - Builds a daily clean conda env that some "quick" builds rely on
    - Goals:
        - During fast test suite we don't want to spend time rebuilds the conda
          env every time, but we want to be notified of a break as soon as
          possible
        - Test that the conda env can be recreated from scratch with
          `create_conda`

- `build_clean_env.run_fast_coverage_tests.sh`
    - What it does:
        - Build a clean env
        - Run fast tests with coverage
    - Goal: We want to ensure that fast tests are passing and assess their code
      coverage

- `build_clean_env.run_fast_tests.sh`
    - Same as above
    - TODO(gp): Maybe a bit redundant

- `build_clean_env.run_slow_coverage_tests.sh`
    - Same as above but using slow test suite

- `run_parallel_fast_tests.sh`
    - What it does:
        - Run fast tests in parallel

- `run_fast_tests.sh`
    - What it does:
        - Run fast tests in serial mode

- `run_pytest_collect.run_linter.sh`
    - What it does:
        - Run some pytest diagnostic, e.g., counting the unit tests
        - Run the linter on the entire tree

## Description of the Jenkins builds

- `amp.dev.build_clean_env.run_slow_coverage_tests`
    - This is the build used to test dangerous changes in `dev` before going into
      `master` (see section below)
    - Runs on demand

- `amp.master.build_clean_env.run_fast_coverage_tests`
    - Runs once a day at around 23 UTC

- `amp.master.run_pytest_collect.run_linter`
    - Runs once a day at around 23 UTC

- `p1.master.run_pytest_collect.run_linter`
    - Runs once a day at around 23 UTC

- `amp.master.run_fast_tests`
    - Polls Git `master` every minute and if there is a change runs the fast tests

- `amp.master.build_clean_env.amp_develop`
    - Runs at 23 ET

- `p1.master.build_clean_env.p1_develop`
    - Runs at 23 ET

# `dev` Jenkins build

## Goal

- This is useful to test dangerous code as Jenkins (e.g., that might have
  dependencies from our env) before committing to `master`

## Setup

- Create a `dev` branch in `amp` / `p1` repo

## Running a dev build

- Merge `master` to `dev`
```bash
> git checkout dev
> git merge master
```

- `dev` should have no difference with `master`
```bash
> git ll master..dev
> git ll dev..master
```

- Merge your code from the branch into `dev`
```
> git checkout dev
> git merge PartTask354_INFRA_Populate_S3_bucket
```

- Trigger a Jenkins build in dev to see if it passes

- Optional review

- Merge `dev` into `master`

# Installing / setting up Jenkins

1) Map python3 as python
    ```bash
    sudo ln -s /usr/bin/python3 /usr/bin/python
    ```

2) Create a `.bashrc`
    ```bash
    export PATH=/anaconda3/bin:$PATH
    export PYTHONPATH=""
    ```

3) Initialize conda with `conda init bash` which adds to `.bashrc` something like:
    ```bash
    # >>> conda initialize >>>
    # !! Contents within this block are managed by 'conda init' !!
    __conda_setup="$('/anaconda3/bin/conda' 'shell.bash' 'hook' 2> /dev/null)"
    if [ $? -eq 0 ]; then
        eval "$__conda_setup"
    else
        if [ -f "/anaconda3/etc/profile.d/conda.sh" ]; then
            . "/anaconda3/etc/profile.d/conda.sh"
        else
            export PATH="/anaconda3/bin:$PATH"
        fi
    fi
    unset __conda_setup
    # <<< conda initialize <<<
    ```

4) The build script for Jenkins is like
    ```bash
    #!/bin/bash -xe

    amp/dev_scripts/jenkins/amp.pytest.sh
    ```

# Expose webpages

- From `https://wiki.jenkins.io/display/JENKINS/User+Content`
- https://plugins.jenkins.io/htmlpublisher
