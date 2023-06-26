<!-- toc -->
- [Setting up Git credentials](#setting-up-git-credentials)
  * [Preamble](#preamble)
  * [Check Git credentials](#check-git-credentials)
  * [Setting Git credentials](#setting-git-credentials)
  * [Enforcing Git credentials](#enforcing-git-credentials)
- [Create the env](#create-the-env)
- [Invoke](#invoke)
  * [Listing all the tasks](#listing-all-the-tasks)
  * [Implementation details](#implementation-details)
  * [GitHub](#github)
  * [See all the workflows](#see-all-the-workflows)
  * [Getting help for a workflow](#getting-help-for-a-workflow)
  * [Merge master in the current branch](#merge-master-in-the-current-branch)
  * [Create a PR](#create-a-pr)
  * [Extract a PR from a larger one](#extract-a-pr-from-a-larger-one)
  * [Systematic code transformation](#systematic-code-transformation)
  * [Replace `check_string` with `assert_equal`](#replace-check_string-with-assert_equal)
  * [Run unit tests with coverage](#run-unit-tests-with-coverage)
  * [Munge list of failed tests](#munge-list-of-failed-tests)
- [Docker](#docker)
  * [Generate a local `amp` Docker image](#generate-a-local-amp-docker-image)
  * [Update the dev `amp` Docker image](#update-the-dev-amp-docker-image)
  * [Experiment in a local image](#experiment-in-a-local-image)
- [GitHub Actions (CI)](#github-actions-ci)
  * [Running a single test in GH Actions](#running-a-single-test-in-gh-actions)
  * [run: invoke run_fast_tests](#run-invoke-run_fast_tests)
- [pytest](#pytest)
  * [Run with coverage](#run-with-coverage)
  * [Iterating on stacktrace of failing test](#iterating-on-stacktrace-of-failing-test)
  * [Iterating on a failing regression test](#iterating-on-a-failing-regression-test)
  * [Detect mismatches with golden test outcomes](#detect-mismatches-with-golden-test-outcomes)
- [Playback](#playback)
- [Publish a notebook](#publish-a-notebook)
  * [Detailed instructions](#detailed-instructions)
  * [Publish notebooks](#publish-notebooks)
  * [Open a published notebook](#open-a-published-notebook)
    + [Start a server](#start-a-server)
    + [Using the dev box](#using-the-dev-box)
    + [Using Windows browser](#using-windows-browser)
- [How to create a private fork](#how-to-create-a-private-fork)
- [Integrate public to private: amp -> cmamp](#integrate-public-to-private-amp---cmamp)
  * [Set-up](#set-up)
  * [Ours vs theirs](#ours-vs-theirs)
  * [Sync the repos (after double integration)](#sync-the-repos-after-double-integration)
  * [Updated sync](#updated-sync)
  * [Check that things are fine](#check-that-things-are-fine)
  * [Integrate private to public: cmamp -> amp](#integrate-private-to-public-cmamp---amp)
  * [Squash commit of everything in the branch](#squash-commit-of-everything-in-the-branch)
- [Double integration cmamp amp](#double-integration-cmamp--amp)
  * [Script set-up](#script-set-up)
  * [Manual set-up branches](#manual-set-up-branches)
  * [High-level plan](#high-level-plan)
  * [Sync `im` cmamp -> amp](#sync-im-cmamp---amp)
  * [Sync everything](#sync-everything)
  * [Files that need to be different](#files-that-need-to-be-different)
    + [Lint everything](#lint-everything)
    + [Testing](#testing)
<!-- tocstop -->

# Setting up Git credentials

## Preamble

Git allows setting credentials at different "levels":

- system (set for all the users in `/etc/git`)

- global (set for a single user in `$HOME/.gitconfig` or
  `$HOME/.config/git/config`)

- local (set on a per client basis in `.git/config` in the repo root)

Git uses a hierarchical config approach in which settings of a broader scope are
inherited if not overridden.

Refs:

- How to customize Git:
  [https://git-scm.com/book/en/v2/Customizing-Git-Git-Configuration](https://git-scm.com/book/en/v2/Customizing-Git-Git-Configuration)
- Details on `git config`: https://git-scm.com/docs/git-config

## Check Git credentials

You can check the Git credentials that will be used to commit in a client by
running:

```
> git config -l | grep user
user.name=saggese
user.email=saggese@gmail.com
github.user=gpsaggese
```

To know at which level each variable is defined, run

```
> git config --show-origin user.name
file:/Users/saggese/.gitconfig saggese
```

You can see all the `Authors` in a Git repo history with:

```
> git log | grep -i Author | sort | uniq
...
```

Git doesn't do any validation of `user.name` and `user.email` but it just uses
these values to compose a commit message like:

```
> git log -2
commit 31052d05c226b1c9834d954e0c3d5586ed35f41e (HEAD ->
AmpTask1290_Avoid_committing_to_master_by_mistake)
Author: saggese <saggese@gmail.com>
Date: Mon Jun 21 16:22:25 2021

Update hooks
```

## Setting Git credentials

To keep things simple and avoid variability, our convention is to use:

- as `user.name` our Linux user name on the local computer we are using to
  commit which is returned by `whoami` (e.g., `user.name=saggese`)
- as `user.email` the email that corresponds to that user (e.g,.
  `user.email=saggese@gmail.com`)

To accomplish the set-up above you can:

- use in `/Users/saggese/.gitconfig` the values for our open-source account, so
  that they are used by default

```
> git config --global user.name $(whoami)
> git config --global user.email YOUR_EMAIL
```

- use the correct user / email in the repos that are not open-source

```
> cd $GIT_ROOT
> git config --local user.name $(whoami)
> git config --local user.email YOUR_EMAIL
```

-- Note that you need to set these local values on each Git client that you have
cloned, since Git doesn't version control these values

## Enforcing Git credentials

We use Git hooks to enforce that certain emails are used for certain repos
(e.g., we should commit to our open-source repos only using our personal
non-corporate email).

You need to install the hooks in each Git client that you use. Conceptually this
step is part of `git clone`: every time you clone a repo locally you need to set
the hooks.

TODO(gp): We could create a script to automate cloning a repo and setting it up.

```
> cd //amp
> ./dev_scripts/git/git_hooks/install_hooks.py --action install
> cd //lem
> ./amp/dev_scripts/git/git_hooks/install_hooks.py --action install
```

This procedure creates some links from `.git/hook` to the scripts in the repo.

You can also use the action `status` to see the status and `remove` to the
hooks.

# Create the env

You can follow the

```
# Build the client env
> dev_scripts/client_setup/build.sh
> source dev_scripts/setenv_amp.sh
```

# Invoke

We use `invoke` to implement workflows (aka "tasks") similar to Makefile
targets, but using Python.

[The official documentation.](https://docs.pyinvoke.org/en/0.11.1/index.html)

We use `invoke` to automate tasks and package workflows for:

- docker: `docker_*`

- Git: `git_*`

- GitHub (relying on `gh` integration): `gh_*`

- running tests: `run_*`

- integrate: `integrate_*`

- releasing tools and Docker images: `docker_*`

- lint: `lint_*`

- pytest:

Each set of commands starts with the topic, e.g., `docker_*` for all the docker
related tasks

The best approach to getting familiar with the tasks is to browse the list and
then check the output of the help

```
> invoke --help command
> i -h gh_issue_title
Usage: inv[oke] [--core-opts] gh_issue_title [--options] [other tasks here ...]

Docstring:
Print the title that corresponds to the given issue and repo_short_name.
E.g., AmpTask1251_Update_GH_actions_for_amp.

:param pbcopy: save the result into the system clipboard (only on macOS)

Options:
-i STRING, --issue-id=STRING
-p, --[no-]pbcopy
-r STRING, --repo-short-name=STRING
```

I can guarantee you a 2x improvement in performance, if you master the
workflows, but it takes some time and patience

## Listing all the tasks

```
> invoke --list
INFO: > cmd='/Users/saggese/src/venv/amp.client_venv/bin/invoke --list'
Available tasks:

check_python_files Compile and execute Python files checking for errors.
docker_bash Start a bash shell inside the container corresponding to a stage.
docker_build_local_image Build a local image (i.e., a release candidate "dev"
image).
docker_build_prod_image (ONLY CI/CD) Build a prod image.
docker_cmd Execute the command `cmd` inside a container corresponding to a
stage.
docker_images_ls_repo List images in the logged in repo_short_name.
docker_jupyter Run jupyter notebook server.
docker_kill Kill the last Docker container started.
docker_login Log in the AM Docker repo_short_name on AWS.
docker_ps List all the running containers.
docker_pull Pull latest dev image corresponding to the current repo from the
registry.
docker_pull_dev_tools Pull latest prod image of `dev_tools` from the registry.
docker_push_dev_image (ONLY CI/CD) Push the "dev" image to ECR.
docker_push_prod_image (ONLY CI/CD) Push the "prod" image to ECR.
docker_release_all (ONLY CI/CD) Release both dev and prod image to ECR.
docker_release_dev_image (ONLY CI/CD) Build, test, and release to ECR the latest
"dev" image.
docker_release_prod_image (ONLY CI/CD) Build, test, and release to ECR the prod
image.
docker_rollback_dev_image Rollback the version of the dev image.
docker_rollback_prod_image Rollback the version of the prod image.
docker_stats Report last started Docker container stats, e.g., CPU, RAM.
docker_tag_local_image_as_dev (ONLY CI/CD) Mark the "local" image as "dev".
find_check_string_output Find output of `check_string()` in the test running
find_test_class Report test files containing `class_name` in a format compatible
with
find_test_decorator Report test files containing `class_name` in pytest format.
fix_perms :param action:
gh_create_pr Create a draft PR for the current branch in the corresponding
gh_issue_title Print the title that corresponds to the given issue and
repo_short_name.
gh_workflow_list Report the status of the GH workflows.
gh_workflow_run Run GH workflows in a branch.
git_add_all_untracked Add all untracked files to Git.
git_branch_copy Create a new branch with the same content of the current branch.
git_branch_diff_with_base Diff files of the current branch with master at the
branching point.
git_branch_files Report which files were added, changed, and modified in the
current branch
git_branch_next_name Return a name derived from the branch so that the branch
doesn't exist.
git_clean Clean the repo_short_name and its submodules from artifacts.
git_create_branch Create and push upstream branch `branch_name` or the one
corresponding to
git_create_patch Create a patch file for the entire repo_short_name client from
the base
git_delete_merged_branches Remove (both local and remote) branches that have
been merged into master.
git_files Report which files are changed in the current branch with respect to
git_last_commit_files Print the status of the files in the previous commit.
git_merge_master Merge `origin/master` into the current branch.
git_pull Pull all the repos.
git_fetch_master Pull master without changing branch.
git_rename_branch Rename current branch both locally and remotely.
integrate_compare_branch_with_base Compare the files modified in both the
branches in src_dir and dst_dir to
integrate_copy_dirs Copy dir `subdir` from dir `src_dir` to `dst_dir`.
integrate_create_branch Create the branch for integration in the current dir.
integrate_diff_dirs Integrate repos from dir `src_dir` to `dst_dir`.
lint Lint files.
lint_create_branch Create the branch for linting in the current dir.
print_setup Print some configuration variables.
print_tasks Print all the available tasks in `lib_tasks.py`.
pytest_clean Clean pytest artifacts.
pytest_compare Compare the output of two runs of `pytest -s --dbg` removing
irrelevant
pytest_failed Process the list of failed tests from a pytest run.
pytest_failed_freeze_test_list Copy last list of failed tests to not overwrite
with successive pytest
run_blank_tests (ONLY CI/CD) Test that pytest in the container works.
run_coverage_report
run_fast_slow_tests Run fast and slow tests independently.
run_fast_tests Run fast tests.
run_qa_tests Run QA tests independently.
run_slow_tests Run slow tests.
run_superslow_tests Run superslow tests.
traceback Parse the traceback from Pytest and navigate it with vim.
```

## Implementation details

- By convention all invoke targets are in `*_lib_tasks.py`, e.g.,

  - `helpers/lib_tasks.py` - tasks to be run in `cmamp`

  - `optimizer/opt_lib_tasks.py` - tasks to be run in `cmamp/optimizer`

- All invoke tasks are functions with the `@task` decorator, e.g.,

```
from invoke import task

@task
def invoke_task(...):
  ...
```

- To run a task we use `context.run(...)`, see
  [the official docs](https://docs.pyinvoke.org/en/0.11.1/concepts/context.html)

- To be able to run a specified invoke task one should import it in `tasks.py`

  - E.g., see `cmamp/tasks.py`

- A task can be run only in a dir where it is imported in a corresponding
  `tasks.py`, e.g.,

  - `invoke_task1` is imported in `cmamp/tasks.py` so it can be run only from
    `cmamp`

  - `invoke_task2` is imported in `cmamp/optimizer/tasks.py` so it can be run
    only from `cmamp/optimizer`

    - In other words one should do `cd cmamp/optimizer` before doing
      `i invoke_task2 ...`

## GitHub

```
Get the official branch name corresponding to an Issue

> i gh_issue_title -i 256
## gh_issue_title: issue_id='256', repo_short_name='current'

# Copied to system clipboard:
AmpTask256_Part_task2236_jenkins_cleanup_split_scripts:
https://github.com/alphamatic/amp/pull/256
```

## See all the workflows

Workflows are organized somehow around Linux command that they are related to,
e.g.,

- `docker_*` for all `docker` related workflows

<!-- -->

- `find_*` for all workflows related to finding (e.g., unit tests)

- `gh_*` for GitHub related workflows

- `git_*` for Git related workflows

- etc.

Once in a while it can be useful to list all the available workflows and see if
something interesting was added.

```
> invoke --list
Available tasks:

docker_bash     Start a bash shell inside the container corresponding to a stage.
docker_build_local_image    Build a local image (i.e., a release candidate "dev" image).
...
```

## Getting help for a workflow

You can get a more detailed help with

```
> invoke --help run_fast_tests
Usage: inv[oke] [--core-opts] run_fast_tests [--options] [other tasks here ...]

Docstring:
Run fast tests.

:param stage: select a specific stage for the Docker image
:param pytest_opts: option for pytest
:param pytest_mark: test list to select as `@pytest.mark.XYZ`
:param dir_name: dir to start searching for tests
:param skip_submodules: ignore all the dir inside a submodule
:param coverage: enable coverage computation
:param collect_only: do not run tests but show what will be executed

Options:
-c, --coverage
-d STRING, --dir-name=STRING
-k, --skip-submodules
-o, --collect-only
-p STRING, --pytest-opts=STRING
-s STRING, --stage=STRING
-y STRING, --pytest-mark=STRING
```

## Merge master in the current branch

```
> i git_merge_master
```

## Create a PR

TODO(gp): Describe

## Extract a PR from a larger one

My workflow is to have a feature branch (e.g.,
`AmpTask1891_Sketch_out_the_design_of_RT_OMS`) that I develop in.

When a piece of code can be merged:

- create a new branch (e.g., `AmpTask1891_Sketch_out_the_design_of_RT_OMS_02`)

- copy the current feature branch to the new branch

- remove the pieces that you don't want to merge

- run regressions

- PR

- merge into master

- merge master into the feature branch

This workflow allows you to develop and regress / merge without too much hassle
solving the problem of "stacked PRs".

```
# Go to the client with the branch that you want to divvy up.
> git checkout ${feature_branch}

# Make sure that the branch is up-to-date with master
> i git_merge_master

# Lint.
> i lint -b

# Create a patch from the branch (there are many options to tweak the workflow, check the help)
> i git_create_patch -b

# To apply the patch and execute:
> git checkout 8f9cda97
> git apply /Users/saggese/src/lemonade1/amp/patch.amp.8f9cda97.20210609_080439.patch
...
```

Go to a fresh Git client (I have 2-3 Git clients separated from the one in which
I develop for this kind of operations) or go to master in the same Git client

```
# Go to master
> git checkout master

# Apply the patch from the run of `git_create_patch`

> git apply
> /Users/saggese/src/lemonade1/amp/patch.amp.8f9cda97.20210609_080439.patch

This patch should apply cleanly and with no errors from git, otherwise it means
that your feature branch doesn't have the latest master

# Remove what you don't want to commit.

Do not change anything or run the linter otherwise your feature branch will not
merge easily.

> git diff
> git checkout master -- ...
...
> git commit; git push

# Create a PR (non-draft so that GH can start running the tests)
> i gh_create_pr --no-draft

# Regress the branch
> i run_fast_tests ...

# Merge the PR into master

# Go back to your feature branch and merge master
> gco ${feature_branch}
> git pull

# Now one piece of your feature branch has been merged and you can repeat until
all the code is merged.

### Using git
> git checkout `dst_branch`
> git merge --squash --ff `src_branch`
> git reset HEAD
```

## Systematic code transformation

See the help of amp/dev_scripts/replace_text.py

## Replace `check_string` with `assert_equal`

## Run unit tests with coverage

## Munge list of failed tests

# Docker

## Generate a local `amp` Docker image

This is a manual flow used to test and debug images before releasing them to the
team.

The flow is similar to the dev image, but by default tests are not run and the
image is not released.

```
Build the local image (and update Poetry dependencies, if needed).

> i docker_build_local_image --update-poetry
...
docker image ls 665840871993.dkr.ecr.us-east-1.amazonaws.com/amp:local

REPOSITORY TAG IMAGE ID CREATED SIZE
665840871993.dkr.ecr.us-east-1.amazonaws.com/amp local 9b3f8f103a2c 1 second ago 1.72GB

# Test the new "local" image
> i docker_bash --stage "local" python -c "import async_solipsism" python -c
> "import async_solipsism; print(async_solipsism.**version**)"

# Run the tests with local image
# Make sure the new image is used: e.g., add an import and trigger the tests.
> i run_fast_tests --stage "local" --pytest-opts core/dataflow/test/test_real_time.py
> i run_fast_slow_tests --stage "local"

# Promote a local image to dev.
> i docker_tag_local_image_as_dev
> i docker_push_dev_image
```

## Update the dev `amp` Docker image

To implement the entire Docker QA process of a dev image

```
Clean all the Docker images locally, to make sure there is no hidden state.
> docker system prune --all

# Update the needed packages.
> devops/docker_build/pyproject.toml

# Visually inspect the updated packages.
> git diff devops/docker_build/poetry.lock

# Run entire release process.
> i docker_release_dev_image
```

## Experiment in a local image

To install packages in an image, do `i docker_bash`

```
# Switch to root and install package.
> sudo su -
> source /venv/bin/activate
> pip install <package>

# Switch back to user.
> exit
```

You should test that the package is installed for your user, e.g.,

```
> source /venv/bin/activate python -c "import foobar; print(foobar);print(foobar.__version__)"
```

You can now use the package in this container. Note that if you exit the
container, the modified image is lost, so you need to install it again.

You can save the modified image, tagging the new image as local, while the
container is still running.

- Copy your Container ID. You can find it

  - in the docker bash session, e.g., if the command line in the container
    starts with `user_1011@da8f3bb8f53b:/app$`, your Container ID is
    `da8f3bb8f53b`

  - by listing running containers, e.g., run `docker ps` outside the container

- Commit image

```
    > docker commit <Container ID> <IMAGE>/cmamp:local-$USER
```

    E.g.
    `docker commit da8f3bb8f53b 665840871993.dkr.ecr.us-east-1.amazonaws.com/cmamp:local-julias`

If you are running inside a notebook using `i docker_jupyter` you can install
packages using a one liner `! sudo su -; source ...; `

# GitHub Actions (CI)

## Running a single test in GH Actions

Create a branch

Change .github/workflows/fast_tests.yml

run: invoke run_fast_tests

## run: invoke run_fast_tests

--pytest-opts="helpers/test/test_git.py::Test_git_modified_files1::test_get_modified_files_in_branch1
-s --dbg"

In the current implementation (where we try to not run for branches) to run in a
branch

# pytest

From https://gist.github.com/kwmiebach/3fd49612ef7a52b5ce3a

## Run with coverage

```
> i run_fast_tests --pytest-opts="core/test/test_finance.py" --coverage
```

## Iterating on stacktrace of failing test

Inside docker bash

```
> pytest ...
```

The test fails: switch to using `pytest.sh` to save the stacktrace to a file

Then from outside Docker launch vim in quickfix mode

```
> invoke traceback
```

The short form is `it`

## Iterating on a failing regression test

The workflow is:

```
# Run a lot of tests, e.g., the entire regression suite.
> pytest ...
# Some tests fail.

# Run the `pytest_repro` to summarize test failures and to generate commands to reproduce them.
> invoke pytest_repro
```

## Detect mismatches with golden test outcomes

The command is

```
> i pytest_find_unused_goldens
```

The specific dir to check can be specified with the `dir_name` parameter.

The invoke detects and logs mismatches between the tests and the golden outcome
files.

- When goldens are required by the tests but the corresponding files do not
  exist

  - This usually happens if the tests are skipped or commented out.

  - Sometimes it's a FP hit (e.g. the method doesn't actually call
    `check_string` but instead has it in a string, or `check_string` is called
    on a missing file on purpose to verify that an exception is raised).

- When the existing golden files are not actually required by the corresponding
  tests.

  - In most cases it means the files are outdated and can be deleted.

  - Alternatively, it can be a FN hit: the test method A, which the golden
    outcome corresponds to, doesn't call `check_string` directly, but the test's
    class inherits from a different class, which in turn has a method B that
    calls `check_string`, and this method B is called in the test method A.

For more details see
[CmTask528](https://github.com/cryptokaizen/cmamp/issues/528).

# Playback

# Publish a notebook

- `publish_notebook.py` is a little tool that allows to:

1. Opening a notebook in your browser (useful for read-only mode)

- E.g., without having to use Jupyter notebook (which modifies the file in your
  client) or github preview (which is slow or fails when the notebook is too
  large)

2. Sharing a notebook with others in a simple way

3. Pointing to detailed documentation in your analysis Google docs

4. Reviewing someone's notebook

5. Comparing multiple notebooks against each other in different browser windows

6. Taking a snapshot / checkpoint of a notebook as a backup or before making
   changes

- This is a lightweight alternative to "unit testing" to capture the desired
  behavior of a notebook

- One can take a snapshot and visually compare multiple notebooks side-by-side
  for changes

## Detailed instructions

- You can get details by running:

```bash
> dev_scripts/notebooks/publish_notebook.py -h
```

Plug-in for Chrome

[my-s3-browser](https://chrome.google.com/webstore/detail/my-s3-browser/lgkbddebikceepncgppakonioaopmbkk?hl=en)

## Publish notebooks

Make sure that your environment is set up properly

```
> more ~/.aws/credentials
[am]
aws_access_key_id=**
aws_secret_access_key=**
aws_s3_bucket=alphamatic-data

> printenv | grep AM_
AM_AWS_PROFILE=am
```

If you don't have them, you need to re-run `source dev_scripts/setenv.sh` in all
the shells. It might be easier to kill that tmux session and restart it

```
> tmux kill-session --t limeXYZ

> ~/go_lem.sh XYZ
```

Inside or outside a Docker bash run

```
> publish_notebook.py --file http://127.0.0.1:2908/notebooks/notebooks/Task40_Optimizer.ipynb --action publish_on_s3
```

The file is copied to S3

```
Copying './Task40_Optimizer.20210717_010806.html' to
's3://alphamatic-data/notebooks/Task40_Optimizer.20210717_010806.html'
```

You can also save the data locally:

```
> publish_notebook.py --file
amp/oms/notebooks/Master_forecast_processor_reader.ipynb --action publish_on_s3
--aws_profile saml-spm-sasm
```

You can also use a different path or profile by specifying it directly

```
> publish_notebook.py \
--file http://127.0.0.1:2908/notebooks/notebooks/Task40_Optimizer.ipynb \
--action publish_on_s3 \
--s3_path s3://alphamatic-data/notebooks \
--aws_profile am
```

## Open a published notebook

### Start a server

(cd /local/home/share/html/published_notebooks; python3 -m http.server 8000)

go to the page in the local browser

### Using the dev box

To open a notebook saved on S3, \*outside\* a Docker container run:

```
> publish_notebook.py --action open --file
s3://alphamatic-data/notebooks/Task40_Optimizer.20210717_010806.html
```

This opens a Chrome window through X-windows.

To open files faster you can open a Chrome window in background with

```
> google-chrome
```

and then navigate to the path (e.g.,
/local/home/share/html/published_notebooks/Master_forecast_processor_reader.20220810-112328.html)

### Using Windows browser

Another approach is:

```
> aws s3 presign --expires-in 36000
s3://alphamatic-data/notebooks/Task40_Optimizer.20210716_194400.html | xclip
```

Open the link saved in the clipboard in the Windows browser

For some reason, Chrome saves the link instead of opening, so you need to click
on the saved link

# How to create a private fork

https://stackoverflow.com/questions/10065526/github-how-to-make-a-fork-of-public-repository-private

From
https://docs.github.com/en/github/creating-cloning-and-archiving-repositories/creating-a-repository-on-github/duplicating-a-repository

```
> git clone --bare git@github.com:alphamatic/amp.git amp_bare

> git push --mirror https://github.com/cryptomtc/cmamp.git
```

It worked only as cryptomtc, but not using my key

# Integrate public to private: amp -> cmamp

## Set-up

```
> git remote add public git@github.com:alphamatic/amp

# Go to cmamp
> cd /data/saggese/src/cmamp1
> cd /Users/saggese/src/cmamp1

# Add the remote
# git remote add public https://github.com/exampleuser/public-repo.git
> git remote add public git@github.com:alphamatic/amp

> git remote -v
origin https://github.com/cryptomtc/cmamp.git (fetch)
origin https://github.com/cryptomtc/cmamp.git (push)
public git@github.com:alphamatic/amp (fetch)
public git@github.com:alphamatic/amp(push)
```

## Ours vs theirs

From
[https://stackoverflow.com/questions/25576415/what-is-the-precise-meaning-of-ours-and-theirs-in-git/25576672](https://stackoverflow.com/questions/25576415/what-is-the-precise-meaning-of-ours-and-theirs-in-git/25576672)

When merging:

- ours = branch checked out (git checkout \*ours\*)

- theirs = branch being merged (git merge \*theirs\*)

When rebasing the role is swapped

- ours = branch being rebased onto (e.g., master)

- theirs = branch being rebased (e.g., feature)

## Sync the repos (after double integration)

```
> git fetch origin; git fetch public

# Pull from both repos

> git pull public master -X ours

You might want to use `git pull -X theirs` or `ours`

> git pull -X theirs

> git pull public master -s recursive -X ours

When there is a file added it's better to add

> git diff --name-status --diff-filter=U | awk '{print $2}'

im/ccxt/db/test/test_ccxt_db_utils.py

# Merge branch

> gs
+ git status
On branch AmpTask1786_Integrate_20211128_02 Your branch and 'origin/AmpTask1786_Integrate_20211128_02' have diverged, and have 861 and 489
different commits each, respectively. (use "git pull" to merge the remote branch into yours)

You are in a sparse checkout with 100% of tracked files present.

nothing to commit, working tree clean

> git pull -X ours

## Make sure it's synced at ToT

> rsync --delete -r /Users/saggese/src/cmamp2/ /Users/saggese/src/cmamp1
--exclude='.git/'

> diff -r --brief /Users/saggese/src/cmamp1 /Users/saggese/src/cmamp2 | grep -v \.git
```

## Updated sync

```
> git fetch origin; git fetch public
```

## Check that things are fine

```
> git diff origin/master... >patch.txt

> cd /Users/saggese/src/cmamp2

# Create a branch

> git checkout -b Cmamp114_Integrate_amp_cmamp_20210928
> git apply patch.txt

# Compare branch with references

> dev_scripts/diff_to_vimdiff.py --dir1 /Users/saggese/src/cmamp1/im --dir2
/Users/saggese/src/cmamp2/im

> diff -r --brief /Users/saggese/src/lemonade3/amp \~/src/cmamp2 | grep -v "/im"

# Creates a merge commit
> git push origin master
```

## Integrate private to public: cmamp -> amp

```
> cd /data/saggese/src/cmamp1
> tar cvzf patch.tgz $(git diff --name-onlyorigin/master public/master | grep -v repo_config.py)

> cd /Users/saggese/src/amp1 git remote add cmamp
> git@github.com:cryptomtc/cmamp.git

> GIT_SSH_COMMAND="ssh -i \~/.ssh/cryptomatic/id_rsa.cryptomtc.github" git fetch
> git@github.com:cryptomtc/cmamp.git

> git checkout -b Integrate_20210928

> GIT_SSH_COMMAND="ssh -i \~/.ssh/cryptomatic/id_rsa.cryptomtc.github" git pull
> cmamp master -X ours
```

## Squash commit of everything in the branch

From
[https://stackoverflow.com/questions/25356810/git-how-to-squash-all-commits-on-branch](https://stackoverflow.com/questions/25356810/git-how-to-squash-all-commits-on-branch)

```
> git checkout yourBranch
> git reset $(git merge-base master $(git branch
--show-current))
> git add -A
> git commit -m "Squash"
> git push --force
```

# Double integration cmamp < -- > amp

The bug is
[https://github.com/alphamatic/amp/issues/1786](https://github.com/alphamatic/amp/issues/1786)

## Script set-up

```
> vi /Users/saggese/src/amp1/dev_scripts/integrate_repos/setup.sh
Update the date

> vi /Users/saggese/src/amp1/dev_scripts/integrate_repos/*

> cd \~/src/amp1
> source /Users/saggese/src/amp1/dev_scripts/integrate_repos/setup.sh

> cd \~/src/cmamp1
> source /Users/saggese/src/amp1/dev_scripts/integrate_repos/setup.sh
```

## Manual set-up branches

```
# Go to cmamp1
> go_amp.sh cmamp 1

# Set up the env vars in both clients
> export AMP_DIR=/Users/saggese/src/amp1; export
CMAMP_DIR=/Users/saggese/src/cmamp1; echo "$AMP_DIR"; ls
$AMP_DIR; echo "$CMAMP_DIR"; ls $CMAMP_DIR

Create two branches
> export BRANCH_NAME=AmpTask1786_Integrate_20211010 export BRANCH_NAME=AmpTask1786_Integrate_2021117
...
> cd $AMP_DIR

# Create automatically
> i git_create_branch -b $BRANCH_NAME

# Create manually
> git checkout -b $BRANCH_NAME
> git push --set-upstream origin $BRANCH_NAME

> cd $CMAMP_DIR
> i git_create_branch -b $BRANCH_NAME
```

## High-level plan

SUBDIR=im

- Typically cmamp is copied on top of amp

SUBDIR=devops

- cmamp and amp need to be different (until we unify the Docker flow)

Everything else

- Typically amp -> cmamp

## Sync `im` cmamp -> amp

```
SUBDIR=im

# Check different files
> diff -r --brief $AMP_DIR/$SUBDIR $CMAMP_DIR/$SUBDIR | grep -v .git

# Diff the entire dirs with vimdiff
> dev_scripts/diff_to_vimdiff.py --dir1 $AMP_DIR/$SUBDIR --dir2 $CMAMP_DIR/$SUBDIR

# Find different files
> find $AMP_DIR/$SUBDIR -name "*"; find $CMAMP_DIR/$SUBDIR -name "*" sdiff
/tmp/dir1 /tmp/dir2

# Copy cmamp -> amp
> rsync --delete -au $CMAMP_DIR/$SUBDIR/ $AMP_DIR/$SUBDIR
-a = archive
-u = ignore newer

# Add all the untracked files
> cd $AMP_DIR/$SUBDIR && git add $(git ls-files -o --exclude-standard)

# Check that there are no differences after copying
> dev_scripts/diff_to_vimdiff.py --dir1 $AMP_DIR/$SUBDIR --dir2 $CMAMP_DIR/$SUBDIR

==========

> rsync --delete -rtu $AMP_DIR/$SUBDIR/ $CMAMP_DIR/$SUBDIR

> rsync --dry-run -rtui --delete $AMP_DIR/$SUBDIR/ $CMAMP_DIR/$SUBDIR/ .d..t.... ./
> f..t.... __init__.py
cd+++++++ features/
> f+++++++ features/__init__.py
> f+++++++ features/pipeline.py
cd+++++++ features/test/
> f+++++++ features/test/test_feature_pipeline.py
cd+++++++ features/test/TestFeaturePipeline.test1/
cd+++++++ features/test/TestFeaturePipeline.test1/output/
> f+++++++ features/test/TestFeaturePipeline.test1/output/test.txt
.d..t.... price/
.d..t.... real_time/
> f..t.... real_time/__init__.py
.d..t.... real_time/notebooks/
> f..t.... real_time/notebooks/Implement_RT_interface.ipynb
> f..t.... real_time/notebooks/Implement_RT_interface.py
.d..t.... real_time/test/
cd+++++++ real_time/test/TestRealTimeReturnPipeline1.test1/
cd+++++++ real_time/test/TestRealTimeReturnPipeline1.test1/output/
> f+++++++ real_time/test/TestRealTimeReturnPipeline1.test1/output/test.txt
.d..t.... returns/
> f..t.... returns/__init__.py
> f..t.... returns/pipeline.py
.d..t.... returns/test/
> f..t.... returns/test/test_returns_pipeline.py
.d..t.... returns/test/TestReturnsBuilder.test_equities1/
.d..t.... returns/test/TestReturnsBuilder.test_equities1/output/
.d..t.... returns/test/TestReturnsBuilder.test_futures1/
.d..t.... returns/test/TestReturnsBuilder.test_futures1/output/

> rsync --dry-run -rtui --delete $CMAMP_DIR/$SUBDIR/ $AMP_DIR/$SUBDIR/
> f..t.... price/__init__.py
> f..t.... price/pipeline.py
> f..t.... real_time/pipeline.py
> f..t.... real_time/test/test_dataflow_amp_real_time_pipeline.py
> f..t.... returns/test/TestReturnsBuilder.test_equities1/output/test.txt
> f..t.... returns/test/TestReturnsBuilder.test_futures1/output/test.txt
```

## Sync everything

```
# Check if there is anything in cmamp more recent than amp
> rsync -au --exclude='.git' --exclude='devops' $CMAMP_DIR/ $AMP_DIR

# vimdiff
> dev_scripts/diff_to_vimdiff.py --dir1 $AMP_DIR --dir2
$CMAMP_DIR

F1: skip
F9: choose left (i.e., amp)
F10: choose right (i.e,. cmamp)

# Copy

> rsync -au --delete --exclude='.git' --exclude='devops' --exclude='im'
$AMP_DIR/
$CMAMP_DIR

# Add all the untracked files

> (cd $CMAMP_DIR/$SUBDIR && git add $(git ls-files -o --exclude-standard))

> diff -r --brief $AMP_DIR $CMAMP_DIR | grep -v .git | grep Only
```

## Files that need to be different

amp needs an `if False` helpers/lib_tasks.py

amp needs two tests disabled im/ccxt/data/load/test/test_loader.py
im/ccxt/data/load/test/test_loader.py

TODO(gp): How to copy files in vimdiff including last line?

- Have a script to remove all the last lines
- Some files end with an `0x0a`
- tr -d '\\r'

```
find . -name "\*.txt" | xargs perl -pi -e 's/\\r\\n/\\n/g'
# Remove `No newline at end of file`
find . -name "\*.txt" | xargs perl -pi -e 'chomp if eof'
```

### Lint everything

```
> autoflake amp_check_filename amp_isort amp_flake8 amp_class_method_order
amp_normalize_import amp_format_separating_line amp_black

> i lint --phases="amp_isort amp_class_method_order amp_normalize_import
amp_format_separating_line amp_black" --files='$(find . -name "\*.py")'
```

### Testing

- Run amp on my laptop (or on the server)

- IN PROGRESS: Get amp PR to pass on GH

- IN PROGRESS: Run lemonade on my laptop

- Run cmamp on the dev server

- Get cmamp PR to pass on GH

- Run dev_tools on the dev server
