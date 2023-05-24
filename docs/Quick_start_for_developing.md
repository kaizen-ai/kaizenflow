# Supporting OS
- We support Mac x86, Apple Silicon and Linux Ubuntu.
- If you are using Windows,
  - Install VMWare software.
  - Reference video for installing [ubuntu](https://www.youtube.com/watch?v=NhlhJFKmzpk&ab_channel=ProgrammingKnowledge) on VMWare software.
  - Make sure you set up your git and github.
  - Install [docker](https://docs.docker.com/engine/install/ubuntu/#install-using-the-repository) on your Ubuntu VM.

# Getting a first issue to work on (aka warm-up issue)

- The goal is to get comfortable with the development system

- We mark certain bugs as "good-as-first-bug"

- Typical warm-up issues are:
  - Write unit tests
  - Copy-paste-modify (ideally refactor and then change) code
  - Simple refactoring

# How to contribute code (short version)

- Make sure you are familiar with our coding style
- Create a branch of your assigned issues/bugs
  - E.g., for a GitHub issue with the name "Expose the linter container to
    Sorrentum contributors #63", the branch name should be
    `SorrTask63_Expose_the_linter_container_to_Sorrentum_contributors`
  - This step is automated through the `invoke` flow (see docs for more info)
- Run the [linter](https://github.com/sorrentum/sorrentum/blob/master/docs/First_review_process.md#run-linter) on your code before pushing.
- Push the code to your branch
- Make sure your branch is up-to-date with the master branch
- Create a Pull Request (PR) from your branch
- Add your assigned reviewers for your PR so that they are informed of your PR
- After being reviewed, the PR will be merged to the master branch by your
  reviewers

# Cloning the code

- All the source code should go under `~/src` (e.g., `/Users/<YOUR_USER>/src` on a Mac PC)
- The path to the local repo folder should look like this `~/src/{REPO_NAME}{IDX}` where
  - `IDX` is an integer
  - `REPO_NAME` is a name of the repository
- To clone the repo, use the cloning command described in [the Github official documentation](https://docs.github.com/en/github/creating-cloning-and-archiving-repositories/cloning-a-repository-from-github/cloning-a-repository)
- Example of cloning command:
```
> git clone git@github.com:sorrentum/sorrentum.git ~/src/sorrentum1
```
  

# Sorrentum Docker container

- Get familiar with Docker if you are not (e.g.,
  https://docs.docker.com/get-started/overview/)

- We work in a Docker container that has all the required dependencies installed

  - You can use PyCharm / VS code on your laptop to edit code, but you want to
    run code inside the dev container since this makes sure everyone is running
    with the same system, and it makes it easy to share code and reproduce
    problems

- Preparation

  1. Build the thin environment

  ```
  > source dev_scripts/client_setup/build.sh
  ```

  2. Activate the thin environment

  ```
  > source dev_scripts/setenv_amp.sh
  ```

  3. Pull the latest cmamp image

  ```
  > i docker_pull
  or
  > docker pull sorrentum/cmamp:latest
  ```

  4. Merge the latest version of master into your branch

  ```
  > i git_merge_master
  ```

- To start a Docker container:

  ```
  > i docker_bash
  ```

  Ignore all the warnings that do not prevent you from running the tests, e.g.,
  ```
  WARNING: The AM_AWS_ACCESS_KEY_ID variable is not set. Defaulting to a blank string.
  WARNING: The AM_AWS_DEFAULT_REGION variable is not set. Defaulting to a blank string.
  WARNING: The AM_AWS_SECRET_ACCESS_KEY variable is not set. Defaulting to a blank string.
  WARNING: The AM_FORCE_TEST_FAIL variable is not set. Defaulting to a blank string.
  WARNING: The CK_AWS_ACCESS_KEY_ID variable is not set. Defaulting to a blank string.
  WARNING: The CK_AWS_DEFAULT_REGION variable is not set. Defaulting to a blank string.
  WARNING: The CK_AWS_SECRET_ACCESS_KEY variable is not set. Defaulting to a blank string.
  WARNING: The CK_TELEGRAM_TOKEN variable is not set. Defaulting to a blank string.
  ```

  ```
  -----------------------------------------------------------------------------
  This code is not in sync with the container:
  code_version='1.4.1' != container_version='1.4.0'
  -----------------------------------------------------------------------------
  You need to:
  - merge origin/master into your branch with `invoke git_merge_master`
  - pull the latest container with `invoke docker_pull`
  ```

- To start a Jupyter server:

  ```
  > i docker_jupyter
  ```

  To open a Jupyter notebook in a local web-browser:
  - in the output from the cmd above find an assigned port, e.g., `[I 14:52:26.824 NotebookApp] http://0044e866de8d:10091/` -> port is `10091`
  - add the port to the link like so: `http://localhost:10091/` or `http://127.0.0.1:10091`
  - copy-paste the link into a web-browser and update the page
