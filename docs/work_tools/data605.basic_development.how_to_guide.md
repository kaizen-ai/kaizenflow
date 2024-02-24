

<!-- toc -->

- [Convention Documentation](#convention-documentation)
  * [Introduction](#introduction)
  * [1. Building the Coding Environment](#1-building-the-coding-environment)
    + [Supporting OS](#supporting-os)
    + [Cloning the Code](#cloning-the-code)
    + [Building the Thin Environment](#building-the-thin-environment)
  * [2. Coding Style](#2-coding-style)
  * [3. Linter](#3-linter)
    + [Run the linter and check the linter results](#run-the-linter-and-check-the-linter-results)
  * [4. Writing and Contributing Code](#4-writing-and-contributing-code)
      - [By adhering to these conventions, we aim to create a collaborative and efficient coding environment for all contributors. Happy coding!](#by-adhering-to-these-conventions-we-aim-to-create-a-collaborative-and-efficient-coding-environment-for-all-contributors-happy-coding)

<!-- tocstop -->

# Convention Documentation

## Introduction

This document outlines the conventions to be followed by the Sorrentum
contributors. By documenting these conventions, we aim to streamline the
information flow and make the contribution process seamless.

## 1. Building the Coding Environment

### Supporting OS

- We support Mac x86, Apple Silicon and Linux Ubuntu
- Install `docker-cli` On Mac using
  ```bash
  > brew install docker
  > brew link docker
  > brew install colima
  ```
- If you are using Windows,
  - Install VMWare software
  - Reference video for installing
    [ubuntu](https://www.youtube.com/watch?v=NhlhJFKmzpk&ab_channel=ProgrammingKnowledge)
    on VMWare software
  - Make sure you set up your git and github
  - Install
    [docker](https://docs.docker.com/engine/install/ubuntu/#install-using-the-repository)
    on your Ubuntu VM

### Cloning the Code

- All the source code should go under ~/src (e.g., /Users/<YOUR_USER>/src on a
  Mac PC)
- The path to the local repo folder should look like this
  `~/src/{REPO_NAME}{IDX}` where
  - IDX is an integer
  - REPO_NAME is a name of the repository

- To clone the repo, use the cloning command described in the GitHub official
  documentation

- Example of cloning command:
  - The following command might not work sometimes
  ```
  > git clone git@github.com:sorrentum/sorrentum.git ~/src/sorrentum1
  ```
  - Alternative command
  ```
  > git clone https://github.com/sorrentum/sorrentum.git ~/src/sorrentum1
  ```

### Building the Thin Environment

- Create a thin environment for running the linter using the Sorrentum Dev
  Docker container

- Build the thin environment; this is done once
  ```
  > source dev_scripts/client_setup/build.sh
  ```

- Activate the thin environment; make sure it is always activated
  ```
  > source dev_scripts/setenv_amp.sh
  ```

- If you see output like this, your environment is successfully built! If not
  and you encounter any issues, please post them by creating a new issue and
  assign it to the TA
  ```
  alias sp='echo '\''source ~/.profile'\''; source ~/.profile'
  alias vi='/usr/bin/vi'
  alias vim='/usr/bin/vi'
  alias vimdiff='/usr/bin/vi -d'
  alias vip='vim -c "source ~/.vimrc_priv"'
  alias w='which'
  ==> SUCCESS <==
  ```

## 2. Coding Style

- Adopt the coding style outlined -
  [here](https://github.com/sorrentum/sorrentum/blob/master/docs/coding/all.coding_style.how_to_guide.md).

## 3. Linter

- The linter is in charge of reformatting the code according to our conventions
  and reporting potential problems

### Run the linter and check the linter results

- Run the linter against the changed files in the PR branch
  ```
  invoke lint --files "file1 file2..."
  ```

- More information about Linter -
  [Link](https://github.com/sorrentum/sorrentum/blob/master/docs/infra/linter_gh_workflow.explanation.md)
- Internalize the guidelines to maintain code consistency

## 4. Writing and Contributing Code

- If needed, always start with creating an issue first, providing a summary of
  what you want to implement and assign it to yourself and your team
- Create a branch of your assigned issues/bugs
  - E.g., for a GitHub issue with the name: "Expose the linter container to
    Sorrentum contributors #63", The branch name should be :
    `SorrTask63_Expose_the_linter_container_to_Sorrentum_contributors`
- Implement the code based on the requirements in the assigned issue
- Run the linter on your code before pushing
- Do `git commit` and `git push` together so the latest changes are readily
  visible
- Make sure your branch is up-to-date with the master branch
- Create a Pull Request (PR) from your branch
- Add your assigned reviewers for your PR so that they are informed of your PR
- After being reviewed, the PR will be merged to the master branch by your
  reviewers

#### By adhering to these conventions, we aim to create a collaborative and efficient coding environment for all contributors. Happy coding!
