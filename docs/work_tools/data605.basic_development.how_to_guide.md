

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

- We support Mac (both x86, Apple Silicon) and Linux Ubuntu
- We do not support Windows and WSL: we have tried several times to port the
  toolchain to it, but there are always subtle incompatible behaviors that drive
  everyone crazy
  - If you are using Windows, we suggest to use dual boot with Linux or use a
    virtual machine with Linux
    - Install VMWare software
    - Reference video for installing
      [ubuntu](https://www.youtube.com/watch?v=NhlhJFKmzpk&ab_channel=ProgrammingKnowledge)
      on VMWare software
    - Make sure you set up your git and github
    - Install
      [docker](https://docs.docker.com/engine/install/ubuntu/#install-using-the-repository)
      on your Ubuntu VM

### Install Docker

- Follow https://docs.docker.com/engine/install/

- For Mac you can also install `docker-cli` without the GUI using
  ```bash
  > brew install docker
  > brew link docker
  > brew install colima
  ```

- After installing make sure Docker works on your laptop (of course the version
  will be newer)
  ```bash
  > docker version
  Client:
   Cloud integration: v1.0.24
   Version:           20.10.17
   API version:       1.41
   Go version:        go1.17.11
   Git commit:        100c701
   Built:             Mon Jun  6 23:04:45 2022
   OS/Arch:           darwin/amd64
   Context:           default
   Experimental:      true

  Server: Docker Desktop 4.10.1 (82475)
   Engine:
    Version:          20.10.17
    API version:      1.41 (minimum version 1.12)
    Go version:       go1.17.11
    Git commit:       a89b842
    Built:            Mon Jun  6 23:01:23 2022
    OS/Arch:          linux/amd64
    Experimental:     false
   containerd:
    Version:          1.6.6
    GitCommit:        10c12954828e7c7c9b6e0ea9b0c02b01407d3ae1
   runc:
    Version:          1.1.2
    GitCommit:        v1.1.2-0-ga916309
   docker-init:
    Version:          0.19.0
    GitCommit:        de40ad0
    ```

### Cloning the Code

- To clone the repo, use the cloning command described in the GitHub official
  documentation

- Example of cloning command:
  ```
  > git clone git@github.com:sorrentum/sorrentum.git ~/src/sorrentum1
  ```
  - The previous command might not work sometimes and an alternative command
    using HTTP instead of SSH
  ```
  > git clone https://github.com/sorrentum/sorrentum.git ~/src/sorrentum1
  ```

- All the source code should go under `~/src` (e.g., `/Users/<YOUR_USER>/src` on a
  Mac)
- The path to the local repo folder should look like this
  `~/src/{REPO_NAME}{IDX}` where
  - `REPO_NAME` is a name of the repository
  - IDX is an integer

### Building the Thin Environment

- Create a thin environment for running the Sorrentum Dev Docker container

- Build the thin environment; this is done once per client
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
  ...
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
