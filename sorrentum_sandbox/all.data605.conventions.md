

<!-- toc -->

- [Convention Documentation](#convention-documentation)
  * [Introduction](#introduction)
  * [Convention Sections](#convention-sections)
    + [1. Building the Coding Environment](#1-building-the-coding-environment)
      - [1.1 Supporting OS](#11-supporting-os)
      - [1.2 Cloning the Code](#12-cloning-the-code)
        * [Sometimes it does not work.](#sometimes-it-does-not-work)
        * [Alternative command.](#alternative-command)
      - [1.3 Building the Thin Environment](#13-building-the-thin-environment)
    + [2. Coding Style](#2-coding-style)
      - [Linter](#linter)
        * [Run the linter and check the linter results](#run-the-linter-and-check-the-linter-results)
    + [3. Writing and Contributing Code](#3-writing-and-contributing-code)
    + [4. Additional Document links](#4-additional-document-links)
      - [By adhering to these conventions, we aim to create a collaborative and efficient coding environment for all contributors. Happy coding!](#by-adhering-to-these-conventions-we-aim-to-create-a-collaborative-and-efficient-coding-environment-for-all-contributors-happy-coding)

<!-- tocstop -->

# Convention Documentation

## Introduction

This document outlines the conventions to be followed by the Sorrentum
contributors. By documenting these conventions, we aim to streamline the
information flow and make the contribution process seamless.

## Convention Sections

### 1. Building the Coding Environment

#### 1.1 Supporting OS

- We support Mac x86, Apple Silicon and Linux Ubuntu

- If you are using Windows,

  Install VMWare software -
  [Reference video for installing ubuntu](https://youtu.be/NhlhJFKmzpk?si=4MMOYzLnhyP4eSj2)
  on VMWare software

  Install
  [docker](https://docs.docker.com/engine/install/ubuntu/#install-using-the-repository)
  on your Ubuntu VM

- Make sure you set up your git and github

#### 1.2 Cloning the Code

    All the source code should go under ~/src (e.g., /Users/<YOUR_USER>/src on a Mac PC)

    The path to the local repo folder should look like this ~/src/{REPO_NAME}{IDX} where

    IDX is an integer
    REPO_NAME is a name of the repository
    To clone the repo, use the cloning command described in the Github official documentation

Example of cloning command:

##### Sometimes it does not work.
    ```
    > git clone git@github.com:sorrentum/sorrentum.git ~/src/sorrentum1
    ```

##### Alternative command.
    ```
    > git clone https://github.com/sorrentum/sorrentum.git ~/src/sorrentum1
    ```

#### 1.3 Building the Thin Environment

Create a thin environment for running the linter using the Sorrentum Dev Docker
container (Dev container, cmamp container).

    1. Build the thin environment; this is done once
        ```
        > source dev_scripts/client_setup/build.sh
        ```

    2. Activate the thin environment; make sure it is always activated
        ```
        > source dev_scripts/setenv_amp.sh
        ```

    3. If you see output like this, your environment is successfully built! If not and you encounter any issues, please post them under your designated on-boarding issue

        alias sp='echo '\''source ~/.profile'\''; source ~/.profile'
        alias vi='/usr/bin/vi'
        alias vim='/usr/bin/vi'
        alias vimdiff='/usr/bin/vi -d'
        alias vip='vim -c "source ~/.vimrc_priv"'
        alias w='which'
        ==> SUCCESS <==

### 2. Coding Style

- Adopt the coding style outlined -
  [here](https://github.com/sorrentum/sorrentum/blob/master/docs/coding/all.coding_style.how_to_guide.md).

#### Linter

The linter is in charge of reformatting the code according to our conventions
and reporting potential problems.

##### Run the linter and check the linter results

Run the linter against the changed files in the PR branch
`         invoke lint --branch         `

Check if the git client is clean `         git status         `

If the git client is not clean, abort the execution and the workflow will fail
If the git client is clean, the workflow will exit successfully

Invoke task for this action is:
`         invoke lint_check_if_it_was_run         `

-More information can be found in the above link for coding style.

-Internalize the guidelines to maintain code consistency.

### 3. Writing and Contributing Code

Follow these steps when writing and contributing code: - Always start with
creating an issue first, providing a summary of what you want to implement and
assign it to yourself and your team - Create a branch of your assigned
issues/bugs - E.g., for a GitHub issue with the name: "Expose the linter
container to Sorrentum contributors #63", The branch name should be :
"SorrTask63_Expose_the_linter_container_to_Sorrentum_contributors" - Run the
linter on your code before pushing. - Do 'git commit' and 'git push' together so
the latest changes are readily visible - Make sure your branch is up-to-date
with the master branch - Create a Pull Request (PR) from your branch - Add your
assigned reviewers for your PR so that they are informed of your PR - After
being reviewed, the PR will be merged to the master branch by your reviewers

### 4. Additional Document links

Refer the below links for a detailed description of every step mentioned

1. Introduction to Sorrentum -
   [Link](https://github.com/sorrentum/sorrentum/blob/master/docs/onboarding/all.sorrentum_intro.reference.md)
2. Sign up for Sorrentum Project -
   [Link](https://github.com/sorrentum/sorrentum/blob/master/docs/onboarding/all.sign_up_for_sorrentum.how_to_guide.md)
3. Sorrentum Development Guide -
   [Link](https://github.com/sorrentum/sorrentum/blob/master/docs/work_tools/all.sorrentum_development.how_to_guide.md)

#### By adhering to these conventions, we aim to create a collaborative and efficient coding environment for all contributors. Happy coding!
