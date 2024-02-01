

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
  * [5. Additional Document links](#5-additional-document-links)
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
- If you are using Windows,
  - Install VMWare software -
    [Reference video for installing ubuntu on VMWare software ](https://youtu.be/NhlhJFKmzpk?si=4MMOYzLnhyP4eSj2)
  - Install
    [docker](https://docs.docker.com/engine/install/ubuntu/#install-using-the-repository)
    on your Ubuntu VM
- Make sure you set up your git and github

### Cloning the Code

- All the source code should go under ~/src (e.g., /Users/<YOUR_USER>/src on a
  Mac PC)
- The path to the local repo folder should look like this ~/src/{REPO_NAME}{IDX}
  where
  - IDX is an integer
  - REPO_NAME is a name of the repository

- To clone the repo, use the cloning command described in the Github official
  documentation

- Example of cloning command:
  - The following command might not work sometimes
  ```
  > git clone git@github.com:sorrentum/sorrentum.git ~/src/sorrentum1
  ```
  - Alternative command.
  ```
  > git clone https://github.com/sorrentum/sorrentum.git ~/src/sorrentum1
  ```

### Building the Thin Environment

- Create a thin environment for running the linter using the Sorrentum Dev
  Dockercontainer (Dev container, cmamp container).

- Build the thin environment; this is done once
```
> source dev_scripts/client_setup/build.sh
```

- Activate the thin environment; make sure it is always activated
```
> source dev_scripts/setenv_amp.sh
```

- If you see output like this, your environment is successfully built! If not
  and you encounter any issues, please post them under your designated
  on-boarding issue
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
  and reporting potential problems.

### Run the linter and check the linter results

- Run the linter against the changed files in the PR branch
```
invoke lint --branch
```

- Check if the git client is clean
```
git status
```

- If the git client is not clean, abort the execution and the workflow will fail
- If the git client is clean, the workflow will exit successfully

- Invoke task for this action is:
```
invoke lint_check_if_it_was_run
```

- More information about Linter -
  [Link](https://github.com/sorrentum/sorrentum/blob/master/docs/infra/linter_gh_workflow.explanation.md)
- Internalize the guidelines to maintain code consistency.

## 4. Writing and Contributing Code

- Follow these steps when writing and contributing code:
- Always start with creating an issue first, providing a summary of what you
  want to implement and assign it to yourself and your team
- Create a branch of your assigned issues/bugs
  - E.g., for a GitHub issue with the name: "Expose the linter container to
    Sorrentum contributors #63", The branch name should be :
    "SorrTask63_Expose_the_linter_container_to_Sorrentum_contributors"
- Run the linter on your code before pushing
- Do 'git commit' and 'git push' together so the latest changes are readily
  visible
- Make sure your branch is up-to-date with the master branch
- Create a Pull Request (PR) from your branch - Add your assigned reviewers for
  your PR so that they are informed of your PR
- Afterbeing reviewed, the PR will be merged to the master branch by your
  reviewers

## 5. Additional Document links

Refer the below links for a detailed description of every step mentioned

- Introduction to Sorrentum -
  [Link](https://github.com/sorrentum/sorrentum/blob/master/docs/onboarding/all.sorrentum_intro.reference.md)
- Sign up for Sorrentum Project -
  [Link](https://github.com/sorrentum/sorrentum/blob/master/docs/onboarding/all.sign_up_for_sorrentum.how_to_guide.md)
- Sorrentum Development Guide -
  [Link](https://github.com/sorrentum/sorrentum/blob/master/docs/work_tools/all.sorrentum_development.how_to_guide.md)

#### By adhering to these conventions, we aim to create a collaborative and efficient coding environment for all contributors. Happy coding!
