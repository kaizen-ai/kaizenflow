# Thin environment dependencies

<!-- toc -->

- [Description](#description)
- [Change in requirements file](#change-in-requirements-file)
- [Confirm with Build team](#confirm-with-build-team)
- [Update requirements file](#update-requirements-file)
- [Update Documentation](#update-documentation)
- [Notify Team](#notify-team)

<!-- tocstop -->

## Description

- We have 3 sources of package requirements in the project:

  1. The thin environment to run `invoke` targets outside the container
     - [/dev_scripts/client_setup/requirements.txt](/dev_scripts/client_setup/requirements.txt)
     - This is managed with `pip`
  2. GitHub requirements used for GitHub Actions specifically
     - [/.github/gh_requirements.txt](/.github/gh_requirements.txt)
     - This is managed with `pip`
  3. Requirements necessary for the container:
     - [/devops/docker_build/pyproject.toml](/devops/docker_build/pyproject.toml)
     - This is managed with `poetry`

- We want to keep the thin environment as "thin" as possible (i.e., with fewer
  dependencies)
- The thin environment and GitHub requirements have to be in sync
  - The only difference is that the GitHub requirements have some limitations
    due to the GitHub Actions environment
  - TODO(Vlad): Still not clear what exact difference between the two
    requirements files

- This document provides a step-by-step guide for adding or make any changes in
  the requirements file of both the thin env and GitHub

## Change in requirements file

- Some reasons for updating/changing the `requirements.txt` file are:
  - A new feature requires a new package outside the container, e.g., a new or
    updated `invoke` target
  - Upgrading the package version since the current one is outdated
  - Removing a package since it is not used anymore

## Confirm with Build team

- Changes in any of the requirement files should be confirmed with the Build
  team before merging the PR
  - Is the new dependencies really needed?
  - If the new dependencies is really needed, can we limit the scope of the
    dependency? E.g.,
    - Move the related imports to where it is strictly needed in the code
    - Do a try-catch `ImportError`

Example:

- The [/helpers/lib_tasks_gh.py](/helpers/lib_tasks_gh.py) module has some
  `invoke` targets that are executed only in the container
- If the new package is needed for the `invoke` target only in the container, we
  should move the import to the function where it is strictly needed
- See the `gh_publish_buildmeister_dashboard_to_s3()` in the
  [/helpers/lib_tasks_gh.py](https://github.com/cryptokaizen/cmamp/blob/master/helpers/lib_tasks_gh.py#L469)
  for reference.

## Update requirements file

- Update both the requirements file if relevant
  [/dev_scripts/client_setup/requirements.txt](/dev_scripts/client_setup/requirements.txt)
  and [/.github/gh_requirements.txt](/.github/gh_requirements.txt)
  - This file should be changed in every repository (e.g., `cmamp`,
    `kaizenflow`, `orange`)
- After adding the new requirements the build team will run all the tests
  locally as well as on GitHub

## Update Documentation

- Update the
  [/docs/dev_tools/thin_env/all.gh_and_thin_env_requirements.reference.md](/docs/dev_tools/thin_env/all.gh_and_thin_env_requirements.reference.md)

## Notify Team

In the @all Telegram channel, notify the team about the new package and ask them
to rebuild the thin env.

Example:
```
Hi! In the PR: https://github.com/cryptokaizen/cmamp/pull/6800 we removed
unused packages from the thin environment.

You need to update the thin environment by running:

> cd ~/src/cmamp1
> dev_scripts/client_setup/build.sh
```

Last review: GP on 2024-05-07
