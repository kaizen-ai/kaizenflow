# Python package upgrade & troubleshooting

<!-- toc -->

- [Description](#description)
- [Building the local image](#building-the-local-image)
- [Run the tests](#run-the-tests)
- [Document Errors](#document-errors)
- [Identify forward-compatible fixes](#identify-forward-compatible-fixes)
- [Handle non-forward compatible fixes](#handle-non-forward-compatible-fixes)
- [Image Release](#image-release)
- [Merge Base PR](#merge-base-pr)

<!-- tocstop -->

## Description

- Upgrading Python or its packages is a complex process requiring significant
  attention and time
- This document aims to provide a step-by-step guide for upgrading Python or its
  packages
- Several steps are outlined to be followed to avoid any potential issues
- The primary objective is to streamline the upgrade process, ensuring it
  unfolds seamlessly while mitigating any potential issues that might arise
  along the way

## Building the local image

- Upgrade the packages versions in the
  [/devops/docker_build/pyproject.toml](devops/docker_build/pyproject.toml)
- After the upgrade, the first step is to build the local image
- The build command is described in the
  [/docs/work_tools/all.docker.how_to_guide.md#multi-architecture-build](/docs/work_tools/all.docker.how_to_guide.md#multi-architecture-build)

## Run the tests

- Create a new task-specific gdoc, e.g., CmTask7256_Upgrade_Pandas.docx
- Run all the tests from the orange repo against the new dev image
  - Fast
  - Slow
  - Superslow
  - QA
  - Example: `i run_fast_tests --stage local --version {new version}`

## Document Errors

- Collect all the failures in the GDOC:
  - Test name
  - Traceback
- Group errors by type, e.g.,
  ```
  # Error type 1
  ## Test name 1
  traceback
  ## Test name 2
  traceback
  # Error type 2
  ## Test name 1
  traceback
  ## Test name 2
  traceback
  # Error type N ...
  ```

## Identify forward-compatible fixes

Forward-compatible fix is a fix that works on both versions of a Python package,
i.e. on the current one and the target one.

- Identify forward-compatible changes
- Move them to a separate GDOC section to isolate from the other changes
- For each forward-compatible fix file a separate GH issue and a separate PR to
  master

## Handle non-forward compatible fixes

- File an issue for the error
- Describe the solution in the GDOC
- Apply changes to the main PR
- Run the tests for this error type and make sure they do not fail
- Repeat this process for all the types of errors
- Once resolved, run all the regressions locally on the new local image from the
  orange repo and make sure the regressions are green

## Image Release

- Run release command described in the
  [/docs/work_tools/all.docker.how_to_guide.md#command-to-run-the-release-flow](/docs/work_tools/all.docker.how_to_guide.md#command-to-run-the-release-flow)
- Follow the post-release check-list in the
  [/docs/work_tools/all.docker.how_to_guide.md#post-release-check-list](/docs/work_tools/all.docker.how_to_guide.md#post-release-check-list)

## Merge Base PR

- Run the regressions from GitHub on the main PR
- Check that the GitHub Actions is picking up new image and all regressions
  (fast, slow, superslow) are green on `cmamp`, `orange` and `kaizenflow`
- Merge the main PR on all the repos (`cmamp`, `orange`, `kaizenflow`)
