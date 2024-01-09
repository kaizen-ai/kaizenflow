# Required Packages for the thin environment and GH Actions

<!-- toc -->

- [Thin environment](#thin-environment)
  * [Packages](#packages)
  * [Candidate Packages to remove](#candidate-packages-to-remove)
- [GH Actions](#gh-actions)
  * [Packages](#packages-1)
  * [Candidate Packages to remove](#candidate-packages-to-remove-1)

<!-- tocstop -->

## Thin environment

File location:

- [requirements.txt](https://github.com/cryptokaizen/cmamp/blob/master/dev_scripts/client_setup/requirements.txt)

### Packages

- `boto3`
  - Interacts with the AWS services:
  - [`boto3` import in the `haws`](https://github.com/cryptokaizen/cmamp/blob/master/helpers/haws.py#L10)
  - [`haws` usage in the `lib_tasks_docker_release.py`](https://github.com/cryptokaizen/cmamp/blob/master/helpers/lib_tasks_docker_release.py#L862)

- `invoke`
  - Need for running the invoke targets:
  - [\_run_tests](https://github.com/cryptokaizen/cmamp/blob/master/helpers/lib_tasks_pytest.py#L299)

- `poetry`
  - Manage dependencies in the dev image:
  - [docker_build_local_image](https://github.com/cryptokaizen/cmamp/blob/master/helpers/lib_tasks_docker_release.py#L119)

- `pytest`
  - To run `Docker image QA tests`:
  - [\_run_qa_tests](https://github.com/cryptokaizen/cmamp/blob/master/helpers/lib_tasks_docker_release.py#L119)

- `pyyaml`
  - Used once for checking the generated docker-compose.yaml file in the:
  - [\_generate_docker_compose_file](https://github.com/cryptokaizen/cmamp/blob/master/helpers/lib_tasks_docker.py#L683)

- `tqdm`
  - Widely used for showing the progress of the process for example:
  - [\_fix_invalid_owner](https://github.com/cryptokaizen/cmamp/blob/master/helpers/lib_tasks_perms.py#L243)

- `s3fs`
  - Needed for some invoke targets, for example:
  - [docker_update_prod_task_definition](https://github.com/cryptokaizen/cmamp/blob/CmampTask6520_gDoc_for_required_packages_in_github_workflow_and_thin_env/helpers/lib_tasks_docker_release.py#L866)

### Candidate Packages to remove

- Packages used in container only
  - `pytest-cov`
  - `pytest-instafail`
  - `pytest-xdist`

- `awscli`

  Need to move AWS CLI installation to the os packages
  https://github.com/cryptokaizen/cmamp/issues/526

- `docker` and `docker-compose`

  Should to be moved to os installation
  https://github.com/cryptokaizen/cmamp/issues/6498

## GH Actions

File location:

- [gh_requirements.txt](https://github.com/cryptokaizen/cmamp/blob/master/.github/gh_requirements.txt)

### Packages

- `invoke`
  - Need for running the invoke targets:
  - [\_run_tests](https://github.com/cryptokaizen/cmamp/blob/master/helpers/lib_tasks_pytest.py#L299)

- `poetry`
  - Manages dependencies in the dev image:
  - [docker_build_local_image](https://github.com/cryptokaizen/cmamp/blob/master/helpers/lib_tasks_docker_release.py#L119)

- `pytest` and `pytest-cov`
  - To run `Test coverage GH workflow`:
  - [`run_coverage_report`](https://github.com/cryptokaizen/cmamp/blob/master/helpers/lib_tasks_pytest.py#L794)

- `pyyaml`
  - Used once for checking the generated docker-compose.yaml file in the:
  - [\_generate_docker_compose_file](https://github.com/cryptokaizen/cmamp/blob/master/helpers/lib_tasks_docker.py#L683)

- `tqdm`
  - Widely used for showing the progress of the process for example:
  - [\_fix_invalid_owner](https://github.com/cryptokaizen/cmamp/blob/master/helpers/lib_tasks_perms.py#L243)

- `s3fs`
  - Needed for some invoke targets, for example:
  - [docker_update_prod_task_definition](https://github.com/cryptokaizen/cmamp/blob/CmampTask6520_gDoc_for_required_packages_in_github_workflow_and_thin_env/helpers/lib_tasks_docker_release.py#L866)

### Candidate Packages to remove

- `flaky` There is no dependencies at all. Some examples that could be found in
  the
  [TestRunNotebook1](https://github.com/cryptokaizen/cmamp/blob/master/dev_scripts/test/test_run_notebook.py#L224)
  ```
  @pytest.mark.flaky(reruns=2)
  ```

  …actually this is the `pytest-rerunfailures` usage.

- Packages used in container only
  - `pytest-instafail`
  - `pytest-xdist`

- `numpy` and `pandas` already deleted in the `requirements.txt`.

- `python-dateutil`

  Example of usage:
  [`_LocalTimeZoneFormatter.__init__`](https://github.com/cryptokaizen/cmamp/blob/master/helpers/hlogging.py#L224)

  The `hlogging` is the one of the central package of the `cmamp` repo.

  It’s weird, but `python-dateutil` exists in the `gh_requirements.txt` but at
  the same time it’s in the `poetry.lock` and not in the `pyproject.toml`.
  Definitely have to be refactored.

  https://github.com/cryptokaizen/cmamp/issues/6617

- `awscli` Need to move AWS CLI installation to the os packages
  https://github.com/cryptokaizen/cmamp/issues/526
- `docker` and `docker-compose` should to be moved to os installation
  https://github.com/cryptokaizen/cmamp/issues/6498
