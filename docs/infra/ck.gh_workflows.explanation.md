# GitHub Actions Workflows Explanation

<!-- toc -->

- [Tests](#tests)
- [Check if the linter was run](#check-if-the-linter-was-run)
- [Allure tests](#allure-tests)
- [Build dev image](#build-dev-image)
- [Build production `cmamp` image](#build-production-cmamp-image)
- [Test coverage](#test-coverage)
- [Import cycles detector](#import-cycles-detector)
- [Release new ECS `preprod` task definition](#release-new-ecs-preprod-task-definition)
- [Release new ECS `prod` task definition](#release-new-ecs-prod-task-definition)
- [Update amp submodule](#update-amp-submodule)

<!-- tocstop -->

This document describes all the GitHub actions workflows in the
`.github/workflows` directory of all the repos.

## Tests

Execute repository-wide tests, categorized by duration. Additional details
available at
[all.run_unit_tests.how_to_guide.md](../../docs/coding/all.run_unit_tests.how_to_guide.md#test-lists)

- Fast tests: [fast_tests.yml](../../.github/workflows/fast_tests.yml)
- Slow tests: [slow_tests.yml](../../.github/workflows/slow_tests.yml)
- Superslow tests:
  [superslow_tests.yml](../../.github/workflows/superslow_tests.yml)

## Check if the linter was run

Run the linter on the changed files of the PR. Workflow fails if the linter was
not executed before committing. See
[linter_gh_workflow.explanation.md](../../docs/infra/linter_gh_workflow.explanation.md)
for more details.

[linter.yml](../../.github/workflows/fast_tests.yml)

## Allure tests

Run the repository-wide tests with Allure. Generate an Allure Report based on
the test types. See
[all.pytest_allure.explanation.md](../../docs/infra/all.pytest_allure.explanation.md)
for more details.

- Allure fast tests:
  [allure.fast_tests.yml](../../.github/workflows/allure.fast_tests.yml)
- Allure slow tests:
  [allure.slow_tests.yml](../../.github/workflows/allure.slow_tests.yml)
- Allure superslow tests:
  [allure.superslow_tests.yml](../../.github/workflows/allure.superslow_tests.yml)

## Build dev image

Run the process for releasing the development image. See
[all.docker.how_to_guide.md](../../docs/work_tools/all.docker.how_to_guide.md#end-to-end-flow-for-dev-image)
for more details.

[build_image.dev.yml](../../.github/workflows/build_image.dev.yml)

## Build production `cmamp` image

Run the process for releasing the production image. See
[all.docker.how_to_guide.md](../../docs/work_tools/all.docker.how_to_guide.md#end-to-end-flow-for-prod-image)
for more details.

[build_image.cmamp.yml](../../.github/workflows/build_image.cmamp.yml)

## Test coverage

Run the fast and slow tests with the coverage, generate HTML-coverage report and
upload it to S3. See
[all.run_unit_tests.how_to_guide.md](../../docs/coding/all.run_unit_tests.how_to_guide.md#generate-coverage-report-with-invoke)
for more details.

[test_coverage.yml](../../.github/workflows/test_coverage.yml)

## Import cycles detector

Detect import cycles in the code. See:
[docs/coding/all.imports_and_packages.how_to_guide.md](#circular-dependency-aka-import-cycle-import-loop)
for more details.

[import_cycles_detector.yml](../../.github/workflows/import_cycles_detector.yml)

## Release new ECS `preprod` task definition

Releases a new ECS `preprod` task definition that is used for the `preprod`
Airflow DAGs. See examples of usage:

- [Airflow DAG development flow](https://docs.google.com/document/d/1C-22QF_gOe1k4HgyD6E6iOO_F_FxKKECd4MXaJEuTxo/edit#heading=h.w09szgua0lmq)
- [General flow of running experiment](../../docs/trade_execution/ck.full_system_execution_experiment.how_to_guide.md#general-flow)

[release_new_ecs_preprod_task_definition.yml](../../.github/workflows/release_new_preprod_ecs_task_definition.yml)

## Release new ECS `prod` task definition

Releases a new ECS `prod` task definition that is used for the `prod` Airflow
DAGs.

[release_new_ecs_prod_task_definition.yml](../../.github/workflows/release_new_prod_ecs_task_definition.yml)

## Update amp submodule

Check and update the `amp` submodule pointer in the `orange`, `dev_tools` and
`lemonade` repositories.

[update_amp_submodule.yml](https://github.com/cryptokaizen/orange/blob/master/.github/workflows/update_amp_submodule.yml)
