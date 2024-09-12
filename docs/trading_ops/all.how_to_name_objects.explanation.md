

<!-- toc -->

- [How to name objects](#how-to-name-objects)
  * [Airflow DAGs](#airflow-dags)
  * [Git branch names](#git-branch-names)
  * [Docker image names](#docker-image-names)
  * [Directory names](#directory-names)
  * [Notebook names](#notebook-names)
  * [Telegram channels](#telegram-channels)
  * [Invoke task definition](#invoke-task-definition)

<!-- tocstop -->

# How to name objects

When we have lots of accounts, experiments, production systems running at the
same time, we need to standardize naming conventions.

This document specifies some standard nomenclature for the objects involved in
deployment of a system, e.g.,

- Airflow DAG names
- Git branch names
- Docker image names
- Directory names
- Notebook names
- Telegram channel names
- Invoke task definition

- The name of each object needs to have reference to its important metadata
- The schema and the values of each field should be listed and explained in this
  markdown

- Note that we prefer to spell `pre_prod` rather than `preprod`, since it's more
  readable

## Airflow DAGs

- The schema is `{stage}.{location}.{workload}.{subschema}` where:
  - `stage = {test, pre_prod, prod}`
  - `location = {tokyo, stockholm, ...}`
    - TODO(gp): It would be better to switch to the AWS region name
  - `workload` can be:
    - `scheduled_trading`: a system trading actual capital
      - TODO(gp): We should rename it to something better, e.g., `real_trading`.
        "Scheduled trading" is a bad name since we could trade not on a schedule
        with real money. "Prod trading" is a bad name since prod is a stage
    - `shadow_trading`: a system running a model but saving trades, instead of
      sending them to the exchange
    - `broker_experiment`: an experiment running the broker interface to collect
      information about market impact
    - `system_reconciliation`
    - `scheduled_trading_system_observer`: publish notebooks every N minutes to
      track the performance of a `scheduled_trading` or `shadow_trading` system
      - TODO(gp): Maybe rename `internal_monitoring`?
    - `external_monitoring`: publish notebooks every N minutes to represent the
      performance of a `scheduled_trading` or `shadow_trading` system for
      investors
  - `subschema` depends on the workload
    - E.g., for `*_trading` can be `{model}.{config}.[optional tag]`

- The path of the DAG in the source tree and in the deployment directory has the
  same name as the DAG itself

## Git branch names

- Branch names should follow the schema
  `{stage}.{workload}.{model}.{config}.{tag}`
- E.g., `pre_prod.shadow_trading.C11a.config1`
- If one branch is used for several values in a field (e.g., for all the
  workloads) we use `all` for the value of that field or just skip it
  - E.g., a Git branch used for all the models in `pre_prod` `scheduled_trading`
    is called `pre_prod.scheduled_trading`
- We can add as tag a date in the format `YYYYMMDD`

## Docker image names

- The name should be `cmamp.{stage}.{location}.{workload}`
- TODO(gp): Right now is `cmamp-test-tokyo-full-system`
  ```
  > i docker_create_candidate_image --task-definition "cmamp-test-tokyo-full-system" --user-tag "grisha" --region "ap-northeast-1"
  ```

## Directory names

- Derived by the Airflow DAG name with a timestamp

## Notebook names

- The schema is `{stage}.{location}.{workload}.{model}.{config}.{tag}`
- E.g., `prod.tokyo.system_observer.C11a.config1.last_5minutes`
- Current links are like
  http://172.30.2.44/system_reconciliation/C11a.config1.prod.last_5minutes.html

## Telegram channels

- Channels should be names referring to the `stage` and `workload` they refer to
  - E.g., `KT - pre_prod.scheduled_trading`

## Invoke task definition

- The format is like `{repo}.{subschema}` where:
  - `repo` is the name of the repo (e.g., `cmamp`)
  - `subschema` represents the rest of the information
- E.g., `cmamp.pre_prod.tokyo.scheduled_trading`
