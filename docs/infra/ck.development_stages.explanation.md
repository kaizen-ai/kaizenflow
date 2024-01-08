<!--ts-->

<!--te-->

# Development stages

In order to allow seamless development and deployment process, we use a concept
of stages. A development stage is an isolated layer in the life-cycle of product
(service) delivery. The concept of stage allows developers to experiment and
roll-out new features without the risk of existing system impairment. Below is a
description of stages used throughout `cmamp` and it's related
projects/products.

## Local stage

- Environment used by a single developer
- The stage used for writing new code and running unit-tests on it
- Examples:
  - Docker container created by a developer to test a new feature
  - Jupyter notebook created by a developer to conduct research
  - Database container
- Breaking things in local stage is expected and has no impact on other team
  members

## Test stage

- Stage to freely perform any type of feature testing, experimenting running or
  reseatch which requires additional infrastructure not available locally.
- Breaking things in test stage has no consequences on business critical
  operations but can temporarily impact other developers since some resources
  are shared
  - Test database needs to be re-created if the schema is violated
  - Database table needs to be truncated if it's polluted by false data
- Examples:
  - Testing new Apache Airflow DAG in test stage
  - Interacting with postgres database (testing a new real time DataPull
    component)
- Shared resources can sometimes idle, used only when a developer needs them
- Local/Test stage is sometimes used interchangeably, in essence they are the
  same, the different lies in the notion of shared/isolated underlying HW
  resources.

## Pre-production stage

- AKA "preprod"
- Attempts to provide a 1:1 replica of production systems as reliably as
  possible
- Preprod stage is periodically updated by changes from master
- Resources are running 24/7
- Breaks do not impact business critical operations, but provide insights that
  the latest state of master might not be suitable to be deployed to production
  yet.
- Breaks/Errors are reported to relevant team members

## Production stage

- AKA "prod"
- Changes in master branch are only promoted to production after thorough
  confirmation of correctness in the preprod stage
- Each failure/error/impairment is reported to the relevant team members

### Design choices

Although we strive for complete isolation between stages, sometimes it is much
more convenient to (safely) expose a part of the interface. For example, a
developer might use read-only access to a preprod/prod database in order to use
data collected in real time data and quickly run an experiment.

Some underlying hardware resources are partially shared between stages in order
to simplify management and reduce costs. Examples:

- Test and Preprod Airflow DAGs run in a single underlying Airflow environment
- Test and Preprod RDS server instance hosts both test and preprod postgres
  databases.

Notifications about breaks are propagated to the team-members using mailing and
telegram group, more information in the corresponding docs:

- TODO(Juraj): add link to mailing group documentation #CmTask5529
- TODO(Juraj): add link to telegram documentation #CmTask5737

Additional resources:

- TODO(Juraj): Add link to documentation about RDS
- TODO(Juraj): Add link to documentation about Airflow
- TODO(Juraj): Add link to the preprod/prod deployment documentation
