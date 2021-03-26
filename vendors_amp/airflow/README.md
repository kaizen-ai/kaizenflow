<!--ts-->
   * [Current versions](#current-versions)
   * [Run instructions](#run-instructions)
      * [LOCAL stage](#local-stage)
      * [DEV stage](#dev-stage)
      * [PRE_PROD stage](#pre_prod-stage)
      * [PROD stage](#prod-stage)
   * [Infra](#infra)
      * [Servers](#servers)
         * [DEV stage](#dev-stage-1)
         * [PRE_PROD stage](#pre_prod-stage-1)
         * [PROD stage](#prod-stage-1)
      * [AWS credentials](#aws-credentials)
      * [Logging](#logging)
      * [Monitoring](#monitoring)
   * [Release procedure](#release-procedure)
   * [Edgar DAGs description](#edgar-dags-description)
      * [Infra DAGs](#infra-dags)
      * [Edgar apps](#edgar-apps)

<!--te-->

# Current versions


- Production images:

- Releases:

- Email list for Airflow notifications: 
  
# Run instructions

## LOCAL stage

## DEV stage

## PRE_PROD stage

## PROD stage

# Infra

## Servers

### DEV stage

- We have one instances running all the services

- One server for Postgres (that is outside the stack)
- Located on server `` TODO(Sergey, Vitaliy): Change to outside PostgreSQL DEV DB 

- Developer server: `im.dev.p1`
  - AirFlow internal web UI: http://im.dev.p1:8080
    - Username: ``
    - Password: ``
  - Redis
  - AirFlow Postgres DB
  
  - IM KIBOT extract worker
  - IM KIBOT transform and load worker
  
  - IM IB extract worker
  - IM IB transform and load worker
  
  - IM daily sync worker 


### PRE_PROD stage

- We have one instances running all the services

- One server for Postgres (that is outside the stack)
- Located on server `` TODO(Sergey, Vitaliy): Change to outside PostgreSQL PRE-PROD DB  

- Pre-prod server `im.pre-prod.p1`
  - AirFlow internal web UI: http://im.pre-prod.p1:8080
    - Username: ``
    - Password: ``
  - Redis
  - AirFlow Postgres DB
  
  - IM KIBOT extract worker
  - IM KIBOT transform and load worker
  
  - IM IB extract worker
  - IM IB transform and load worker
  
  - IM daily sync worker 



### PROD stage

- We have:
  - One instance under control of the new infrastructure;
  - One server for Postgres (that is outside the stack)
    - Located on server ``

- Pre-prod server `im.pre-prod.p1`
  - AirFlow internal web UI: http://im.prod.p1:8080
    - Username: ``
    - Password: ``
  - Redis
  - AirFlow Postgres DB
  
  - IM KIBOT extract worker
  - IM KIBOT transform and load worker
  
  - IM IB extract worker
  - IM IB transform and load worker
  
  - IM daily sync worker 

- EDGAR Postgres pre-prod server: `pg-im.prod.p1`
  - It contains only the Postgres database. Outside the stack.
  - We added proper DNS name that points to the old server.
  - Alternative server names ``, ``.

## AWS credentials

- AWS credentials stored in `~/.aws/credentials`
  - Stage: `local`
  - We assume that developers should have the following access policy for S3
    - Associated policies:
      - Name: `im-user-S3`
      - Permissions for S3 buckets:
        - `im-store-local`: RW
        - `im-store-dev`: RW
        - `im-store-pre-prod`: R
        - `im-store`: R
  - TODO(\*): to think about FSX policies
    - `im-user-FSX`

- AWS credentials stored in as environment variables
  - Stage: `test`

- IAM roles
  - Stage: `dev`
    - Role name: `IM-dev`
    - Associated policies:
      - `im-s3-dev`
        - Permissions for S3 buckets:
          - `im-store-dev`: RW
      - TODO(\*): add FSX policy
  - Stage: `pre-prod`
    - Role name: `IM-pre-prod`
    - Associated policies:
      - `im-s3-pre-prod`
        - Permissions for S3 buckets:
          - `im-store-pre-prod`: RW
      - TODO(\*): add FSX policy
  - Stage: `prod`
    - Role name: `IM-prod`
    - Associated policies:
      - `im-s3-prod`
        - Permissions for S3 buckets:
          - `im-store`: RW
      - TODO(\*): add FSX policy

## Logging

## Monitoring

- [Grafana](http://monitoring.p1/)

# Release procedure

- Test the code on dev server
  - Run manual tests
- Build production images (TODO: Check/create GH action for this)
- Test code and images on pre-prod stage
  - Run manual tests
- Add release tags for prod images (TODO: Check/create GH action for this)
- Update the README.md and changelog.md
- Create release on GH
- Roll-out the release on prod-stage
  - Run manual tests

# Edgar DAGs description

- Edgar DAG templates are located in `im/airflow/dags` folder

## Infra DAGs


## IM apps

