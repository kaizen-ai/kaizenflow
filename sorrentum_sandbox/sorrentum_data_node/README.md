# User notes

- This implements a dockerized Airflow system that can run on a machine locally

## 
- Inspect the Dockerfile and the compose file:
  ```
  vi docker-compose.yml
  vi Dockerfile
  vi .env
  ```
- Export environment variables to set up proper access to volumes
 ```
 > export UID=$(id -u)
 ```

- Build the Airflow container and bring up the service
  ```
  > cd devops/compose/airflow/
  > docker-compose up
  ```

- Building the container for the first time takes a few minutes

- The best approach is to run Airflow server in one Terminal window so one can
  see the logs

- Note that there can be some errors / warnings, but things are good as long as
  you see Airflow starting like below
```
airflow_scheduler_cont  |   ____________       _____________
airflow_scheduler_cont  |  ____    |__( )_________  __/__  /________      __
airflow_scheduler_cont  | ____  /| |_  /__  ___/_  /_ __  /_  __ \_ | /| / /
airflow_scheduler_cont  | ___  ___ |  / _  /   _  __/ _  / / /_/ /_ |/ |/ /
airflow_scheduler_cont  |  _/_/  |_/_/  /_/    /_/    /_/  \____/____/|__/
airflow_scheduler_cont  | [2022-11-10 17:09:29,521] {scheduler_job.py:596} INFO - Starting the scheduler
airflow_scheduler_cont  | [2022-11-10 17:09:29,522] {scheduler_job.py:601} INFO - Processing each file at most -1 times
airflow_scheduler_cont  | [2022-11-10 17:09:29,531] {manager.py:163} INFO - Launched DagFileProcessorManager with pid: 20
airflow_scheduler_cont  | [2022-11-10 17:09:29,533] {scheduler_job.py:1114} INFO - Resetting orphaned tasks for active dag runs
airflow_scheduler_cont  | [2022-11-10 17:09:29 +0000] [19] [INFO] Starting gunicorn 20.1.0
airflow_scheduler_cont  | [2022-11-10 17:09:29,539] {settings.py:52} INFO - Configured default timezone Timezone('UTC')
airflow_scheduler_cont  | [2022-11-10 17:09:29 +0000] [19] [INFO] Listening at: http://0.0.0.0:8793 (19)
airflow_scheduler_cont  | [2022-11-10 17:09:29 +0000] [19] [INFO] Using worker: sync
airflow_scheduler_cont  | [2022-11-10 17:09:29 +0000] [21] [INFO] Booting worker with pid: 21
airflow_scheduler_cont  | [2022-11-10 17:09:29 +0000] [22] [INFO] Booting worker with pid: 22
```

- You can see the service:
  ```
  > docker container ls
  CONTAINER ID   IMAGE                                                     COMMAND                  CREATED          STATUS                  PORTS                                                                    NAMES
  454e70e42b9f   resdev-airflow:latest                                     "/usr/bin/dumb-init …"   12 seconds ago   Up Less than a second   8080/tcp                                                                 airflow_scheduler_cont
  5ac4c7f6d883   resdev-airflow:latest                                     "/usr/bin/dumb-init …"   12 seconds ago   Up 10 seconds           0.0.0.0:8090->8080/tcp                                                   airflow_cont
  37e86b58c247   postgres:14.0                                             "docker-entrypoint.s…"   13 seconds ago   Up 11 seconds           0.0.0.0:5532->5432/tcp
  postgres_cont

  > docker images  | grep airflow
  resdev-airflow                                            latest               3e9a5e7a5144   5 hours ago     1.98GB

  > docker volume ls
  DRIVER    VOLUME NAME
  local     airflow_ck-airflow-database-data
  local     airflow_ck-airflow-log-volume
  ```

- Take a look at:
  ```
  > vi ./init_airflow_setup.sh
  ```

- When starting the containers for the first time you need to initialize Airflow,
  so in a different Terminal window run:
  ```
  > ./init_airflow_setup.sh
  ...
  [2022-11-10 17:09:38,394] {manager.py:214} INFO - Added user airflow
  User "airflow" created with role "Admin"
  Created airflow Initial admin user with username airflow
  ```

- On your local machine go to the browser to `localhost:8090` (or to the IP
  `172.30.2.136:8090`)
- The default login credentials are `airflow`:`airflow`

- You can check the version with:
  ```
  > docker_bash.sh
  airflow@d47496583402:/opt/airflow$ airflow version
  2.2.2
  ```

## Managing Airflow

- You can bring down the service (persisting the state) with:
  ```
  > docker compose down

  > docker container ls
  > docker volume ls
  ```
- The containers are only paused

- Restarting the service keeps the volume which contains the state of Postgres:
  ```
  docker compose restart

  > docker container ls
  > docker volume ls
  ```
- The containers are recreated

- To remove all containers and volumes:
  ```
  > docker-compose down -v --rmi all
  Removing airflow_scheduler_cont ... done
  Removing airflow_cont           ... done
  Removing postgres_cont          ... done
  Removing network airflow_default
  Removing volume airflow_ck-airflow-database-data
  Removing volume airflow_ck-airflow-log-volume
  Removing image postgres:14.0
  Removing image resdev-airflow:latest

  > docker container ls
  > docker volume ls
  ```
- Since you are starting from scratch here you need to re-run
  `./init_airflow_setup.sh`

## CLI

- List the DAGs
  ```
  > docker_cmd.sh airflow dags list
  + docker exec -ti airflow_cont airflow dags list
  dag_id   | filepath            | owner   | paused
  =========+=====================+=========+=======
  tutorial | airflow_tutorial.py | airflow | False
  ```

# print the list of active DAGs
airflow dags list

# prints the list of tasks in the "tutorial" DAG
airflow tasks list tutorial

# prints the hierarchy of tasks in the "tutorial" DAG
airflow tasks list tutorial --tree


## Notes
- The current solution uses `SequentialExecutor` so task parallelization is not
  possible
- To remove this limitation we can add the rest of the infrastructure needed to
  enable parallelization (e.g., celery and redis)

# Dev notes
- To rebuild after trying out some changes in dockerfile/compose file
```
> docker-compose up --build --force-recreate
```

# User notes

- This implements a dockerized Airflow system that can run on a machine locally

## 
- Inspect the Dockerfile and the compose file:
  ```
  vi docker-compose.yml
  vi Dockerfile
  vi .env
  ```
- Export environment variables to set up proper access to volumes
 ```
 > export UID=$(id -u)
 ```

- Build the Airflow container and bring up the service
  ```
  > cd devops/compose/airflow/
  > docker-compose up
  ```

- Building the container for the first time takes a few minutes

- The best approach is to run Airflow server in one Terminal window so one can
  see the logs

- Note that there can be some errors / warnings, but things are good as long as
  you see Airflow starting like below
```
airflow_scheduler_cont  |   ____________       _____________
airflow_scheduler_cont  |  ____    |__( )_________  __/__  /________      __
airflow_scheduler_cont  | ____  /| |_  /__  ___/_  /_ __  /_  __ \_ | /| / /
airflow_scheduler_cont  | ___  ___ |  / _  /   _  __/ _  / / /_/ /_ |/ |/ /
airflow_scheduler_cont  |  _/_/  |_/_/  /_/    /_/    /_/  \____/____/|__/
airflow_scheduler_cont  | [2022-11-10 17:09:29,521] {scheduler_job.py:596} INFO - Starting the scheduler
airflow_scheduler_cont  | [2022-11-10 17:09:29,522] {scheduler_job.py:601} INFO - Processing each file at most -1 times
airflow_scheduler_cont  | [2022-11-10 17:09:29,531] {manager.py:163} INFO - Launched DagFileProcessorManager with pid: 20
airflow_scheduler_cont  | [2022-11-10 17:09:29,533] {scheduler_job.py:1114} INFO - Resetting orphaned tasks for active dag runs
airflow_scheduler_cont  | [2022-11-10 17:09:29 +0000] [19] [INFO] Starting gunicorn 20.1.0
airflow_scheduler_cont  | [2022-11-10 17:09:29,539] {settings.py:52} INFO - Configured default timezone Timezone('UTC')
airflow_scheduler_cont  | [2022-11-10 17:09:29 +0000] [19] [INFO] Listening at: http://0.0.0.0:8793 (19)
airflow_scheduler_cont  | [2022-11-10 17:09:29 +0000] [19] [INFO] Using worker: sync
airflow_scheduler_cont  | [2022-11-10 17:09:29 +0000] [21] [INFO] Booting worker with pid: 21
airflow_scheduler_cont  | [2022-11-10 17:09:29 +0000] [22] [INFO] Booting worker with pid: 22
```

- You can see the service:
  ```
  > docker container ls
  CONTAINER ID   IMAGE                                                     COMMAND                  CREATED          STATUS                  PORTS                                                                    NAMES
  454e70e42b9f   resdev-airflow:latest                                     "/usr/bin/dumb-init …"   12 seconds ago   Up Less than a second   8080/tcp                                                                 airflow_scheduler_cont
  5ac4c7f6d883   resdev-airflow:latest                                     "/usr/bin/dumb-init …"   12 seconds ago   Up 10 seconds           0.0.0.0:8090->8080/tcp                                                   airflow_cont
  37e86b58c247   postgres:14.0                                             "docker-entrypoint.s…"   13 seconds ago   Up 11 seconds           0.0.0.0:5532->5432/tcp
  postgres_cont

  > docker images  | grep airflow
  resdev-airflow                                            latest               3e9a5e7a5144   5 hours ago     1.98GB

  > docker volume ls
  DRIVER    VOLUME NAME
  local     airflow_ck-airflow-database-data
  local     airflow_ck-airflow-log-volume
  ```

- Take a look at:
  ```
  > vi ./init_airflow_setup.sh
  ```

- When starting the containers for the first time you need to initialize Airflow,
  so in a different Terminal window run:
  ```
  > ./init_airflow_setup.sh
  ...
  [2022-11-10 17:09:38,394] {manager.py:214} INFO - Added user airflow
  User "airflow" created with role "Admin"
  Created airflow Initial admin user with username airflow
  ```

- On your local machine go to the browser to `localhost:8090` (or to the IP
  `172.30.2.136:8090`)
- The default login credentials are `airflow`:`airflow`

- You can check the version with:
  ```
  > docker_bash.sh
  airflow@d47496583402:/opt/airflow$ airflow version
  2.2.2
  ```

## Managing Airflow

- You can bring down the service (persisting the state) with:
  ```
  > docker compose down

  > docker container ls
  > docker volume ls
  ```
- The containers are only paused

- Restarting the service keeps the volume which contains the state of Postgres:
  ```
  docker compose restart

  > docker container ls
  > docker volume ls
  ```
- The containers are recreated

- To remove all containers and volumes:
  ```
  > docker-compose down -v --rmi all
  Removing airflow_scheduler_cont ... done
  Removing airflow_cont           ... done
  Removing postgres_cont          ... done
  Removing network airflow_default
  Removing volume airflow_ck-airflow-database-data
  Removing volume airflow_ck-airflow-log-volume
  Removing image postgres:14.0
  Removing image resdev-airflow:latest

  > docker container ls
  > docker volume ls
  ```
- Since you are starting from scratch here you need to re-run
  `./init_airflow_setup.sh`

## CLI

- List the DAGs
  ```
  > docker_cmd.sh airflow dags list
  + docker exec -ti airflow_cont airflow dags list
  dag_id   | filepath            | owner   | paused
  =========+=====================+=========+=======
  tutorial | airflow_tutorial.py | airflow | False
  ```

# print the list of active DAGs
airflow dags list

# prints the list of tasks in the "tutorial" DAG
airflow tasks list tutorial

# prints the hierarchy of tasks in the "tutorial" DAG
airflow tasks list tutorial --tree


## Notes
- The current solution uses `SequentialExecutor` so task parallelization is not
  possible
- To remove this limitation we can add the rest of the infrastructure needed to
  enable parallelization (e.g., celery and redis)
- TODO(Juraj): it might be sufficient to change to `LocalExecutor` which also allows parallelization

# Dev notes
- To rebuild after trying out some changes in dockerfile/compose file
```
> docker-compose up --build --force-recreate
```

## Data Pipeline Examples

- The following examples demonstrate small standalone data pipelines
- The code can be found under `sorrentum_sandbox/examples`  
- Each example implements concrete classes from interfaces specified in `sorrentum_sandbox/`, upon which a command line scripts are built.
- The actual execution of scripts is orchestrated by Apache Airflow

### Binance

- In this example, we utilize Binance REST API, available free of charge. We build a small ETL pipeline used to download and transform OHLCV market data for selected cryptocurrencies

#### First steps

1. The example code can be found in `sorrentum_sandbox/examples/binance`
2. To get to know what type of data we are working with in this example you can run
```
> sorrentum_sandbox/examples/binance/download_to_csv.py \
    --start_timestamp '2022-10-20 10:00:00+00:00' \
    --end_timestamp '2022-10-21 15:30:00+00:00' \
    --target_dir 'binance_data'
```
  - The script downloads ~1 day worth of OHLCV (or candlestick) into a csv
3. To familiarize yourself with the concepts of data quality assurance/validation you can proceed with the example script `example_load_and_validate.py` which runs a trivial data QA operations (i.e. checkign the dataset is not empty)
```
> example_load_and_validate.py \
    --start_timestamp '2022-10-20 12:00:00+00:00' \
    --end_timestamp '2022-10-21 12:00:00+00:00' \
    --source_dir 'binance_data' \
    --dataset_signature 'bulk.manual.download_1min.csv.ohlcv.spot.v7.binance.binance.v1_0_0'
```

#### Quickstart using Airflow

1. Bring up the services via docker-compose as described above
2. Visit localhost:8090/home
3. Sign in using credentials (the defaults are airflow:airflow)
4. There are two Airflow DAGs preloaded for this example
- `download_periodic_1min_postgres_ohlcv` - by default scheduled to run every minute and download last minute worth of OHLCV data using `sorrentum_sandbox/examples/binance/download_to_db.py`
- `download_periodic_1min_postgres_ohlcv` - by default scheduled to run every 5 minutes, load data from a postgres table, resample and save back
5. TODO(Juraj): rather then explaining how to switch on a DAG in every example we can just add a separate section above.
