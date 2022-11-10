# User notes

- This implements a dockerized Airflow system that can run on a machine locally

## 
- Inspect the Dockerfile and the compose file:
  ```
  vi docker-compose.yml
  vi Dockerfile
  vi .env
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

- To remove all containers and volumes
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
