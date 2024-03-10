# Sorrentum system container

## High-level description

- This system contains several services:
  - Airflow
  - Postgres
  - MongoDB

- It is modeled after https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

- Inspect the Dockerfile and the compose file to understand what's happening
  behind the scenes
  ```
  > cd $GIT_ROOT/sorrentum_sandbox/devops
  > vi docker-compose.yml Dockerfile .env
  ```

- The system needs three Docker images:
  - `postgres`: pre-built image for PostgreSQL, downloaded directly from
    DockerHub
  - `mongo`: pre-built image for Mongo, downloaded directly from DockerHub
  - `sorrentum/sorrentum`: the image can be either built locally or downloaded
    from DockerHub
    - It is used to run Airflow and the Sorrentum application
    - The container containing the application is `airflow_cont`

  <!--
  // Generated with:
  // cd ~/src/sorrentum1/sorrentum_sandbox/devops
  // docker run --rm -it --name dcv -v $(pwd):/input pmsipilot/docker-compose-viz render -m image docker-compose.yml --no-volumes --force
  -->
  ![image](https://user-images.githubusercontent.com/33238329/223691802-0f0ec9ce-9854-48a7-9a30-1a8d452f77ce.png)
  
## Scripts

- Remember that commands prepended with:
  - `>` are run outside the Sorrentum app container in a terminal of your local
    computer
  - `docker>` are run inside the Sorrentum app container after running
    `docker_bash.sh` or `docker_exec.sh`

- To configure the environment run:
  ```bash
  > cd $GIT_ROOT/sorrentum_sandbox/devops
  > source $GIT_ROOT/sorrentum_sandbox/devops/setenv.sh
  ```

- There are several scripts that allow to use the Airflow container:
  - `docker_ls.sh`: show the Sorrentum containers
  - `docker_prune.sh`: remove the Sorrentum app image
  - `docker_prune_all.sh`: remove all the images needed for the Sorrentum node
    (e.g., mongo, postgres, ...)
  - `docker_pull.sh`: pull image of the Sorrentum app container
  - `docker_bash.sh`: start a Sorrentum app container
  - `docker_exec.sh`: start another shell in an already running Sorrentum app
    container
  - `docker_cmd.sh`: execute one command inside the Sorrentum app container
  - `docker_build.sh`: build the image of the Sorrentum app container
  - `docker_push.sh`: push to DockerHub the image of the Sorrentum app container

- E.g., you can check the Airflow version with:
  ```
  > docker_bash.sh
  docker> airflow version
  2.8.2
  ```

## Sorrentum app container

### Clean up

- You might have some images from previous runs, e.g.,
  ```bash
  > docker images | grep sorrentum
  sorrentum/sorrentum             latest            d298b34ae6db   43 minutes ago   4.03GB
  sorrentum/jupyter               latest            360b1d8a2f7e   3 days ago       1.38GB
  sorrentum/cmamp                 dev               e61ee1b8c77a   2 weeks ago      3.31GB
  sorrentum/sorrentum             Spring2023        0a0b62706879   10 months ago    2.42GB

  > docker images | grep mongo
  mongo                           latest            35e5c11c442d   8 days ago       721MB

  > docker images | grep postgres
  postgres                        14.0              01b2dbb34042   2 years ago      354MB

  > docker images | grep airflow
  ```

- There is also a convenient script
  ```bash
  > docker_ls.sh
  # Sorrentum
  sorrentum/sorrentum           latest            d298b34ae6db   12 hours ago    4.03GB
  sorrentum/jupyter             latest            360b1d8a2f7e   3 days ago      1.38GB
  sorrentum/cmamp               dev               e61ee1b8c77a   2 weeks ago     3.31GB
  sorrentum/sorrentum           Spring2023        0a0b62706879   10 months ago   2.42GB
  # Mongo
  mongo                         latest            35e5c11c442d   9 days ago      721MB
  # Postgres
  postgres                      14.0              01b2dbb34042   2 years ago     354MB
  # Airflow
  ```

- You can clean up your Docker system from the images
  ```
  > docker_prune_all.sh
  ```

### Pull image from Dockerhub

- The Sorrentum app container image is already pre-built and can be downloaded
  from DockerHub `sorrentum/sorrentum`
  ```
  > docker_pull.sh
  ```

- You can see the public available images on https://hub.docker.com/u/sorrentum
  or from command line
  ```
  > curl -s "https://hub.docker.com/v2/repositories/sorrentum/?page_size=100" | jq '.results|.[]|.name'
  ```

### Build image locally
 
- You can also build manually the Sorrentum container using Docker
  ```
  > cd $GIT_ROOT/sorrentum_sandbox/devops
  > docker_build.sh
  ```
- Building the container takes a few minutes
- There can be warnings but it's fine as long as the build process terminates
  with something along the lines of:
  ```
  Successfully built a7ca17283abb
  Successfully tagged sorrentum/sorrentum:latest

  + exit 0
  + docker image ls sorrentum/sorrentum
  REPOSITORY            TAG       IMAGE ID       CREATED         SIZE
  sorrentum/sorrentum   latest    a7ca17283abb   2 seconds ago   2.2GB
  ```

- Note that `docker-compose` automates the building phases of the entire system,
  by building, pulling images according to the compose file
  ```
  > cd $GIT_ROOT/sorrentum_sandbox/devops
  > docker-compose build
  ```
  
## Bring up Sorrentum data node

- The best approach is to see the Airflow logs in one window at the same time as
  running other commands in a different windows:
  - Run Airflow server in one terminal window using `docker_bash.sh`
  - Run other tools in other windows using `docker_exec.sh`
- You can use `tmux` to allow multiple windows in the same shell

- After the containers are ready, you can bring up the service with:
  ```
  > cd $GIT_ROOT/sorrentum_sandbox/devops
  > docker-compose up
  ```

- Starting the system can take some time

- Note that there can be some errors / warnings, but things are good as long 
  as you see Airflow starting like below:
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

- The Airflow service provided in the container uses `LocalExecutor` which is 
  suitable for test / dev environments
  - For more robust deployments it is possible to add more components to the
    docker-compose (e.g., `Celery` and `Redis`)

## Check the Airflow status

- Check that the Airflow service is up by going with your browser to
  `localhost:8091`
  - You should see the Airflow login
  - You can't log in since you don't have username / password yet

- You can see the services running as Docker containers:
  ```
  > docker images | perl -n -e 'print if /sorrentum|postgres|mongo/;'
  sorrentum/sorrentum:latest    latest    223859f6c70e   5 hours ago     2.19GB
  mongo                         latest    0850fead9327   4 weeks ago     700MB
  postgres                      14.0      317a302c7480   14 months ago   374MB

  > docker container ls
  CONTAINER ID   IMAGE                      COMMAND                  CREATED   STATUS                  PORTS                     NAMES
  454e70e42b9f   sorrentum/sorrentum:latest "/usr/bin/dumb-init …"   12 seconds ago   Up Less than a second   8080/tcp                  airflow_scheduler_cont
  5ac4c7f6d883   sorrentum/sorrentum:latest "/usr/bin/dumb-init …"   12 seconds ago   Up 10 seconds           0.0.0.0:8091->8080/tcp    airflow_cont
  37e86b58c247   postgres:14.0              "docker-entrypoint.s…"   13 seconds ago   Up 11 seconds           0.0.0.0:5532->5432/tcp    postgres_cont
  c33f70195dd3   mongo                      "docker-entrypoint.s…"   13 seconds ago   Up 12 seconds           0.0.0.0:27017->27017/tcp  mongo_cont

  > docker volume ls
  DRIVER    VOLUME NAME
  local     5bd623d00c7c07d2364d876883669f3032182da89ad858aac57a0219528f4272
  local     sorrentum_data_node_airflow-database-data
  local     sorrentum_data_node_airflow-log-volume
  ```

- When starting the Airflow container for the first time you need to initialize
  Airflow
- Take a look at the script that configures Airflow
  ```
  > cd $GIT_ROOT/sorrentum_sandbox/devops
  > vi ./init_airflow_setup.sh
  ```
- In a different terminal window outside the Docker container, run:
  ```
  > cd $GIT_ROOT/sorrentum_sandbox/devops
  > ./init_airflow_setup.sh
  ...
  [2023-01-22 01:07:31,578] {manager.py:214} INFO - Added user airflow
  User "airflow" created with role "Admin"
  ```

- Now if you go to the browser to `localhost:8091` on your local machine you can
  log in with the default login credentials `airflow`:`airflow`
- Upon successful login you should see the Airflow UI
  ![image](https://user-images.githubusercontent.com/49269742/215845132-6ca56974-495d-4ca2-9656-32000033f341.png)
- To enable a DAG and start executing it based on provided interval, flip the
  switch next to the DAG name
  - E.g., you can enable the DAG `download_periodic_1min_postgres_ohlcv_binance` which
    downloads OHLCV data from Binance and saves it into Postgres

## Pausing Airflow service

- You can bring down the Sorrentum service (persisting the state) with:
  ```
  > docker compose down
  Container mongo_cont                 Removed
  Container airflow_cont               Removed
  Container airflow_scheduler_cont     Removed
  Container postgres_cont              Removed
  Network sorrentum_data_node_default  Removed
  ```
- You can see in the Airflow window that the service has stopped

- You can verify the state of Docker containers directly with:
  ```
  > docker container ls
  > docker volume ls
  ```
- Note that the containers are only paused

- Restarting the service keeps the volume which contains the state of Postgres:
  ```
  docker compose restart

  > docker container ls
  > docker volume ls
  ```
- The containers are restarted

## Restarting the services

- To remove all the containers and volumes, which corresponds to resetting 
  completely the system
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

- To rebuild after trying out some changes in dockerfile/compose file
  ```
  > docker-compose up --build --force-recreate
  ```

