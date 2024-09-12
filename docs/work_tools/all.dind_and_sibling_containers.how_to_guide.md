

<!-- toc -->

- [Docker-in-docker (dind)](#docker-in-docker-dind)
  * [Sibling container approach](#sibling-container-approach)
    + [Connecting to Postgres instance using sibling containers](#connecting-to-postgres-instance-using-sibling-containers)

<!-- tocstop -->

# Docker-in-docker (dind)

- It is possible to install a Docker engine inside a Docker container so that
  one can run Docker container (e.g., OMS or IM) inside an isolated `amp`
  container.
- The problems with this approach are:
  - Dind requires to run the external container in privileged mode, which might
    not be possible due to security concerns
  - The Docker / build cache is not shared across parent and children
    containers, so one needs to pull / build an image every time the outermost
    container is restarted
- An alternative approach is the "sibling container" approach

## Sibling container approach

- Refs:
  - [Can I run Docker-in-Docker without using the --privileged flag - Stack Overflow](https://stackoverflow.com/questions/29612463/can-i-run-docker-in-docker-without-using-the-privileged-flag)
  - [https://jpetazzo.github.io/2015/09/03/do-not-use-docker-in-docker-for-ci/](https://jpetazzo.github.io/2015/09/03/do-not-use-docker-in-docker-for-ci/)
- Often what's really needed is the ability to build / run a container from
  another container (e.g., CI or unit test). This can be achieved by mounting
  the Docker socket `/var/run/docker.sock` to the container, so that a container
  can talk to Docker Engine.
- This approach allows reuse of the build cache across the sibling containers.
- The downside is less isolation from the external container, e.g., spawned
  containers can be left hanging or can collide.
- E.g.,
  ```
  # Run `docker ps` in a container, showing the containers running in the main
  container
  > docker run -ti --rm \
        -v /var/run/docker.sock:/var/run/docker.sock \
        dindtest \
        docker ps

  # Start a sibling hello world container:
  > docker run -it --rm \
        -v /var/run/docker.sock:/var/run/docker.sock \
        dindtest \
        docker run -ti --rm hello-world
  ```

### Connecting to Postgres instance using sibling containers

- We can start the Docker container with Postgres as a service from outside the
  container.
  ```
  > (cd oms;  i oms_docker_up -s local)
  INFO: > cmd='/local/home/gsaggese/src/venv/amp.client_venv/bin/invoke oms_docker_up -s local'
  report_memory_usage=False report_cpu_usage=False
  docker-compose \
  --file /local/home/gsaggese/src/sasm-lime4/amp/oms/devops/compose/docker-compose.yml \
  --env-file /local/home/gsaggese/src/sasm-lime4/amp/oms/devops/env/local.oms_db_config.env \
  up \
  oms_postgres
  Creating compose_oms_postgres_1 ... done
  Attaching to compose_oms_postgres_1
  oms_postgres_1  |
  oms_postgres_1  | PostgreSQL Database directory appears to contain a database; Skipping initialization
  oms_postgres_1  |
  oms_postgres_1  | 2022-05-19 22:57:15.659 UTC [1] LOG:  starting PostgreSQL 13.5 (Debian 13.5-1.pgdg110+1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 10.2.1-6) 10.2.1 20210110, 64-bit
  oms_postgres_1  | 2022-05-19 22:57:15.659 UTC [1] LOG:  listening on IPv4 address "0.0.0.0", port 5432
  oms_postgres_1  | 2022-05-19 22:57:15.659 UTC [1] LOG:  listening on IPv6 address "::", port 5432
  oms_postgres_1  | 2022-05-19 22:57:15.663 UTC [1] LOG:  listening on Unix socket "/var/run/postgresql/.s.PGSQL.5432"
  oms_postgres_1  | 2022-05-19 22:57:15.670 UTC [25] LOG:  database system was shut down at 2022-05-19 22:56:50 UTC
  oms_postgres_1  | 2022-05-19 22:57:15.674 UTC [1] LOG:  database system is ready to accept connections
  ```
- Note that Postgres needs to be
- Start a container able to
- From inside a container I launch postgres through the /var/...
  ```
  > docker ps | grep postgres
  CONTAINER ID IMAGE COMMAND CREATED STATUS PORTS NAMES
  83bba0818c74 postgres:13 "docker-entrypoint.s..." 6 minutes ago Up 6 minutes
  0.0.0.0:5432->5432/tcp compose-oms_postgres-1
  ```
- Test connection to the DB from outside the container
  ```
  > psql --host=cf-spm-dev4 --port=5432 --user aljsdalsd -d oms_postgres_db_local
  Password for user aljsdalsd:
  psql (9.5.25, server 13.5 (Debian 13.5-1.pgdg110+1))
  WARNING: psql major version 9.5, server major version 13.
          Some psql features might not work.
  Type "help" for help.
  oms_postgres_db_local=#
  ```
- Test connection to the DB from inside the container
  ```
  > psql --host=cf-spm-dev4 --port=5432 --user aljsdalsd -d oms_postgres_db_local
  ...
  ```
