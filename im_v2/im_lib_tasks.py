"""
Tasks related to `im` project.

Import as:

import im_v2.im_lib_tasks as imvimlita
"""

import logging

from invoke import task

import helpers.dbg as hdbg
import helpers.lib_tasks as hlibtask

_LOG = logging.getLogger(__name__)

# #############################################################################


def _get_docker_cmd(docker_cmd: str) -> str:
    """
    Construct the `docker-compose' command to run a script inside this
    container Docker component.

    E.g, to run the `.../devops/set_schema_im_db.py`:
    ```
        docker-compose \
        --file devops/compose/docker-compose.yml \
        run --rm im_app \
        .../devops/set_schema_im_db.py
    ```

    :param docker_cmd: command to execute inside docker
    """
    cmd = ["docker-compose"]
    # Add `docker-compose` file path.
    docker_compose_file_path = hlibtask.get_base_docker_compose_path()
    cmd.append(f"--file {docker_compose_file_path}")
    # Add `run`.
    service_name = "im_app"
    cmd.append(f"run --rm {service_name}")
    cmd.append(docker_cmd)
    # Convert the list to a multiline command.
    multiline_docker_cmd = hlibtask._to_multi_line_cmd(cmd)
    return multiline_docker_cmd  # type: ignore[no-any-return]


@task
def im_docker_cmd(ctx, cmd):  # type: ignore
    """
    Execute the command `cmd` inside a container attached to the `im app`.

    :param cmd: command to execute
    """
    hdbg.dassert_ne(cmd, "")
    # Get docker cmd.
    docker_cmd = _get_docker_cmd(cmd)
    # Execute the command.
    hlibtask._run(ctx, docker_cmd, pty=True)


# #############################################################################


def _get_docker_up_cmd(detach: bool) -> str:
    """
    Construct the command to bring up the `im` service.

    E.g.,
    ```
    docker-compose \
        --file devops/compose/docker-compose.yml \
        up \
        im_postgres_local
    ```

    :param detach: run containers in the background
    """
    cmd = ["docker-compose"]
    # Add `docker-compose` file path.
    docker_compose_file_path = hlibtask.get_base_docker_compose_path()
    cmd.append(f"--file {docker_compose_file_path}")
    # Add `down` command.
    cmd.append("up")
    if detach:
        # Enable detached mode.
        cmd.append("-d")
    service = "im_postgres_local"
    cmd.append(service)
    cmd = hlibtask._to_multi_line_cmd(cmd)
    return cmd  # type: ignore[no-any-return]


@task
def im_docker_up(ctx, detach=False):  # type: ignore
    """
    Start im container with Postgres inside.

    :param ctx: `context` object
    :param detach: run containers in the background
    """
    # Get docker down command.
    docker_clean_up_cmd = _get_docker_up_cmd(detach)
    # Execute the command.
    hlibtask._run(ctx, docker_clean_up_cmd, pty=True)


# #############################################################################


def _get_docker_down_cmd(volumes_remove: bool) -> str:
    """
    Construct the command to shut down the `im` service.

    E.g.,
    ```
    docker-compose \
        --file devops/compose/docker-compose.yml \
        down \
        -v
    ```

    :param volumes_remove: whether to remove attached volumes or not
    """
    cmd = ["docker-compose"]
    # Add `docker-compose` file path.
    docker_compose_file_path = hlibtask.get_base_docker_compose_path()
    cmd.append(f"--file {docker_compose_file_path}")
    # Add `down` command.
    cmd.append("down")
    if volumes_remove:
        # Use the '-v' option to remove attached volumes.
        _LOG.warning(
            "Removing the attached volumes resetting the state of the DB"
        )
        cmd.append("-v")
    cmd = hlibtask._to_multi_line_cmd(cmd)
    return cmd  # type: ignore[no-any-return]


@task
def im_docker_down(ctx, volumes_remove=False):  # type: ignore
    """
    Bring down the `im` service.

    By default volumes are not removed, to also remove volumes do
    `invoke im_docker_down -v`.

    :param volumes_remove: whether to remove attached volumes or not
    :param ctx: `context` object
    """
    # Get docker down command.
    cmd = _get_docker_down_cmd(volumes_remove)
    # Execute the command.
    hlibtask._run(ctx, cmd, pty=True)


# #############################################################################


def _get_create_db_cmd(
    dbname: str,
    overwrite: bool,
    credentials: str,
) -> str:
    """
    Construct the `docker-compose` command to run a `create_db` script inside
    this container Docker component.

    ```
    docker-compose \
        --file devops/compose/docker-compose.yml \
        run --rm im_app \
        .../db/create_db.py
    ```

    :param dbname: db to create inside docker
    :param overwrite: to overwrite existing db
    :param credentials: credentials to connect a db, there are 3 options: 
        - credentials are inferred from environment variables, pass 'from_env'
        - as string `dbname =... host = ... port =... user =... password = ...`
        - from a `JSON` file, pass a path to a `JSON` file
    """
    cmd = ["docker-compose"]
    docker_compose_file_path = hlibtask.get_base_docker_compose_path()
    cmd.append(f"--file {docker_compose_file_path}")
    cmd.append(f"run --rm im_app")
    cmd.append("im_v2/common/db/create_db.py")
    cmd.append(f"--db-name '{dbname}'")
    if overwrite:
        cmd.append("--overwrite")
    # Quotes added because they dropped in the shell, cause params conflict
    cmd.append(f"--credentials \'\"{credentials}\"\'")
    multiline_docker_cmd = hlibtask._to_multi_line_cmd(cmd)
    return multiline_docker_cmd  # type: ignore[no-any-return]


# TODO(Dan3): add unit tests for `im_create_db` #547.
@task
def im_create_db(
    ctx,
    dbname,
    overwrite=False,
    credentials="from_env",
):  # type: ignore
    """
    Create database inside a container attached to the `im app`.
    By default it will connect to postgres db through env vars.
    
    Will overwrite test_db database with credentials from json file:
    ```
    > i im_create_db test_db --overwrite --credentials file.json
    ```

    :param dbname: db to create inside docker
    :param overwrite: to overwrite existing db
    :param credentials: credentials to connect a db, there are 3 options: 
        - credentials are inferred from environment variables, pass 'from_env'
        - as string `dbname =... host = ... port =... user =... password = ...`
        - from a `JSON` file, pass a path to a `JSON` file
    """
    # Get docker cmd.
    docker_cmd = _get_create_db_cmd(dbname, overwrite, credentials)
    # Execute the command.
    hlibtask._run(ctx, docker_cmd, pty=True)


# #############################################################################


def _get_remove_db_cmd(
    dbname: str,
    credentials: str,
) -> str:
    """
    Construct the `docker-compose' command to run a `remove_db` script inside
    this container Docker component.

    ```
    docker-compose \
        --file devops/compose/docker-compose.yml \
        run --rm im_app \
        .../db/remove_db.py
    ```

    :param dbname: db to remove inside docker
    :param credentials: credentials to connect a db, there are 3 options: 
        - credentials are inferred from environment variables, pass 'from_env'
        - as string `dbname =... host = ... port =... user =... password = ...`
        - from a `JSON` file, pass a path to a `JSON` file
    """
    cmd = ["docker-compose"]
    docker_compose_file_path = hlibtask.get_base_docker_compose_path()
    cmd.append(f"--file {docker_compose_file_path}")
    cmd.append(f"run --rm im_app")
    cmd.append("im_v2/common/db/remove_db.py")
    cmd.append(f"--db-name '{dbname}'")
    # Quotes added because they dropped in the shell, cause params conflict
    cmd.append(f"--credentials \'\"{credentials}\"\'")
    multiline_docker_cmd = hlibtask._to_multi_line_cmd(cmd)
    return multiline_docker_cmd  # type: ignore[no-any-return]


# TODO(Dan3): add unit tests for `im_remove_db` #547.
@task
def im_remove_db(
    ctx,
    dbname,
    credentials="from_env",
):  # type: ignore
    """
    Remove database inside a container attached to the `im app`.

    Will remove test_db database with credentials from json file:
    ```
    > i im_remove_db test_db --credentials a.json
    ```

    :param dbname: db to remove inside docker
    :param credentials: credentials to connect a db, there are 3 options: 
        - credentials are inferred from environment variables, pass 'from_env'
        - as string `dbname =... host = ... port =... user =... password = ...`
        - from a `JSON` file, pass a path to a `JSON` file
    """
    # Get docker cmd.
    docker_cmd = _get_remove_db_cmd(dbname, credentials)
    # Execute the command.
    hlibtask._run(ctx, docker_cmd, pty=True)
