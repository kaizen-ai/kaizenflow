"""
Tasks related to `im` project.

Import as:

import im_v2.im_lib_tasks as imimlitas
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
    docker_compose_file_path = hlibtask._get_base_docker_compose_path()
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
    docker_compose_file_path = hlibtask._get_base_docker_compose_path()
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
    docker_compose_file_path = hlibtask._get_base_docker_compose_path()
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
