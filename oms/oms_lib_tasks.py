"""
Tasks related to `oms` project.

Import as:

import oms.oms_lib_tasks as oomlitas
"""

import os

from invoke import task

import helpers.dbg as hdbg
import helpers.git as hgit
import helpers.lib_tasks as hlibtask

# TODO(gp): This was branched from im/im_lib_tasks.py. We should factor out the
#  common part.

# TODO(gp): This should be used also from the unit tests?
def _get_docker_compose_path() -> str:
    """
    Return the absolute path to the docker-compose file for this component.

    E.g., `im/devops/compose/docker-compose.yml`.
    """
    # Get `amp` path.
    amp_path = hgit.get_amp_abs_path()
    # Get `docker-compose` file path.
    # TODO(gp): Factor out this dir.
    docker_compose_dir = "oms/devops/compose"
    compose_file_name = "docker-compose.yml"
    docker_compose_path = os.path.join(
        amp_path, docker_compose_dir, compose_file_name
    )
    # Get absolute version of a file path.
    docker_compose_abs_path = os.path.abspath(docker_compose_path)
    # Verify that the file exists.
    hdbg.dassert_file_exists(docker_compose_abs_path)
    return docker_compose_abs_path


# #############################################################################


def _get_docker_cmd(docker_cmd: str) -> str:
    """
    Construct the `docker-compose' command to run a script inside this
    container Docker component.

    E.g, to run the `.../devops/set_schema_im_db.py`:
    ```
    docker-compose \
        --file devops/compose/docker-compose.yml \
        run --rm app \
        .../devops/set_schema_im_db.py
    ```

    :param cmd: command to execute inside docker
    """
    cmd = ["docker-compose"]
    # Add `docker-compose` file path.
    docker_compose_file_path = _get_docker_compose_path()
    cmd.append(f"--file {docker_compose_file_path}")
    # Add `run`.
    service_name = "app"
    cmd.append(f"run --rm {service_name}")
    cmd.append(docker_cmd)
    # Convert the list to a multiline command.
    multiline_docker_cmd = hlibtask._to_multi_line_cmd(cmd)
    return multiline_docker_cmd


@task
def oms_docker_cmd(ctx, cmd):  # type: ignore
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


def _get_docker_up_cmd() -> str:
    """
    Construct the command to bring up the `oms` service.

    E.g.,
    ```
    docker-compose \
        --file devops/compose/docker-compose.yml \
        up \
        oms_postgres_local
    ```
    """
    cmd = ["docker-compose"]
    # Add `docker-compose` file path.
    docker_compose_file_path = _get_docker_compose_path()
    cmd.append(f"--file {docker_compose_file_path}")
    # Add `down` command.
    cmd.append("up")
    service = "oms_postgres_local"
    cmd.append(service)
    cmd = hlibtask._to_multi_line_cmd(cmd)
    return cmd


@task
def oms_docker_up(ctx):  # type: ignore
    """
    Start oms container with Postgres inside.

    :param ctx: `context` object
    """
    # Get docker down command.
    docker_clean_up_cmd = _get_docker_up_cmd()
    # Execute the command.
    hlibtask._run(ctx, docker_clean_up_cmd, pty=True)


# #############################################################################


def _get_docker_down_cmd(volumes_remove: bool) -> str:
    """
    Construct the command to shut down the `oms` service.

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
    docker_compose_file_path = _get_docker_compose_path()
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
    return cmd


@task
def oms_docker_down(ctx, volumes_remove=False):  # type: ignore
    """
    Bring down the `oms` service.

    By default volumes are not removed, to also remove volumes do
    `invoke im_docker_down -v`.

    :param volumes_remove: whether to remove attached volumes or not
    :param ctx: `context` object
    """
    # Get docker down command.
    cmd = _get_docker_down_cmd(volumes_remove)
    # Execute the command.
    hlibtask._run(ctx, cmd, pty=True)
