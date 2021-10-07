"""
Tasks related to `im` project.

Import as:

import im.im_lib_tasks as imimlitas
"""

import os
from typing import Optional

from invoke import task

import helpers.dbg as hdbg
import helpers.lib_tasks as hlitas


def _get_im_docker_compose_path(stage: Optional[str] = None) -> str:
    """
    Return path to the stage-specific docker-compose file.

    E.g., `im/devops/compose/docker-compose.dev.yml`.

    :param stage: development stage, e.g. `local`, `prod`
    """
    # Get `docker-compose` file path.
    docker_compose_dir = "im/devops/compose"
    compose_file_name = "docker-compose.yml"
    if stage is not None:
        hdbg.dassert_in(stage, ["local", "dev", "prod"])
        compose_file_name = f"docker-compose.{stage}.yml"
    docker_compose_path = os.path.join(docker_compose_dir, compose_file_name)
    # Get absolute version of a file path.
    docker_compose_abs_path = os.path.abspath(docker_compose_path)
    # Verify that the file exists.
    hdbg.dassert_file_exists(docker_compose_abs_path)
    return docker_compose_abs_path


def _get_im_docker_cmd(stage: str, cmd: str) -> str:
    """
    Construct stage-specific `im docker-compose' command.

    E.g, to run the `im/devops/set_schema_im_db.py` on the `local` stage:
    ```
    docker-compose \
        --file /app/im/devops/compose/docker-compose.yml \
        --file /app/im/devops/compose/docker-compose.local.yml \
        run --rm app \
        im/devops/set_schema_im_db.py
    ```

    :param stage: development stage, e.g. `local`, `prod`
    :param cmd: command to execute
    :return: `im docker-compose' command
    """
    docker_cmd = ["docker-compose"]
    # Add `docker-compose` file path.
    docker_compose_file_path = _get_im_docker_compose_path()
    docker_cmd.append(f"--file {docker_compose_file_path}")
    # Add stage-specific `docker-compose` override file path.
    docker_compose_stage_file_path = _get_im_docker_compose_path(stage)
    docker_cmd.append(f"--file {docker_compose_stage_file_path}")
    # Add `run`.
    service_name = "app"
    docker_cmd.append(f"run --rm {service_name}")
    docker_cmd.append(cmd)
    # Convert the list to a multiline command.
    multiline_docker_cmd = hlitas._to_multi_line_cmd(docker_cmd)
    return multiline_docker_cmd


def _get_im_docker_down(stage: str, volumes_remove: bool) -> str:
    """
    Construct stage-specific `im docker-compose down' command.

    E.g.,
    ```
    docker-compose \
        --file /app/im/devops/compose/docker-compose.yml \
        --file /app/im/devops/compose/docker-compose.local.yml \
        down -v
    ```

    :param stage: development stage, e.g. `local`, `prod`
    :param volumes_remove: whether to remove attached volumes or not
    :return: `im docker-compose down' command
    """
    docker_compose_down = ["docker-compose"]
    # Add `docker-compose` file path.
    docker_compose_file_path = _get_im_docker_compose_path()
    docker_compose_down.append(f"--file {docker_compose_file_path}")
    # Add stage-specific `docker-compose` override file path.
    docker_compose_stage_file_path = _get_im_docker_compose_path(stage)
    docker_compose_down.append(f"--file {docker_compose_stage_file_path}")
    # Add `down` command.
    docker_compose_down.append("down")
    if volumes_remove:
        # Use the '-v' option to remove attached volumes.
        docker_compose_down.append("-v")
    multiline_docker_compose_down = hlitas._to_multi_line_cmd(docker_compose_down)
    return multiline_docker_compose_down


@task
def im_docker_cmd(ctx, stage, cmd):  # type: ignore
    """
    Execute the command `cmd` inside a container attached to the `im app`.

    :param ctx: `context` object
    :param stage: development stage, e.g. `local`, `prod`
    :param cmd: command to execute
    """
    hdbg.dassert_ne(cmd, "")
    # Get docker cmd.
    docker_cmd = _get_im_docker_cmd(stage, cmd)
    # Execute the command.
    hlitas._run(ctx, docker_cmd, pty=True)


@task
def im_docker_down(ctx, stage, volumes_remove=False):  # type: ignore
    """
    Remove containers and volumes attached to the `im app`.

    By default volumes are not removed, to also remove volumes do
    `invoke im_docker_down -v`.

    :param stage: development stage, e.g. `local`, `prod`
    :param volumes_remove: whether to remove attached volumes or not
    :param ctx: `context` object
    """
    # Get docker down command.
    docker_clean_up_cmd = _get_im_docker_down(stage, volumes_remove)
    # Execute the command.
    hlitas._run(ctx, docker_clean_up_cmd, pty=True)
