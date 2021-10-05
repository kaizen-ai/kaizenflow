"""
Tasks related to `im` project.

Import as:

import im.im_lib_tasks as imimlitas
"""

import os

from invoke import task

import helpers.dbg as hdbg
import helpers.lib_tasks as hlitas


def _get_im_docker_compose_path() -> str:
    """
    Return path to the docker-compose file `im/devops/compose/docker-
    compose.yml`.
    """
    # Get `docker-compose` file path.
    docker_compose_dir = "im/devops/compose"
    compose_file_name = "docker-compose.yml"
    docker_compose_path = os.path.join(docker_compose_dir, compose_file_name)
    # Get absolute version of a file path.
    docker_compose_abs_path = os.path.abspath(docker_compose_path)
    # Verify that the file exists.
    hdbg.dassert_file_exists(docker_compose_abs_path)
    return docker_compose_abs_path


def _get_im_docker_cmd(cmd: str) -> str:
    """
    Get `im docker-compose' command.

    E.g, to run the `im/devops/set_schema_im_db.py`:
    ```
    docker-compose \
        --file /app/im/devops/compose/docker-compose.yml \
        run --rm app \
        im/devops/set_schema_im_db.py
    ```

    :param cmd: command to execute
    :return: `im docker-compose' command
    """
    docker_cmd = ["docker-compose"]
    # Add `docker-compose` file path.
    docker_compose_file_path = _get_im_docker_compose_path()
    docker_cmd.append(f"--file {docker_compose_file_path}")
    # Add `run`.
    service_name = "app"
    docker_cmd.append(f"run --rm {service_name}")
    docker_cmd.append(cmd)
    # Convert the list to a multiline command.
    multiline_docker_cmd = hlitas._to_multi_line_cmd(docker_cmd)
    return multiline_docker_cmd


def _get_im_docker_cleanup() -> str:
    """
    Get `im docker-compose' clean up command.

    E.g.,
    ```
    docker-compose \
        --file /app/im/devops/compose/docker-compose.yml \
        down -v
    ```

    :return: `im docker-compose' clean up command
    """
    docker_compose_down = ["docker-compose"]
    # Add `docker-compose` file path.
    docker_compose_file_path = _get_im_docker_compose_path()
    docker_compose_down.append(f"--file {docker_compose_file_path}")
    # Add `down` command with the '-v' option to remove attached volumes.
    docker_compose_down.append("down -v")
    multiline_docker_compose_down = hlitas._to_multi_line_cmd(docker_compose_down)
    return multiline_docker_compose_down


@task
def im_docker_cmd(ctx, cmd):  # type: ignore
    """
    Execute the command `cmd` inside a container attached to the `im app`.

    :param ctx: `context` object
    :param cmd: command to execute
    """
    hdbg.dassert_ne(cmd, "")
    # Get docker cmd.
    docker_cmd = _get_im_docker_cmd(cmd)
    # Execute the command.
    hlitas._run(ctx, docker_cmd, pty=True)


@task
def im_docker_cleanup(ctx):  # type: ignore
    """
    Remove containers and volumes attached to the `im app`.

    :param ctx: `context` object
    """
    # Get docker clean up cmd.
    docker_clean_up_cmd = _get_im_docker_cleanup()
    # Execute the command.
    hlitas._run(ctx, docker_clean_up_cmd, pty=True)
