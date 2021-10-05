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


@task
def im_docker_cmd(ctx, cmd=""):  # type: ignore
    """
    Execute the command `cmd` inside a container attached to the `im app`.

    :param ctx: `context` object
    :param cmd: command to execute
    """
    hdbg.dassert_ne(cmd, "")
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
    # Execute the command.
    hlitas._run(ctx, multiline_docker_cmd, pty=True)
