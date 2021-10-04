import os
from typing import Any

from invoke import task

import helpers.dbg as hdbg
import helpers.lib_tasks as hlibtask


def _get_im_docker_compose_path() -> str:
    docker_compose_dir = "im/devops/compose"
    compose_file_name = "docker-compose.yml"
    docker_compose_path = os.path.join(docker_compose_dir, compose_file_name)
    docker_compose_abs_path = os.path.abspath(docker_compose_path)
    hdbg.dassert_exists(docker_compose_abs_path)
    return docker_compose_abs_path


@task
def im_docker_cmd(ctx: Any, cmd: str) -> None:
    docker_cmd = ["docker_compose"]
    docker_compose_file_path = _get_im_docker_compose_path()
    docker_cmd.append(f"--file {docker_compose_file_path}")
    service_name = "app"
    docker_cmd.append(f"run --rm {service_name}")
    docker_cmd.append(cmd)
    multiline_docker_cmd = hlibtask._to_multi_line_cmd(docker_cmd)
    hlibtask._run(ctx, multiline_docker_cmd)
