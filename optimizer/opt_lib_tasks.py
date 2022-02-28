import os

from invoke import task

import helpers.hsystem as hsystem
import helpers.lib_tasks as hlib


DOCKER_BUILDKIT = 0


@task
def docker_build_local_opt_image(ctx, version):
    """
    ...

    :param version:
    :return:
    """
    docker_file = "devops/docker_build/dev.Dockerfile"
    abs_docker_file = os.path.abspath(docker_file)
    base_image = ""
    stage = "local"
    image_local = hlib.get_image(base_image, stage, version)
    cmd = rf"""
    DOCKER_BUILDKIT={DOCKER_BUILDKIT} \
    time \
    docker build \
        --progress=plain \
        --build-arg OPT_CONTAINER_VERSION={version} \
        --tag {image_local} \
        --file {abs_docker_file} \
        .
    """
    hlib._run(ctx, cmd)
    #
    cmd = f"docker image ls {image_local}"
    hlib._run(ctx, cmd)


@task
def opt_docker_bash(ctx, stage, version):
    base_image = ""
    cmd = "bash"
    # TODO(Grisha): turn on `as_user`.
    _docker_cmd = hlib._get_docker_cmd(base_image, stage, version, cmd, entrypoint=True, as_user=False)
    hlib._run(ctx, _docker_cmd, pty=True)
