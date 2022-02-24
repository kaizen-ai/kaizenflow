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
    base_image = "665840871993.dkr.ecr.us-east-1.amazonaws.com/opt"
    user = hsystem.get_user_name()
    image_local = f"{base_image}:local-{user}-{version}"
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
