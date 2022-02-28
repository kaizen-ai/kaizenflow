import os

from invoke import task

import helpers.hsystem as hsystem
import helpers.lib_tasks as hlib


DOCKER_BUILDKIT = 0


def _get_image(version: str) -> str:
    base_image = "665840871993.dkr.ecr.us-east-1.amazonaws.com/opt"
    user = hsystem.get_user_name()
    image_local = f"{base_image}:local-{user}-{version}"
    return image_local


@task
def docker_build_local_opt_image(ctx, version):
    """
    ...

    :param version:
    :return:
    """
    docker_file = "devops/docker_build/dev.Dockerfile"
    abs_docker_file = os.path.abspath(docker_file)
    image_local = _get_image(version)
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


def _get_opt_docker_bash_cmd(version):
    image_local = _get_image(version)
    docker_cmd_: List[str] = []
    docker_cmd_.append(f"IMAGE={image_local}")
    docker_cmd_.append(
        r"""
        docker-compose"""
    )
    compose_file = "devops/compose/docker-compose.yml"
    docker_cmd_.append(f"--file {compose_file}")
    # - Add the `run` command.
    docker_cmd_.append(
        r"""
        run \
        --rm"""
    )
    service_name = "app"
    docker_cmd_.append(
        rf"""
    --entrypoint bash \
    {service_name}"""
    )
    docker_cmd_ = hlib._to_multi_line_cmd(docker_cmd_)
    print(docker_cmd_)
    return docker_cmd_


@task
def opt_docker_bash(ctx, stage, version):
    base_image = "665840871993.dkr.ecr.us-east-1.amazonaws.com/opt"
    cmd = "bash"
    _docker_cmd = hlib._get_docker_cmd(base_image, stage, version, cmd, entrypoint=True, as_user=False)
    hlib._run(ctx, _docker_cmd, pty=True)