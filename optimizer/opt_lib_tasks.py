"""
Import as:

import optimizer.opt_lib_tasks as ooplitas
"""

import logging
import os

from invoke import task

import helpers.hgit as hgit
import helpers.lib_tasks as hlibtask

_LOG = logging.getLogger(__name__)


_OPTIMIZER_DIR = os.path.join(hgit.get_amp_abs_path(), "optimizer")


# #############################################################################
# Docker image release.
# #############################################################################


@task
def opt_docker_build_local_image(  # type: ignore
    ctx,
    version,
    cache=True,
    base_image="",
    update_poetry=False,
):
    """
    Build a local `opt` image (i.e., a release candidate "dev" image).

    See corresponding invoke target for the main container.
    """
    hlibtask.docker_build_local_image(
        ctx,
        version,
        cache=cache,
        base_image=base_image,
        update_poetry=update_poetry,
        container_dir_name=_OPTIMIZER_DIR,
    )


@task
def opt_docker_tag_local_image_as_dev(  # type: ignore
    ctx,
    version,
    base_image="",
):
    """
    (ONLY CI/CD) Mark the `opt:local` image as `dev`.

    See corresponding invoke target for the main container.
    """
    hlibtask.docker_tag_local_image_as_dev(
        ctx,
        version,
        base_image=base_image,
        container_dir_name=_OPTIMIZER_DIR,
    )


@task
def opt_docker_push_dev_image(  # type: ignore
    ctx,
    version,
    base_image="",
):
    """
    (ONLY CI/CD) Push the `opt:dev` image to ECR.

    See corresponding invoke target for the main container.
    """
    hlibtask.docker_push_dev_image(
        ctx, version, base_image=base_image, container_dir_name=_OPTIMIZER_DIR
    )


@task
def opt_docker_release_dev_image(  # type: ignore
    ctx,
    version,
    cache=True,
    push_to_repo=True,
    update_poetry=False,
):
    """
    (ONLY CI/CD) Build, test, and release to ECR the latest `opt:dev` image.

    See corresponding invoke target for the main container.

    Phases:
    1) Build local image
    2) Mark local as dev image
    3) Push dev image to the repo
    """
    hlibtask.docker_release_dev_image(
        ctx,
        version,
        cache=cache,
        # TODO(Grisha): replace with `opt` tests.
        skip_tests=True,
        fast_tests=False,
        slow_tests=False,
        superslow_tests=False,
        # TODO(Grisha): enable `qa` tests.
        qa_tests=False,
        push_to_repo=push_to_repo,
        update_poetry=update_poetry,
        container_dir_name=_OPTIMIZER_DIR,
    )


# #############################################################################
# Docker development.
# #############################################################################


@task
def opt_docker_bash(  # type: ignore
    ctx,
    base_image="",
    stage="dev",
    version="",
    entrypoint=True,
    as_user=True,
):
    """
    Start a bash shell inside the `opt` container corresponding to a stage.

    See corresponding invoke target for the main container.
    """
    hlibtask.docker_bash(
        ctx,
        base_image=base_image,
        stage=stage,
        version=version,
        entrypoint=entrypoint,
        as_user=as_user,
        container_dir_name=_OPTIMIZER_DIR,
    )


@task
def opt_docker_jupyter(  # type: ignore
    ctx,
    stage="dev",
    version="",
    base_image="",
    auto_assign_port=True,
    port=9999,
    self_test=False,
):
    """
    Run jupyter notebook server in the `opt` container.

    See corresponding invoke target for the main container.
    """
    hlibtask.docker_jupyter(
        ctx,
        stage=stage,
        version=version,
        base_image=base_image,
        auto_assign_port=auto_assign_port,
        port=port,
        self_test=self_test,
        container_dir_name=_OPTIMIZER_DIR,
    )


@task
def opt_docker_cmd(  # type: ignore
    ctx,
    base_image="",
    stage="dev",
    version="",
    cmd="",
    as_user=True,
    use_bash=False,
):
    """
    Run a command inside the `opt` container corresponding to a stage.

    See corresponding invoke target for the main container.
    """
    hlibtask.docker_cmd(
        ctx,
        base_image=base_image,
        stage=stage,
        version=version,
        cmd=cmd,
        as_user=as_user,
        use_bash=use_bash,
        container_dir_name=_OPTIMIZER_DIR,
    )


# #############################################################################
# Run tests.
# #############################################################################

@task
def opt_run_fast_tests(
    ctx,
    stage="dev",
    version="",
    pytest_opts="",
    coverage=False,
    collect_only=False,
    tee_to_file=False,
    **kwargs,
):
    test_list_name = "fast_tests"
    custom_marker = "optimizer"
    pytest_opts = "optimizer"
    # This parameter is not needed as `optimizer` never has a submodule.
    skip_submodules = False
    # This is always False because we cannot call `git_clean` invoke
    # from the `optimizer` dir.
    git_clean = False
    rc = hlibtask._run_tests(
        ctx,
        stage,
        test_list_name,
        custom_marker,
        version,
        pytest_opts,
        skip_submodules,
        coverage,
        collect_only,
        tee_to_file,
        git_clean,
        **kwargs,
    )
    return rc



def test():
    test_list_name = "fast_tests"
    custom_marker = "optimizer"
    pytest_opts = "optimizer"
    skip_submodules = False
    coverage = False
    collect_only = False
    tee_to_file = False
    cmd = hlibtask._build_run_command_line(test_list_name, custom_marker, pytest_opts, skip_submodules, coverage, collect_only, tee_to_file)
    return cmd