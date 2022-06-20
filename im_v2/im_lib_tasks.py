"""
Tasks related to `im` project.

Import as:

import im_v2.im_lib_tasks as imvimlita
"""

import logging
import os
from typing import Optional
import re

from invoke import task

import helpers.hdbg as hdbg
import helpers.hgit as hgit
import helpers.hio as hio
import helpers.lib_tasks as hlibtask
import helpers.hsecrets as hsecret
import helpers.hversion as hversio
import helpers.hsystem as hsystem


_LOG = logging.getLogger(__name__)


def get_db_env_path(stage: str, *, idx: Optional[int] = None) -> str:
    """
    Get path to db env file that contains db connection parameters.

    :param stage: development stage, i.e. `local`, `dev` and `prod`
    :param idx: index used to make the generated file unique
    :return: path to db env file
    """
    hdbg.dassert_in(stage, "local dev prod".split())
    # Get `env` files dir.
    env_dir = "im_v2/devops/env"
    # Get the file name depending on the stage.
    env_file_name = f"{stage}.im_db_config.env"
    if idx is not None:
        env_file_name = hio.add_idx_to_filename(env_file_name, idx)
    # Get file path.
    amp_path = hgit.get_amp_abs_path()
    env_file_path = os.path.join(amp_path, env_dir, env_file_name)
    # We use idx when we want to generate a Docker env file on the fly. So we
    # can't enforce that the file already exists.
    if idx is None:
        hdbg.dassert_file_exists(env_file_path)
    return env_file_path


# #############################################################################


def _get_docker_cmd(stage: str, docker_cmd: str) -> str:
    """
    Construct the `docker-compose' command to run a script inside this
    container Docker component.

    E.g, to run the `.../devops/set_schema_im_db.py`:
    ```
    docker-compose \
        --file devops/compose/docker-compose.yml \
        --env-file devops/env/local.im_db_config.env \
        run --rm im_postgres \
        .../devops/set_schema_im_db.py
    ```

    :param stage: development stage, i.e. `local`, `dev` and `prod`
    :param docker_cmd: command to execute inside docker
    """
    cmd = ["docker-compose"]
    # Add `docker-compose` file path.
    docker_compose_file_path = hlibtask.get_base_docker_compose_path()
    cmd.append(f"--file {docker_compose_file_path}")
    # Add `env file` path.
    env_file = get_db_env_path(stage)
    cmd.append(f"--env-file {env_file}")
    # Add `run`.
    service_name = "im_postgres"
    cmd.append(f"run --rm {service_name}")
    cmd.append(docker_cmd)
    # Convert the list to a multiline command.
    multiline_docker_cmd = hlibtask._to_multi_line_cmd(cmd)
    return multiline_docker_cmd  # type: ignore[no-any-return]


@task
def im_docker_cmd(ctx, stage, cmd):  # type: ignore
    """
    Execute the command `cmd` inside a container attached to the `im app`.

    :param stage: development stage, i.e. `local`, `dev` and `prod`
    :param cmd: command to execute
    """
    hdbg.dassert_ne(cmd, "")
    # Get docker cmd.
    docker_cmd = _get_docker_cmd(stage, cmd)
    # Execute the command.
    hlibtask._run(ctx, docker_cmd, pty=True)


# #############################################################################


def _get_docker_up_cmd(stage: str, detach: bool) -> str:
    """
    Construct the command to bring up the `im` service.

    E.g.,
    ```
    docker-compose \
        --file devops/compose/docker-compose.yml \
        --env-file devops/env/local.im_db_config.env \
        up \
        im_postgres
    ```

    :param stage: development stage, i.e. `local`, `dev` and `prod`
    :param detach: run containers in the background
    """
    cmd = ["docker-compose"]
    # Add `docker-compose` file path.
    docker_compose_file_path = hlibtask.get_base_docker_compose_path()
    cmd.append(f"--file {docker_compose_file_path}")
    # Add `env file` path.
    env_file = get_db_env_path(stage)
    cmd.append(f"--env-file {env_file}")
    # Add `down` command.
    cmd.append("up")
    if detach:
        # Enable detached mode.
        cmd.append("-d")
    service = "im_postgres"
    cmd.append(service)
    cmd = hlibtask._to_multi_line_cmd(cmd)
    return cmd  # type: ignore[no-any-return]


@task
def im_docker_up(ctx, stage, detach=False):  # type: ignore
    """
    Start im container with Postgres inside.

    :param ctx: `context` object
    :param stage: development stage, i.e. `local`, `dev` and `prod`
    :param detach: run containers in the background
    """
    # Get docker down command.
    docker_clean_up_cmd = _get_docker_up_cmd(stage, detach)
    # Execute the command.
    hlibtask._run(ctx, docker_clean_up_cmd, pty=True)


# #############################################################################


def _get_docker_down_cmd(stage: str, volumes_remove: bool) -> str:
    """
    Construct the command to shut down the `im` service.

    E.g.,
    ```
    docker-compose \
        --file devops/compose/docker-compose.yml \
        --env-file devops/env/local.im_db_config.env \
        down \
        -v
    ```

    :param stage: development stage, i.e. `local`, `dev` and `prod`
    :param volumes_remove: whether to remove attached volumes or not
    """
    cmd = ["docker-compose"]
    # Add `docker-compose` file path.
    docker_compose_file_path = hlibtask.get_base_docker_compose_path()
    cmd.append(f"--file {docker_compose_file_path}")
    # Add `env file` path.
    env_file = get_db_env_path(stage)
    cmd.append(f"--env-file {env_file}")
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
def im_docker_down(ctx, stage, volumes_remove=False):  # type: ignore
    """
    Bring down the `im` service.

    By default volumes are not removed, to also remove volumes do
    `invoke im_docker_down -v`.

    :param stage: development stage, i.e. `local`, `dev` and `prod`
    :param volumes_remove: whether to remove attached volumes or not
    :param ctx: `context` object
    """
    # Get docker down command.
    cmd = _get_docker_down_cmd(stage, volumes_remove)
    # Execute the command.
    hlibtask._run(ctx, cmd, pty=True)


@task
def docker_create_candidate_image(ctx, task_definition: str):  # type: ignore
    """
    Create new prod candidate image and update the specified ECS task definition such that
    the Image URL specified in container definition points to the new candidate image.

    :param task_definition: the name of the ECS task definition for which an update 
    to container image URL is made, e.g. `cmamp-test`
    """
    # Get latest version.
    last_version = hversio.get_changelog_version(".")
    # Create new prod image.
    #cmd = f"invoke docker_build_prod_image -v {last_version} --candidate"
    hlibtask.docker_build_prod_image(
        ctx,
        last_version,
        candidate=True
    )
    #rc = hsystem.system_to_string(cmd, abort_on_error=False)
    #hdbg.dassert_eq(rc[0], 0)
    # Extract the image tag from the output log.
    tag = _extract_image_hash(rc[1])
    # Push candidate image.
    cmd = f"invoke docker_push_prod_candidate_image --candidate {tag}"
    rc = hsystem.system(cmd, abort_on_error=False)
    hdbg.dassert_eq(rc, 0)
    # Register new task definition revision with updated image URL. 
    update_task_definition(task_definition, tag)
    return


def _extract_image_hash(output: str) -> str:
    """
    Extract prod image hash from the output log.

    :param output: output of `i docker_build_prod_image`
    :return: the hash of the new candidate image, e.g. `13538588e`
    """
    # Extract image hash, it is stored in the last line of the output.
    lines = output[1].split("\n")
    # Split the last output line into columns, get `TAG` column.
    tag = lines[-1].split("   ")[1]
    # Extract hash, e.g. 'prod-13538588e' -> `13538588e`.
    tag = tag.split("-")[1]
    return tag


def update_task_definition(task_definition: str, image_tag: str) -> None:
    """
    Create the new revision of specified ECS task definition and point 
    Image URL specified to the new candidate image.

    :param task_definition: the name of the ECS task definition for which an update 
    to container image URL is made, e.g. `cmamp-test`
    :param image_tag: the hash of the new candidate image, e.g. `13538588e`
    """
    client = hsecret.get_ecs_client("ck")
    # Get the last revison of the task definition.
    task_description = client.describe_task_definition(
    taskDefinition=task_definition
    )
    task_def = task_description["taskDefinition"]
    old_image = task_def["containerDefinitions"][0]["image"]
    # Edit container version, e.g. `cmamp:prod-12a45` - > cmamp:prod-12b46`
    new_image = re.sub("prod-(.+)$", f"prod-{image_tag}", old_image)
    task_def["containerDefinitions"][0]["image"] = new_image
    # Register the new revision with the new image.
    client.register_task_definition(
        family=task_definition,
        taskRoleArn=task_def["taskRoleArn"],
        executionRoleArn=task_def["taskRoleArn"],
        networkMode=task_def["networkMode"],
        containerDefinitions=task_def["containerDefinitions"],
        volumes=task_def["volumes"],
        placementConstraints=task_def["placementConstraints"],
        requiresCompatibilities=task_def["requiresCompatibilities"],
        cpu=task_def["cpu"],
        memory=task_def["memory"]
    )
    return


# #############################################################################


# TODO(Grisha): use `cmamp` for invokes and move out from `im` CMTask #789.

# def _get_create_db_cmd(
#     dbname: str,
#     overwrite: bool,
#     credentials: str,
# ) -> str:
#     """
#     Construct the `docker-compose` command to run a `create_db` script inside
#     this container Docker component.
#
#     ```
#     docker-compose \
#         --file devops/compose/docker-compose.yml \
#         run --rm im_postgres \
#         .../db/create_db.py
#     ```
#
#     :param dbname: db to create inside docker
#     :param overwrite: to overwrite existing db
#     :param credentials: credentials to connect a db, there are 3 options:
#         - credentials are inferred from environment variables, pass 'from_env'
#         - as string `dbname =... host = ... port =... user =... password = ...`
#         - from a `JSON` file, pass a path to a `JSON` file
#     """
#     cmd = ["docker-compose"]
#     docker_compose_file_path = hlibtask.get_base_docker_compose_path()
#     cmd.append(f"--file {docker_compose_file_path}")
#     cmd.append("run --rm im_postgres")
#     cmd.append("im_v2/common/db/create_db.py")
#     cmd.append(f"--db-name '{dbname}'")
#     if overwrite:
#         cmd.append("--overwrite")
#     # Add quotes so that credentials as string are handled properly by invoke.
#     cmd.append(f"--credentials '\"{credentials}\"'")
#     multiline_docker_cmd = hlibtask._to_multi_line_cmd(cmd)
#     return multiline_docker_cmd  # type: ignore[no-any-return]
#
#
# # TODO(Dan3): add unit tests for `im_create_db` #547.
# @task
# def im_create_db(  # type: ignore
#     ctx,
#     dbname,
#     overwrite=False,
#     credentials="from_env",
# ):
#     """
#     Create database inside a container attached to the `im app`.
#
#     Will overwrite test_db database with credentials from json file:
#     ```
#     > i im_create_db test_db --overwrite --credentials file.json
#     ```
#
#     :param dbname: db to create inside docker
#     :param overwrite: to overwrite existing db
#     :param credentials: credentials to connect a db, there are 3 options:
#         - credentials are inferred from environment variables, pass 'from_env'
#         - as string `dbname =... host = ... port =... user =... password = ...`
#         - from a `JSON` file, pass a path to a `JSON` file
#     """
#     # Get docker cmd.
#     docker_cmd = _get_create_db_cmd(dbname, overwrite, credentials)
#     # Execute the command.
#     hlibtask._run(ctx, docker_cmd, pty=True)
#
#
# # #############################################################################
#
#
# def _get_remove_db_cmd(
#     dbname: str,
#     credentials: str,
# ) -> str:
#     """
#     Construct the `docker-compose' command to run a `remove_db` script inside
#     this container Docker component.
#
#     ```
#     docker-compose \
#         --file devops/compose/docker-compose.yml \
#         run --rm im_postgres \
#         .../db/remove_db.py
#     ```
#
#     :param dbname: db to remove inside docker
#     :param credentials: credentials to connect a db, there are 3 options:
#         - credentials are inferred from environment variables, pass 'from_env'
#         - as string `dbname =... host = ... port =... user =... password = ...`
#         - from a `JSON` file, pass a path to a `JSON` file
#     """
#     cmd = ["docker-compose"]
#     docker_compose_file_path = hlibtask.get_base_docker_compose_path()
#     cmd.append(f"--file {docker_compose_file_path}")
#     cmd.append("run --rm im_postgres")
#     cmd.append("im_v2/common/db/remove_db.py")
#     cmd.append(f"--db-name '{dbname}'")
#     # Add quotes so that credentials as string are handled properly by invoke.
#     cmd.append(f"--credentials '\"{credentials}\"'")
#     multiline_docker_cmd = hlibtask._to_multi_line_cmd(cmd)
#     return multiline_docker_cmd  # type: ignore[no-any-return]
#
#
# # TODO(Dan3): add unit tests for `im_remove_db` #547.
# @task
# def im_remove_db(  # type: ignore
#     ctx,
#     dbname,
#     credentials="from_env",
# ):
#     """
#     Remove database inside a container attached to the `im app`.
#
#     Will remove `test_db` database with credentials from json file:
#     ```
#     > i im_remove_db test_db --credentials a.json
#     ```
#
#     :param dbname: db to remove inside docker
#     :param credentials: credentials to connect a db, there are 3 options:
#         - credentials are inferred from environment variables, pass 'from_env'
#         - as string `dbname =... host = ... port =... user =... password = ...`
#         - from a `JSON` file, pass a path to a `JSON` file
#     """
#     # Get docker cmd.
#     docker_cmd = _get_remove_db_cmd(dbname, credentials)
#     # Execute the command.
#     hlibtask._run(ctx, docker_cmd, pty=True)
