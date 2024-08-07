"""
Tasks related to `im` project.

Import as:

import helpers.lib_tasks_aws as hlitaaws
"""

import logging
import os

from invoke import task

import helpers.hdbg as hdbg
import helpers.hgit as hgit
import helpers.hio as hio
import helpers.hserver as hserver
import helpers.hsystem as hsystem

_LOG = logging.getLogger(__name__)

# pylint: disable=protected-access


@task
def release_dags_to_airflow(
    ctx,
    files,
    platform,
    dst_airflow_dir=None,
    only_release_from_master=True,
):
    """
    Copy the DAGs to the shared Airflow directory.

    :param files: string of filenames separated by space, e.g., "a.py
        b.py c.py"
    :param platform: string indicating the platform, e.g., "EC2" or "K8"
    :param dst_airflow_dir: destination directory path in Airflow
    :param only_release_from_master: boolean indicating whether to
        release only from master branch
    """
    hdbg.dassert(
        hserver.is_inside_docker(), "This is runnable only inside Docker."
    )
    # Make sure we are working from `master`.
    curr_branch = hgit.get_branch_name()
    if only_release_from_master:
        hdbg.dassert_eq(
            curr_branch, "master", msg="You should release from master branch"
        )
    if dst_airflow_dir is None:
        if platform == "EC2":
            dst_airflow_dir = "/shared_data/airflow_preprod_new/dags"
        elif platform == "K8":
            dst_airflow_dir = "/shared_data/airflow/dags"
        else:
            raise ValueError(f"Unknown platform: {platform}")
    hdbg.dassert_dir_exists(dst_airflow_dir)
    file_paths = files.split()
    # Iterate over each file path in the list.
    for file_path in file_paths:
        # Check the file_path is correct.
        hdbg.dassert_file_exists(file_path)
        dest_file = os.path.join(dst_airflow_dir, os.path.basename(file_path))
        # If same file already exists, then overwrite.
        if os.path.exists(dest_file):
            _LOG.warning(
                "DAG already exists in destination, Overwriting ... %s", dest_file
            )
            # Steps to overwrite:
            # 1. Change user to root.
            # 2. Add permission to write.
            # 3. Copy file.
            # 4. Remove write permission.
            cmds = [
                f"sudo chown root {dest_file}",
                f"sudo chmod a+w {dest_file}",
                f"cp {file_path} {dest_file}",
                f"sudo chmod a-w {dest_file}",
            ]
        else:
            _LOG.info(
                "DAG doesn't exist in destination, Copying ... %s", dest_file
            )
            cmds = [f"cp {file_path} {dest_file}", f"sudo chmod a-w {dest_file}"]
        cmd = "&&".join(cmds)
        # TODO(sonaal): Instead of running scripts, run individual commands.
        # Append script for each file to a temporary file
        temp_script_path = "./tmp.release_dags.sh"
        hio.create_executable_script(temp_script_path, cmd)
        hsystem.system(f"bash -c {temp_script_path}")
