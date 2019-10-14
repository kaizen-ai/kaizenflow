#!/usr/bin/env python

"""
# Install the amp default environment:
> create_conda.py --env_name develop --req_file dev_scripts/install/requirements/develop.txt --delete_env_if_exists

# Install the `p1_develop` default environment:
> create_conda.py --env_name p1_develop --req_file amp/dev_scripts/install/requirements/develop.txt --req_file dev_scripts/install/requirements/p1_develop.txt --delete_env_if_exists

# Quick install to test the script:
> create_conda.py --test_install -v DEBUG

# Test the `develop` environment:
> create_conda.py --env_name develop_test --req_file dev_scripts/install/requirements/develop.txt --delete_env_if_exists

# Install pymc3 env:
> create_conda.py --env_name pymc3 --req_file dev_scripts/install/requirements/pymc.txt -v DEBUG
"""

import argparse
import logging
import os
import sys

# ##############################################################################


# Store the values before any modification, by making a copy (out of paranoia).
_PATH = str(os.environ["PATH"]) if "PATH" in os.environ else ""
_PYTHONPATH = str(os.environ["PYTHONPATH"]) if "PYTHONPATH" in os.environ else ""


def _bootstrap(rel_path_to_helpers):
    """
    Tweak PYTHONPATH to pick up amp libraries while we are configuring amp,
    breaking the circular dependency.

    Same code for dev_scripts/_setenv.py and dev_scripts/install/create_conda.py

    # TODO(gp): It is not easy to share it as an import. Maybe we can just read
    # it from a file an eval it.
    """
    exec_name = os.path.abspath(sys.argv[0])
    amp_path = os.path.abspath(
        os.path.join(os.path.dirname(exec_name), rel_path_to_helpers)
    )
    # Check that helpers exists.
    helpers_path = os.path.join(amp_path, "helpers")
    assert os.path.exists(helpers_path), "Can't find '%s'" % helpers_path
    # Update path.
    if False:
        print("PATH=%s" % _PATH)
        print("PYTHONPATH=%s" % _PYTHONPATH)
        print("amp_path=%s" % amp_path)
    # We can't update os.environ since the script is already running.
    sys.path.insert(0, amp_path)
    # Test the imports.
    try:
        pass
    except ImportError as e:
        print("PATH=%s" % _PATH)
        print("PYTHONPATH=%s" % _PYTHONPATH)
        print("amp_path=%s" % amp_path)
        raise e


# This script is dev_scripts/install/create_conda.py, so we need to go up two
# levels to reach "helpers".
_bootstrap("../..")


import helpers.conda as hco  # isort:skip
import helpers.dbg as dbg  # isort:skip
import helpers.env as env  # isort:skip
import helpers.io_ as io_  # isort:skip
import helpers.printing as pri  # isort:skip
import helpers.user_credentials as usc  # isort:skip

# ##############################################################################

_LOG = logging.getLogger(__name__)

# To override python version from the yaml file.
# _PYTHON_VERSION = "2.7"
# _PYTHON_VERSION = "3.7"
_PYTHON_VERSION = None

# Dir of the current create_conda.py.
_CURR_DIR = os.path.dirname(sys.argv[0])

# The following paths are expressed relative to create_conda.py.
# TODO(gp): Allow them to tweak so we can be independent with respect to amp.
# dev_scripts/install/requirements
_REQUIREMENT_DIR = os.path.abspath(os.path.join(_CURR_DIR, "requirements"))

# dev_scripts/install/conda_envs
_CONDA_ENVS_DIR = os.path.abspath(os.path.join(_CURR_DIR, "conda_envs"))


# The script leverages the fact that `conda create` can merge multiple
# requirements files.

def _process_requirements_file(req_file):
    """
    - Read a requirements file `req_file`
    - Skip lines like:
        # docx    # Not on Mac.
      to allow configuration based on target.
    - Merge the result in a tmp file that is created in the same dir as the
      `req_file`
    :return: name of the new file
    """
    txt = []
    # Read file.
    req_file = os.path.abspath(req_file)
    _LOG.debug("req_file=%s", req_file)
    dbg.dassert_exists(req_file)
    txt_tmp = io_.from_file(req_file, split=True)
    # Process.
    for l in txt_tmp:
        # TODO(gp): Can one do conditional builds for different machines?
        #  I don't think so.
        if "# Not on Mac." in l:
            continue
        txt.append(l)
    # Save file.
    txt = "\n".join(txt)
    dst_req_file = os.path.join(
        os.path.dirname(req_file),
        "tmp." + os.path.basename(req_file))
    io_.to_file(dst_req_file, txt)
    return dst_req_file


def _process_requirements_files(req_files):
    dbg.dassert_isinstance(req_files, list)
    dbg.dassert_lte(1, len(req_files))
    out_files = []
    for req_file in req_files:
        out_file = _process_requirements_file(req_file)
        out_files.append(out_file)
    return out_files


def _parse():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--delete_env_if_exists", action="store_true")
    parser.add_argument(
        "--env_name", help="Environment name", default="develop", type=str
    )
    parser.add_argument("--yaml", action="store_true")
    parser.add_argument("--req_file",
                        action="append",
                        default=[],
                        help="Requirement file")
    # Debug options.
    parser.add_argument(
        "--test_install",
        action="store_true",
        help="Just test the install step",
    )
    parser.add_argument(
        "--python_version", default="3.7", type=str, action="store"
    )
    parser.add_argument("--skip_delete_env", action="store_true")
    parser.add_argument("--skip_install_env", action="store_true")
    #
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )
    return parser


def _main(parser):
    args = parser.parse_args()
    dbg.dassert_is_not(args.env_name, None)
    #
    dbg.init_logger(verb=args.log_level, use_exec_path=True)
    _LOG.info("\n%s", env.get_system_info(add_frame=True))
    dbg.dassert_exists(_REQUIREMENT_DIR)
    dbg.dassert_exists(_CONDA_ENVS_DIR)
    #
    # TODO(gp): Break in a sequence of functions to highlight the structure.
    delete_old_conda_if_exists = args.delete_env_if_exists
    install_new_conda = True
    #
    # Set conda root dir.
    #
    conda_env_path = usc.get_credentials()["conda_env_path"]
    hco.set_conda_env_root(conda_env_path)
    #
    # conda info
    #
    _LOG.info("\n%s", pri.frame("Current conda status"))
    cmd = "conda info"
    hco.conda_system(cmd, suppress_output=False)
    #
    # TODO(gp): Clean up cache, if needed.
    #
    # Deactivate conda.
    #
    _LOG.info("\n%s", pri.frame("Check conda status after deactivation"))
    cmd = "conda deactivate; conda info --envs"
    hco.conda_system(cmd, suppress_output=False)
    #
    # Create a package from scratch (otherwise conda is unhappy).
    #
    conda_env_name = args.env_name
    if args.test_install:
        conda_env_name = "test_conda"
    _LOG.info(
        "\n%s", pri.frame("Delete old conda env '%s', if exists" % conda_env_name)
    )
    if args.skip_delete_env:
        _LOG.warning("Skipping")
    else:
        conda_env_dict, _ = hco.get_conda_info_envs()
        conda_env_root = hco.get_conda_envs_dirs()[0]
        conda_env_path = os.path.join(conda_env_root, conda_env_name)
        if (
            conda_env_name in conda_env_dict
            or
            # Sometimes conda is flaky and says that there is no env, even if
            # the dir exists.
            os.path.exists(conda_env_path)
        ):
            _LOG.warning("Conda env '%s' exists", conda_env_path)
            if delete_old_conda_if_exists:
                #
                # Back up the old environment.
                #
                # TODO(gp): Do this.
                #
                # Remove old dir to make conda happy.
                #
                _LOG.warning("Deleting conda env '%s'", conda_env_path)
                # $CONDA remove -y -n $ENV_NAME --all
                cmd = "conda deactivate; rm -rf %s" % conda_env_path
                hco.conda_system(cmd, suppress_output=False)
            else:
                msg = (
                    "Conda env '%s' already exists. You need to use"
                    " --delete_env_if_exists to delete it" % conda_env_name
                )
                _LOG.error(msg)
                sys.exit(-1)
        else:
            _LOG.warning("Skipping")
    #
    # Process requirements file.
    #
    _LOG.info("\n%s", pri.frame("Create new conda env '%s'" % conda_env_name))
    if args.skip_install_env:
        _LOG.warning("Skipping")
    else:
        if install_new_conda:
            #
            # Install.
            #
            cmd = []
            if args.yaml:
                cmd.append("conda env create")
            else:
                cmd.append("conda create")
                # Start installation without prompting the user.
                cmd.append("--yes")
                cmd.append("--name %s" % conda_env_name)
                # cmd.append("--override-channels")
                # TODO(gp): Move to yaml?
                cmd.append("-c conda-forge")
            if args.test_install:
                pass
            else:
                req_files = args.req_file
                tmp_req_files = _process_requirements_files(req_files)
                cmd.append(" ".join(["--file %s" % f for f in tmp_req_files]))
            if _PYTHON_VERSION is not None:
                cmd.append("python=%s" % _PYTHON_VERSION)
            cmd = " ".join(cmd)
            hco.conda_system(cmd, suppress_output=False)
        else:
            _LOG.warning("Skipping")
    #
    # Test activating.
    #
    _LOG.info("\n%s", pri.frame("Test activate"))
    cmd = "conda activate %s && conda info --envs" % conda_env_name
    hco.conda_system(cmd, suppress_output=False)
    #
    # Check packages.
    #
    _, file_name = env.save_env_file(conda_env_name, _CONDA_ENVS_DIR)
    # TODO(gp): Not happy to save all the package list in amp. It should go in
    #  a spot with respect to the git root.
    _LOG.warning(
        "You should commit the file '%s' for future reference", file_name
    )
    #
    _LOG.info("DONE")


if __name__ == "__main__":
    _main(_parse())
