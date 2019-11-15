#!/usr/bin/env python

"""
This script should have *no* dependencies from anything: it should be able to run
before setenv.sh, on a new system with nothing installed. It can use //amp
libraries.

# Install the `amp` default environment:
> create_conda.py --env_name amp_develop --req_file dev_scripts/install/requirements/amp_develop.yaml --delete_env_if_exists

# Install the `p1_develop` default environment:
> create_conda.py --env_name p1_develop --req_file amp/dev_scripts/install/requirements/amp_develop.yaml --req_file dev_scripts/install/requirements/p1_develop.yaml --delete_env_if_exists

# Quick install to test the script:
> create_conda.py --test_install -v DEBUG

# Test the `develop` environment with a different name before switching the old
# develop env:
> create_conda.py --env_name develop_test --req_file dev_scripts/install/requirements/amp_develop.yaml --delete_env_if_exists
"""

import argparse
import logging
import os
import sys

import helpers.parser as prsr

# Dir of the current create_conda.py.
_CURR_DIR = os.path.dirname(sys.argv[0])

# This script is `//amp/dev_scripts/install/create_conda.py`, so we need to go
# up two levels to reach `//amp`.
_AMP_REL_PATH = "../.."
_AMP_PATH = os.path.abspath(os.path.join(_CURR_DIR, _AMP_REL_PATH))
assert os.path.exists(_AMP_PATH), "Can't find '%s'" % _AMP_PATH
sys.path.insert(0, _AMP_PATH)
# pylint: disable=wrong-import-position
import dev_scripts._bootstrap as boot  # isort:skip # noqa: E402

boot.bootstrap(_AMP_REL_PATH)

# pylint: disable=wrong-import-position
import helpers.conda as hco  # isort:skip # noqa: E402
import helpers.dbg as dbg  # isort:skip # noqa: E402
import helpers.env as env  # isort:skip # noqa: E402
import helpers.io_ as io_  # isort:skip # noqa: E402
import helpers.printing as prnt  # isort:skip # noqa: E402
import helpers.user_credentials as usc  # isort:skip # noqa: E402

_LOG = logging.getLogger(__name__)


# The following paths are expressed relative to create_conda.py.
# TODO(gp): Allow them to tweak so we can be independent with respect to amp.
#  dev_scripts/install/requirements
_REQUIREMENTS_DIR = os.path.abspath(os.path.join(_CURR_DIR, "requirements"))

# dev_scripts/install/conda_envs
_CONDA_ENVS_DIR = os.path.abspath(os.path.join(_CURR_DIR, "conda_envs"))

# ##############################################################################


def _set_conda_root_dir():
    conda_env_path = usc.get_credentials()["conda_env_path"]
    hco.set_conda_env_root(conda_env_path)
    #
    # conda info
    #
    _LOG.info("\n%s", prnt.frame("Current conda status"))
    cmd = "conda info"
    hco.conda_system(cmd, suppress_output=False)


def _delete_conda_env(args, conda_env_name):
    """
    Deactivate current conda environment and delete the old conda env.
    """
    # TODO(gp): Clean up cache, if needed.
    #
    # Deactivate conda.
    #
    _LOG.info("\n%s", prnt.frame("Check conda status after deactivation"))
    #
    cmd = "conda deactivate; conda info --envs"
    hco.conda_system(cmd, suppress_output=False)
    #
    # Create a package from scratch (otherwise conda is unhappy).
    #
    _LOG.info(
        "\n%s",
        prnt.frame("Delete old conda env '%s', if exists" % conda_env_name),
    )
    conda_env_dict, _ = hco.get_conda_info_envs()
    conda_env_root = hco.get_conda_envs_dirs()[0]
    conda_env_path = os.path.join(conda_env_root, conda_env_name)
    #
    conda_env_exists = conda_env_name in conda_env_dict
    _LOG.debug(
        "conda_env_name=%s in conda_env_dict=%s -> conda_env_exists=%s",
        conda_env_name,
        conda_env_dict,
        conda_env_exists,
    )
    # Sometimes conda is flaky and says that there is no env, even if the dir
    # exists.
    conda_env_exists = conda_env_exists or os.path.exists(conda_env_path)
    _LOG.debug(
        "conda_env_path=%s -> conda_env_exists=%s",
        conda_env_path,
        conda_env_exists,
    )
    if conda_env_exists:
        _LOG.warning("Conda env '%s' exists", conda_env_path)
        if args.delete_env_if_exists:
            # Back up the old environment.
            # TODO(gp): Do this.
            # Remove old dir to make conda happy.
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
        _LOG.warning("Skipping deleting environment")


def _process_requirements_file(req_file):
    """
    Process a requirement file to allow conditional builds.

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
    for line in txt_tmp:
        # TODO(gp): Can one do conditional builds for different machines?
        #  I don't think so.
        if "# Not on Mac." in line:
            continue
        txt.append(line)
    # Save file.
    txt = "\n".join(txt)
    dst_req_file = os.path.join(
        os.path.dirname(req_file), "tmp." + os.path.basename(req_file)
    )
    io_.to_file(dst_req_file, txt)
    return dst_req_file


def _process_requirements_files(req_files):
    """
    Apply _process_requirements_file() to multiple files.

    :return: list of names of the transformed files.
    """
    dbg.dassert_isinstance(req_files, list)
    dbg.dassert_lte(1, len(req_files))
    _LOG.debug("req_files=%s", req_files)
    out_files = []
    for req_file in req_files:
        out_file = _process_requirements_file(req_file)
        out_files.append(out_file)
    return out_files


def _create_conda_env(args, conda_env_name):
    """
    Process requirements file and create conda env.
    """
    _LOG.info("\n%s", prnt.frame("Create new conda env '%s'" % conda_env_name))
    #
    if args.test_install:
        cmd = f"conda create --yes --name {conda_env_name} -c conda-forge"
    else:
        cmd = []
        # Extract extensions.
        extensions = set()
        for req_file in args.req_file:
            dbg.dassert_exists(req_file)
            _, file_extension = os.path.splitext(req_file)
            extensions.add(file_extension)
        dbg.dassert_eq(
            len(extensions),
            1,
            "There should be only one type of extension: found %s",
            extensions,
        )
        extension = list(extensions)[0]
        _LOG.debug("extension='%s'", extension)
        dbg.dassert_in(extension, (".txt", ".yaml"), "Invalid req file extension")
        if extension == ".yaml":
            cmd.append("conda env create")
        else:
            dbg.dassert_eq(extension, ".txt")
            cmd.append("conda create")
            # Start installation without prompting the user.
            cmd.append("--yes")
            # cmd.append("--override-channels")
            cmd.append("-c conda-forge")
        cmd.append("--name %s" % conda_env_name)
        req_files = args.req_file
        tmp_req_files = _process_requirements_files(req_files)
        _LOG.debug("tmp_req_files=%s", tmp_req_files)
        # Report the files so we can see what we are actually installing.
        for f in tmp_req_files:
            _LOG.debug("tmp_req_file=%s\n%s", f, io_.from_file(f, split=False))
        # TODO(gp): Merge the yaml files (see #579).
        # We leverage the fact that `conda create` can merge multiple
        # requirements files.
        cmd.append(" ".join(["--file %s" % f for f in tmp_req_files]))
        if args.python_version is not None:
            cmd.append("python=%s" % args.python_version)
        cmd = " ".join(cmd)
    hco.conda_system(cmd, suppress_output=False)


def _test_conda_env(conda_env_name):
    # Test activating.
    _LOG.info("\n%s", prnt.frame("Test activate conda env '%s'" % conda_env_name))
    cmd = "conda activate %s && conda info --envs" % conda_env_name
    hco.conda_system(cmd, suppress_output=False)
    # Check packages.
    _, file_name = env.save_env_file(conda_env_name, _CONDA_ENVS_DIR)
    # TODO(gp): Not happy to save all the package list in amp. It should go in
    #  a spot with respect to the git root.
    _LOG.warning(
        "You should commit the file '%s' for future reference", file_name
    )


# ##############################################################################


def _parse():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--delete_env_if_exists", action="store_true")
    parser.add_argument(
        "--env_name", type=str, required=True, help="Environment name"
    )
    parser.add_argument(
        "--req_file",
        action="append",
        default=[],
        required=True,
        help="Requirements file",
    )
    # Debug options.
    parser.add_argument(
        "--test_install",
        action="store_true",
        help="Test the install step without requirements",
    )
    parser.add_argument(
        "--python_version", default=None, type=str, action="store"
    )
    parser.add_argument("--skip_delete_env", action="store_true")
    parser.add_argument("--skip_install_env", action="store_true")
    parser.add_argument("--skip_test_env", action="store_true")
    #
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser):
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    #
    _LOG.info("\n%s", env.get_system_info(add_frame=True))
    dbg.dassert_exists(_REQUIREMENTS_DIR)
    dbg.dassert_exists(_CONDA_ENVS_DIR)
    #
    _set_conda_root_dir()
    #
    conda_env_name = args.env_name
    #
    if args.skip_delete_env:
        _LOG.warning("Skip delete conda env per user request")
    else:
        _delete_conda_env(args, conda_env_name)
    #
    if args.skip_install_env:
        _LOG.warning("Skip create conda env")
    else:
        _create_conda_env(args, conda_env_name)
    #
    if args.skip_test_env:
        _LOG.warning("Skip test conda env")
    else:
        _test_conda_env(conda_env_name)
    #
    _LOG.info("DONE")


if __name__ == "__main__":
    _main(_parse())
