#!/usr/bin/env python
"""
# Install the default package
> create_conda.py
> create_conda.py --env_name develop --req_file dev_scripts/install/requirements/requirements_develop.txt --delete_env_if_exists

# Quick install to test the script
> create_conda.py --test_install -v DEBUG

# Test the develop environment
> create_conda.py --env_name develop_test --req_file dev_scripts/install/requirements/requirements_develop.txt --delete_env_if_exists

# Create pymc3
> create_conda.py --env_name pymc3 --req_file requirements_pymc.txt -v DEBUG
"""

import argparse
import logging
import os
import sys

import helpers.conda as hco
import helpers.dbg as dbg
import helpers.env as env
import helpers.io_ as io_
import helpers.printing as pri
import helpers.user_credentials as usc

_LOG = logging.getLogger(__name__)

# _PYTHON_VERSION = "2.7"
# _PYTHON_VERSION = "3.7"
_PYTHON_VERSION = None

# TODO(gp): Try https://github.com/mwilliamson/stickytape. It doesn't work
# that well.
# > cd ~/src/github/stickytape && conda activate develop && python setup.py install
# > python /usr/local/lib/python2.7/site-packages/stickytape/main.py dev_scripts/create_conda.py --add-python-path . --output-file released_sticky.py

# TODO(gp): Allow yml files with pip deps inside
# https://stackoverflow.com/questions/35245401/combining-conda-environment-yml-with-pip-requirements-txt

# ##############################################################################

# dev_scripts/install/requirements
_CURR_DIR = os.path.dirname(sys.argv[0])

_REQUIREMENT_DIR = os.path.abspath(
    os.path.join(_CURR_DIR,
        "requirements"))

# dev_scripts/install/conda_envs
_CONDA_ENVS_DIR = os.path.abspath(
    os.path.join(_CURR_DIR,
                 "conda_envs"))


def _get_requirements_file():
    res = None
    file_name = os.path.join(_REQUIREMENT_DIR, "requirements.txt")
    if os.path.exists(file_name):
        res = file_name
    if res is None:
        raise RuntimeError("Can't find the requirements file")
    return res


def _process_requirements(req_file):
    """
    - Read req_file
    - Skip lines like:
    # docx    # Not on Mac.
    - Write the result in a tmp file
    :return: name of the new file
    """
    # Read file.
    req_file = os.path.abspath(req_file)
    _LOG.debug("req_file=%s", req_file)
    dbg.dassert_exists(req_file)
    txt = io_.from_file(req_file, split=True)
    # Process.
    txt_tmp = []
    for l in txt:
        if "# Not on Mac." in l:
            continue
        txt_tmp.append(l)
    txt_tmp = "\n".join(txt_tmp)
    # Save file.
    dst_req_file = req_file + ".tmp"
    io_.to_file(dst_req_file, txt_tmp)
    return dst_req_file


def _parse():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--delete_env_if_exists", action="store_true")
    parser.add_argument(
        "--env_name", help="Environment name", default="develop", type=str
    )
    parser.add_argument(
        "--req_file", help="Requirement file", default=None, type=str
    )
    # Debug options.
    parser.add_argument(
        "--test_install", help="Just test the install step", action="store_true"
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
        "\n%s",
        pri.frame("Delete old conda env '%s', if exists" % conda_env_name),
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
            # Sometimes conda is flaky and says that there is no env, even if the dir exists.
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
            if args.req_file is None:
                req_file = _get_requirements_file()
            else:
                req_file = args.req_file
            tmp_req_file = _process_requirements(req_file)
            #
            # Install.
            #
            cmd = (
                # yapf: disable
                "conda create" + " --yes" + " --name %s" % conda_env_name
                # yapf: enable
            )
            if args.test_install:
                pass
            else:
                cmd += (
                    # yapf: disable
                    #" --override-channels " +
                    " -c conda-forge" + " --file %s" % tmp_req_file
                    # yapf: enable
                )
            if _PYTHON_VERSION is not None:
                cmd += " python=%s" % _PYTHON_VERSION
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
    # Install other stuff if needed.
    #
    # TODO(gp): Do this.
    #
    # pip install git+https://github.com/dadadel/pyment.git
    #
    # Check packages.
    #
    _, file_name = env.save_env_file(conda_env_name, _CONDA_ENVS_DIR)
    _LOG.warning(
        "You should commit the file '%s' for future reference", file_name
    )
    #
    _LOG.info("DONE")


if __name__ == "__main__":
    _main(_parse())
