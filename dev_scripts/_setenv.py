#!/usr/bin/env python

"""
Generate and print a bash script that is used to configure the environment.

This script:
- is used to configure the environment
- should have no dependency other than basic python library
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


# This script is dev_scripts/_setenv.sh, so we need to go up one level to reach
# "helpers".
_bootstrap("..")


import helpers.dbg as dbg  # isort:skip
import helpers.io_ as io_  # isort:skip
import helpers.system_interaction as si  # isort:skip
import helpers.user_credentials as usc  # isort:skip

# ##############################################################################

_LOG = logging.getLogger(__name__)


# TODO(gp): This is kind of useless since the cleaning of the path is done at
# run-time now.
def _remove_redundant_paths(paths):
    # Set of unique paths.
    found_paths = set()
    paths_out = []
    for p in paths:
        if p == "":
            continue
        if p not in found_paths:
            # Found a new path. Add it to the output.
            found_paths.add(p)
            paths_out.append(p)
    return paths_out


def _export_env_var(val_name, vals):
    """
    Create snippet of bash script equivalent to the following:

    PYTHONPATH=$CURR_DIR:$PYTHONPATH
    # Remove redundant paths.
    PYTHONPATH="$(echo $PYTHONPATH | perl -e
      'print join(":", grep { not $seen{$_}++ } split(/:/, scalar <>))')"
    echo "PYTHONPATH=$PYTHONPATH"
    echo $PYTHONPATH | $EXEC_PATH/print_paths.sh
    """
    txt = []
    vals = _remove_redundant_paths(vals)
    txt.append("%s=" % val_name + ":".join(vals))
    txt_tmp = "%s=" % val_name
    txt_tmp += "$(echo $%s" % val_name
    # TODO(gp): Improve this script. It doesn't seem to work for the empty
    # paths and with repeated paths.
    txt_tmp += (
        " | perl -e "
        + """'print join(":", grep { not $seen{$_}++ } split(/:/, scalar <>))'"""
    )
    txt_tmp += ")"
    txt.append(txt_tmp)
    txt.append("echo %s=$%s" % (val_name, val_name))
    txt.append(
        "echo $%s" % val_name
        + """ | perl -e 'print "  "; print join("\\n  ", split(/:/, scalar <>))'"""
    )
    return txt


# ##############################################################################


def _parse():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "-o", "--output_file", default=None, help="File to write. None for stdout"
    )
    parser.add_argument(
        "-e", "--conda_env", default="develop", help="Select the conda env to use"
    )
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )
    # To capture some dummy command options (e.g., "bell-style none") passed by
    # source on some systems.
    parser.add_argument("positional", nargs="*", help="...")
    return parser


def _main(parser):
    args = parser.parse_args()
    dbg.init_logger(verb=args.log_level)
    #
    txt = []

    def _frame(comment):
        txt_tmp = []
        line = "#" * 80
        txt_tmp.append("\necho '%s'" % line)
        txt_tmp.append("echo " + comment)
        txt_tmp.append("echo '%s'" % line)
        txt.extend(txt_tmp)

    def _execute(cmd):
        txt.append("echo '> %s'" % cmd)
        txt.append(cmd)

    def _log_var(var_name, var_val):
        txt.append("echo '%s=%s'" % (var_name, var_val))
        _LOG.debug("%s='%s'", var_name, var_val)

    #
    # Info.
    #
    _frame("Info")
    _log_var("cmd_line", dbg.get_command_line())
    # TODO(gp): Use git to make sure you are in the root of the repo to
    # configure the environment.
    exec_name = os.path.abspath(sys.argv[0])
    dbg.dassert_exists(exec_name)
    _log_var("exec_name", exec_name)
    # Full path of this executable which is the same as setenv.sh.
    exec_path = os.path.dirname(exec_name)
    _log_var("exec_path", exec_path)
    dbg.dassert(
        os.path.basename(exec_path), "dev_scripts", "exec_path=%s", exec_path
    )
    # Get the path of helpers.
    submodule_path = os.path.abspath(os.path.join(exec_path, ".."))
    dbg.dassert_exists(submodule_path)
    # Current dir.
    curr_path = os.getcwd()
    _log_var("curr_path", curr_path)
    # Get name.
    user_name = si.get_user_name()
    _log_var("user_name", user_name)
    server_name = si.get_server_name()
    _log_var("server_name", server_name)
    #
    # Check conda.
    #
    # TODO(gp): After updating to anaconda3, conda doesn't work from python anymore.
    # cmd = "conda -V"
    # si.system(cmd)
    #
    # Config git.
    #
    _frame("Config git")
    user_credentials = usc.get_credentials()
    git_user_name = user_credentials["git_user_name"]
    if git_user_name:
        cmd = 'git config --local user.name "%s"' % git_user_name
        _execute(cmd)
    #
    git_user_email = user_credentials["git_user_email"]
    if git_user_email:
        cmd = 'git config --local user.email "%s"' % git_user_email
        _execute(cmd)
    #
    if user_name == "jenkins":
        cmd = "git config --list"
        _execute(cmd)
    #
    # Config python.
    #
    _frame("Config python")
    #
    txt.append("# Disable python code caching.")
    txt.append("export PYTHONDONTWRITEBYTECODE=x")
    #
    txt.append("# Append current dir to PYTHONPATH.")
    pythonpath = [submodule_path] + ["$PYTHONPATH"]
    txt.extend(_export_env_var("PYTHONPATH", pythonpath))
    #
    # Config conda.
    #
    _frame("Config conda")
    #
    conda_sh_path = user_credentials["conda_sh_path"]
    _log_var("conda_sh_path", conda_sh_path)
    # TODO(gp): This makes conda not working for some reason.
    # txt.append("source %s" % conda_sh_path)
    dbg.dassert_exists(conda_sh_path)
    #
    txt.append('echo "CONDA_PATH="$(which conda)')
    txt.append('echo "CONDA_VER="$(conda -V)')
    cmd = "conda info --envs"
    _execute(cmd)
    cmd = "conda activate %s" % args.conda_env
    _execute(cmd)
    cmd = "conda info --envs"
    _execute(cmd)
    #
    # Config bash.
    #
    _frame("Config bash")
    #
    dirs = [".", "aws", "infra", "install", "notebooks"]
    dirs = sorted(dirs)
    dirs = [os.path.abspath(os.path.join(exec_path, d)) for d in dirs]
    for d in dirs:
        dbg.dassert_exists(d)
    path = dirs + ["$PATH"]
    txt.extend(_export_env_var("PATH", path))
    #
    # Test packages.
    #
    _frame("Test packages")
    script = os.path.join(exec_path, "install/check_develop_packages.py")
    script = os.path.abspath(script)
    dbg.dassert_exists(script)
    _execute(script)
    #
    txt = "\n".join(txt)
    if args.output_file:
        io_.to_file(args.output_file, txt)
    else:
        # stdout.
        print(txt)


if __name__ == "__main__":
    _main(_parse())
