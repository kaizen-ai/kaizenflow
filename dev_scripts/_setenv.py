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
_PATH = str(os.environ["PATH"])
_PYTHONPATH = str(os.environ["PYTHONPATH"])


def _config_env():
    """
    Tweak PYTHONPATH to pick up amp, even if we are configuring amp, breaking the
    circular dependency.
    """
    exec_name = os.path.abspath(sys.argv[0])
    # This script is in dev_scripts, which is at the same level of "helpers".
    amp_path = os.path.abspath(os.path.join(os.path.dirname(exec_name), ".."))
    # Check that helpers exists.
    helpers_path = os.path.join(amp_path, "helpers")
    assert os.path.exists(helpers_path), "Can't find '%s'" % helpers_path
    # Update path.
    if False:
        print("PYTHONPATH=%s" % _PYTHONPATH)
        print("amp_path=%s" % amp_path)
    # We can't update os.environ since the script is already running.
    sys.path.insert(0, amp_path)
    # Test the imports.
    try:
        pass
    except ImportError as e:
        print("PYTHONPATH=%s" % _PYTHONPATH)
        print("amp_path=%s" % amp_path)
        raise e


# We need to tweak the PYTHONPATH before importing.
_config_env()


import helpers.dbg as dbg  # isort:skip
import helpers.system_interaction as si  # isort:skip
import helpers.user_credentials as usc  # isort:skip

_LOG = logging.getLogger(__name__)


# ##############################################################################


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
    txt_tmp += " | perl -e " + \
        """'print join(":", grep { not $seen{$_}++ } split(/:/, scalar <>))'"""
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

    def _log_var(var_name, var_val):
        txt.append("echo '%s=%s'" % (var_name, var_val))
        _LOG.debug("%s='%s'", var_name, var_val)

    #
    # Info.
    #
    _frame("Info")
    _log_var("cmd_line", dbg.get_command_line())
    # Full path of this executable.
    exec_name = os.path.abspath(sys.argv[0])
    dbg.dassert_dir_exists(exec_name)
    _log_var("exec_name", exec_name)
    # TODO(gp): Use git to make sure you are in the root of the repo to
    # configure the environment.
    exec_path = os.path.dirname(exec_name)
    _log_var("exec_path", exec_path)
    dbg.dassert(os.path.basename(exec_path), "dev_scripts",
            "exec_path=%s", exec_path)
    # Current dir.
    curr_path = os.getcwd()
    _log_var("curr_path", curr_path)
    # Get name.
    user_name = si.get_user_name()
    _log_var("user_name", user_name)
    server_name = si.get_server_name()
    _log_var("server_name", server_name)
    #
    # Config git.
    #
    _frame("Config git")
    user_credentials = usc.get_credentials(user_name, server_name)
    git_user_name = user_credentials["git_user_name"]
    if git_user_name:
        txt.append('git config --local user.name "%s"' % git_user_name)
    #
    git_user_email = user_credentials["git_user_email"]
    if git_user_email:
        txt.append('git config --local user.email "%s"' % git_user_email)
    #
    if user_name == "jenkins":
        cmd = r"git config --list | \grep %s" % user_name
        txt.append(cmd)
    #
    # Config python.
    #
    _frame("Config python")
    #
    txt.append("# Disable python code caching.")
    txt.append("export PYTHONDONTWRITEBYTECODE=x")
    #
    txt.append("# Append current dir to PYTHONPATH.")
    pythonpath = [curr_path] + ["$PYTHONPATH"]
    txt.extend(_export_env_var("PYTHONPATH", pythonpath))
    #
    # Config conda.
    #
    _frame("Config conda")
    #
    conda_sh_path = user_credentials["conda_sh_path"]
    _log_var("conda_sh_path", conda_sh_path)
    # TODO(gp): This makes conda not working for some reason.
    #txt.append("source %s" % conda_sh_path)
    dbg.dassert_exists(conda_sh_path)
    #
    txt.append('echo "CONDA_PATH="$(which conda)')
    txt.append('echo "CONDA_VER="$(conda -V)')
    txt.append("conda info --envs")
    txt.append("conda activate %s" % args.conda_env)
    txt.append("conda info --envs")
    #
    # Config bash.
    #
    _frame("Config bash")
    #
    dirs = [".", "aws", "infra", "install", "ipynb_script"]
    dirs = sorted(dirs)
    dirs = [os.path.abspath(os.path.join(exec_path, d)) for d in dirs]
    path = dirs + ["$PATH"]
    txt.extend(_export_env_var("PATH", path))
    #
    # Test packages.
    #
    _frame("Test packages")
    script = os.path.join(exec_path, "install/package_tester.py")
    script = os.path.abspath(script)
    dbg.dassert_exists(script)
    txt.append(script)
    #
    txt = "\n".join(txt)
    print(txt)


if __name__ == "__main__":
    _main(_parse())
