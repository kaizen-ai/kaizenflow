"""
Import as:

import _setenv_lib as selib

This library contains functions used by a `setenv_*.py` script in a repo to
generate a bash script that is executed to configure the development
environment.
"""

import argparse
import logging
import os
import sys

import helpers.dbg as dbg  # isort:skip # noqa: E402
import helpers.io_ as io_  # isort:skip # noqa: E402
import helpers.parser as prsr  # isort:skip # noqa: E402
import helpers.system_interaction as si  # isort:skip # noqa: E402


_LOG = logging.getLogger(__name__)

# ##############################################################################
# Helpers for the phases.
# ##############################################################################


# TODO(gp): This is kind of useless since the cleaning of the path is done at
#  run-time now.
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
    Create a snippet of bash script equivalent to the following:

    # Update variable.
    PYTHONPATH=$CURR_DIR:$PYTHONPATH
    # Remove redundant paths.
    PYTHONPATH="$(echo $PYTHONPATH | perl -e
      'print join(":", grep { not $seen{$_}++ } split(/:/, scalar <>))')"
    echo "PYTHONPATH=$PYTHONPATH"
    echo $PYTHONPATH | $EXEC_PATH/print_paths.sh
    """
    txt = []
    # Update variable.
    txt.append("# Update variable.")
    vals = _remove_redundant_paths(vals)
    txt.append("%s=" % val_name + ":".join(vals))
    txt_tmp = "%s=" % val_name
    txt_tmp += "$(echo $%s" % val_name
    # TODO(gp): Improve this script. It doesn't seem to work for the empty
    #  paths and with repeated paths.
    txt_tmp += (
        " | perl -e "
        + """'print join(":", grep { not $seen{$_}++ } split(/:/, scalar <>))'"""
    )
    txt_tmp += ")"
    txt.append(txt_tmp)
    #
    txt.append("# Print variable.")
    txt.append("echo %s=$%s" % (val_name, val_name))
    txt.append(
        "echo $%s" % val_name
        + """ | perl -e 'print "  "; print join("\\n  ", split(/:/, scalar <>))'"""
    )
    #
    txt.append("# Export variable.")
    txt.append("export %s" % val_name)
    return txt


def _frame(comment, txt):
    txt_tmp = []
    line = "#" * 80
    txt_tmp.append("\necho '%s'" % line)
    txt_tmp.append("echo " + comment)
    txt_tmp.append("echo '%s'" % line)
    txt.extend(txt_tmp)


def _execute(cmd, txt):
    txt.append("echo '> %s'" % cmd)
    txt.append(cmd)


def _log_var(var_name, var_val, txt):
    txt.append("echo '%s=%s'" % (var_name, var_val))
    _LOG.debug("%s='%s'", var_name, var_val)


# ##############################################################################
# Various phases.
# ##############################################################################


def report_info(txt):
    """
    Add to the bash script `txt` diagnostic informations.

    :return:
        - client_root_path: the directory that includes the executable as
          `dev_scripts/_setenv_*.py` (i.e., the dir that is ../exec_path)
        - user_name
    """
    _frame("Info", txt)
    _log_var("cmd_line", dbg.get_command_line(), txt)
    exec_name = os.path.abspath(sys.argv[0])
    dbg.dassert_exists(exec_name)
    _log_var("exec_name", exec_name, txt)
    # Full path of this executable which is the same as setenv_*.sh.
    exec_path = os.path.dirname(exec_name)
    _log_var("exec_path", exec_path, txt)
    dbg.dassert(
        os.path.basename(exec_path), "dev_scripts", "exec_path=%s", exec_path
    )
    # Get the path of the root of the Git client.
    client_root_path = os.path.abspath(os.path.join(exec_path, ".."))
    dbg.dassert_exists(client_root_path)
    # Current dir.
    curr_path = os.getcwd()
    _log_var("curr_path", curr_path, txt)
    # Get name.
    user_name = si.get_user_name()
    _log_var("user_name", user_name, txt)
    server_name = si.get_server_name()
    _log_var("server_name", server_name, txt)
    return client_root_path, user_name


def config_git(user_name, user_credentials, txt):
    """
    Add to the bash script in `txt` instructions to:
        - configure git user name and user email
    """
    _frame("Config git", txt)
    git_user_name = user_credentials["git_user_name"]
    if git_user_name:
        cmd = 'git config --local user.name "%s"' % git_user_name
        _execute(cmd, txt)
    #
    git_user_email = user_credentials["git_user_email"]
    if git_user_email:
        cmd = 'git config --local user.email "%s"' % git_user_email
        _execute(cmd, txt)
    #
    if user_name == "jenkins":
        cmd = "git config --list"
        _execute(cmd, txt)


def config_python(dirs, txt):
    """
    Add to the bash script `txt` instructions to configure python by:
        - disable python caching
        - appending the submodule path to $PYTHONPATH
    """
    _frame("Config python", txt)
    #
    txt.append("# Disable python code caching.")
    txt.append("export PYTHONDONTWRITEBYTECODE=x")
    #
    txt.append("# Append paths to PYTHONPATH.")
    dbg.dassert_isinstance(dirs, list)
    dirs = sorted(dirs)
    dirs = [os.path.abspath(d) for d in dirs]
    for d in dirs:
        dbg.dassert_exists(d)
    python_path = dirs + ["$PYTHONPATH"]
    txt.extend(_export_env_var("PYTHONPATH", python_path))


def config_conda(conda_env, user_credentials, txt):
    """
    Add to the bash script `txt` instructions to activate a conda environment.
    """
    _frame("Config conda", txt)
    #
    conda_sh_path = user_credentials["conda_sh_path"]
    _log_var("conda_sh_path", conda_sh_path, txt)
    # TODO(gp): This makes conda not working for some reason.
    # txt.append("source %s" % conda_sh_path)
    dbg.dassert_exists(conda_sh_path)
    #
    txt.append('echo "CONDA_PATH="$(which conda)')
    txt.append('echo "CONDA_VER="$(conda -V)')
    #
    cmd = "conda info --envs"
    _execute(cmd, txt)
    #
    cmd = "which python"
    _execute(cmd, txt)
    #
    cmd = "conda activate %s" % conda_env
    _execute(cmd, txt)
    #
    cmd = "conda info --envs"
    _execute(cmd, txt)
    #
    cmd = "which python"
    _execute(cmd, txt)


def get_dev_scripts_subdirs():
    return (".", "aws", "git", "infra", "install", "notebooks",
            "testing")


def config_path(dirs, txt):
    """
    Prepend to PATH the directories `dirs` rooted in `path`.
    """
    _frame("Config path", txt)
    dbg.dassert_isinstance(dirs, list)
    dirs = sorted(dirs)
    dirs = [os.path.abspath(d) for d in dirs]
    for d in dirs:
        dbg.dassert_exists(d)
    path = dirs + ["$PATH"]
    txt.extend(_export_env_var("PATH", path))


def test_packages(amp_path, txt):
    _frame("Test packages", txt)
    script = os.path.join(
        amp_path, "dev_scripts/install/check_develop_packages.py"
    )
    script = os.path.abspath(script)
    dbg.dassert_exists(script)
    _execute(script, txt)


def save_script(args, txt):
    txt = "\n".join(txt)
    if args.output_file:
        io_.to_file(args.output_file, txt)
    else:
        # stdout.
        print(txt)


# ##############################################################################


def parse():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "-o", "--output_file", default=None, help="File to write. None for stdout"
    )
    parser.add_argument(
        "-e", "--conda_env", default=None, help="Select the conda env to use."
    )
    prsr.add_verbosity_arg(parser)
    # To capture some dummy command options (e.g., "bell-style none") passed by
    # source on some systems.
    parser.add_argument("positional", nargs="*", help="...")
    return parser
