"""
Import as:

import _setenv_lib as selib

This library contains functions used by a `setenv.py` script in a repo to
generate a bash script that is executed to configure the development
environment.
"""

import argparse
import logging
import os
import sys

import helpers.dbg as dbg  # isort:skip # noqa: E402
import helpers.io_ as io_  # isort:skip # noqa: E402
import helpers.system_interaction as si  # isort:skip # noqa: E402
import helpers.user_credentials as usc  # isort:skip # noqa: E402


_LOG = logging.getLogger(__name__)

# ##############################################################################
# Helpers for the phases.
# ##############################################################################


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
    _frame("Info", txt)
    _log_var("cmd_line", dbg.get_command_line(), txt)
    # TODO(gp): Use git to make sure you are in the root of the repo to
    # configure the environment.
    exec_name = os.path.abspath(sys.argv[0])
    dbg.dassert_exists(exec_name)
    _log_var("exec_name", exec_name, txt)
    # Full path of this executable which is the same as setenv.sh.
    exec_path = os.path.dirname(exec_name)
    _log_var("exec_path", exec_path, txt)
    dbg.dassert(
        os.path.basename(exec_path), "dev_scripts", "exec_path=%s", exec_path
    )
    # Get the path of helpers.
    submodule_path = os.path.abspath(os.path.join(exec_path, ".."))
    dbg.dassert_exists(submodule_path)
    # Current dir.
    curr_path = os.getcwd()
    _log_var("curr_path", curr_path, txt)
    # Get name.
    user_name = si.get_user_name()
    _log_var("user_name", user_name, txt)
    server_name = si.get_server_name()
    _log_var("server_name", server_name, txt)
    return exec_path, submodule_path, user_name


def _check_conda():
    cmd = "conda -V"
    si.system(cmd)


def config_git(user_name, txt):
    _frame("Config git", txt)
    user_credentials = usc.get_credentials()
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
    return user_credentials


def config_python(submodule_path, txt):
    _frame("Config python", txt)
    #
    txt.append("# Disable python code caching.")
    txt.append("export PYTHONDONTWRITEBYTECODE=x")
    #
    txt.append("# Append current dir to PYTHONPATH.")
    pythonpath = [submodule_path] + ["$PYTHONPATH"]
    txt.extend(_export_env_var("PYTHONPATH", pythonpath))


def config_conda(args, txt, user_credentials):
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
    cmd = "conda info --envs"
    _execute(cmd, txt)
    cmd = "conda activate %s" % args.conda_env
    _execute(cmd, txt)
    cmd = "conda info --envs"
    _execute(cmd, txt)


def config_bash(exec_path, txt):
    _frame("Config bash", txt)
    #
    dirs = [".", "aws", "infra", "install", "notebooks"]
    dirs = sorted(dirs)
    dirs = [os.path.abspath(os.path.join(exec_path, d)) for d in dirs]
    for d in dirs:
        dbg.dassert_exists(d)
    path = dirs + ["$PATH"]
    txt.extend(_export_env_var("PATH", path))


def test_packages(exec_path, txt):
    _frame("Test packages", txt)
    script = os.path.join(exec_path, "install/check_develop_packages.py")
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
