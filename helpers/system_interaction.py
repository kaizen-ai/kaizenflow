"""
Import as:

import helpers.system_interaction as si

Contain all the code needed to interact with the outside world, e.g., through
system commands, env vars, ...
"""

import getpass
import logging
import os
import shutil
import signal
import subprocess
import sys
import time
from typing import Any, List, Optional, Tuple

import helpers.dbg as dbg
import helpers.printing as prnt

_LOG = logging.getLogger(__name__)


# #############################################################################

_USER_NAME = None


def set_user_name(user_name):
    """
    To impersonate a user. To use only in rare cases.
    """
    _LOG.warning("Setting user to '%s'", user_name)
    global _USER_NAME
    _USER_NAME = user_name


def get_user_name():
    if _USER_NAME is None:
        res = getpass.getuser()
    else:
        res = _USER_NAME
    return res


def get_server_name():
    res = os.uname()
    # posix.uname_result(
    #   sysname='Darwin',
    #   nodename='gpmac.lan',
    #   release='18.2.0',
    #   version='Darwin Kernel Version 18.2.0: Mon Nov 12 20:24:46 PST 2018;
    #       root:xnu-4903.231.4~2/RELEASE_X86_64',
    #   machine='x86_64')
    # This is not compatible with python2.7
    # return res.nodename
    return res[1]


def get_os_name():
    res = os.uname()
    # This is not compatible with python2.7
    # return res.sysname
    return res[0]


def get_env_var(env_var_name):
    if env_var_name not in os.environ:
        msg = "Can't find '%s': re-run dev_scripts/setenv.sh?" % env_var_name
        _LOG.error(msg)
        raise RuntimeError(msg)
    return os.environ[env_var_name]


# #############################################################################

# pylint: disable=too-many-branches,too-many-statements,too-many-arguments,too-many-locals
def _system(
    cmd: str,
    abort_on_error: bool,
    suppress_error: Optional[Any],
    suppress_output: bool,
    blocking: bool,
    wrapper: Optional[Any],
    output_file: Optional[Any],
    tee: bool,
    dry_run: bool,
    log_level: int,
) -> Tuple[int, str]:
    """
    Execute a shell command.

    :param cmd: string with command to execute
    :param abort_on_error: whether we should assert in case of error or not
    :param suppress_error: set of error codes to suppress
    :param suppress_output: whether to print the output or not
        - If "on_debug_level" then print the output if the log level is DEBUG
    :param blocking: blocking system call or not
    :param wrapper: another command to prepend the execution of cmd
    :param output_file: redirect stdout and stderr to this file
    :param tee: if True, tee stdout and stderr to output_file
    :param dry_run: just print the final command but not execute it
    :param log_level: print the command to execute at level "log_level".
        - If "echo" then print the command line to screen as print and not
          logging
    :return: return code (int), output of the command (str)
    """
    orig_cmd = cmd[:]
    # Prepare the command line.
    cmd = "(%s)" % cmd
    dbg.dassert_imply(tee, output_file is not None)
    if output_file is not None:
        dir_name = os.path.dirname(output_file)
        if not os.path.exists(dir_name):
            _LOG.debug("Dir '%s' doesn't exist: creating", dir_name)
            dbg.dassert(bool(dir_name), "dir_name='%s'", dir_name)
            os.makedirs(dir_name)
        if tee:
            cmd += " 2>&1 | tee %s" % output_file
        else:
            cmd += " 2>&1 >%s" % output_file
    else:
        cmd += " 2>&1"
    if wrapper:
        cmd = wrapper + " && " + cmd
    #
    # TODO(gp): Add a check for the valid values.
    # TODO(gp): Make it "ECHO".
    if log_level == "echo":
        print("> %s" % orig_cmd)
        _LOG.debug(log_level, "> %s", cmd)
    else:
        _LOG.log(log_level, "> %s", cmd)
    #
    dbg.dassert_in(suppress_output, ("ON_DEBUG_LEVEL", True, False))
    if suppress_output == "ON_DEBUG_LEVEL":
        # print("eff_lev=%s" % eff_level)
        # print("lev=%s" % logging.DEBUG)
        _LOG.getEffectiveLevel()
        # Suppress the output if the verbosity level is higher than DEBUG,
        # otherwise print.
        suppress_output = _LOG.getEffectiveLevel() > logging.DEBUG
    #
    output = ""
    if dry_run:
        _LOG.warning("Not executing cmd\n%s\nas per user request", cmd)
        rc = 0
        return rc, output
    # Execute the command.
    try:
        stdout = subprocess.PIPE
        stderr = subprocess.STDOUT
        p = subprocess.Popen(
            cmd, shell=True, executable="/bin/bash", stdout=stdout, stderr=stderr
        )
        output = ""
        if blocking:
            # Blocking call: get the output.
            while True:
                line = p.stdout.readline().decode("utf-8")
                if not line:
                    break
                if not suppress_output:
                    print((line.rstrip("\n")))
                output += line
            p.stdout.close()
            rc = p.wait()
        else:
            # Not blocking.
            # Wait until process terminates (without using p.wait()).
            max_cnt = 20
            cnt = 0
            while p.poll() is None:
                # Process hasn't exited yet, let's wait some time.
                time.sleep(0.1)
                cnt += 1
                _LOG.debug("cnt=%s, rc=%s", cnt, p.returncode)
                if cnt > max_cnt:
                    break
            if cnt > max_cnt:
                # Timeout: we assume it worked.
                rc = 0
            else:
                rc = p.returncode
        if suppress_error is not None:
            dbg.dassert_isinstance(suppress_error, set)
            if rc in suppress_error:
                rc = 0
    except OSError:
        rc = -1
    _LOG.debug("rc=%s", rc)
    if abort_on_error and rc != 0:
        msg = (
            "\n"
            + prnt.frame("cmd='%s' failed with rc='%s'" % (cmd, rc))
            + "\nOutput of the failing command is:\n%s\n%s\n%s"
            % (prnt.line(">"), output, prnt.line("<"))
        )
        _LOG.error("%s", msg)
        raise RuntimeError("cmd='%s' failed with rc='%s'" % (cmd, rc))
    # dbg.dassert_type_in(output, (str, ))
    return rc, output


# pylint: disable=too-many-arguments
def system(
    cmd: str,
    abort_on_error: bool = True,
    suppressed_error: Optional[Any] = None,
    suppress_output: bool = True,
    blocking: bool = True,
    wrapper: Optional[Any] = None,
    output_file: Optional[Any] = None,
    tee: bool = False,
    dry_run: bool = False,
    log_level: int = logging.DEBUG,
) -> int:
    """
    Execute a shell command, without capturing its output.
    See _system() for options.
    """
    rc, _ = _system(
        cmd,
        abort_on_error=abort_on_error,
        suppress_error=suppressed_error,
        suppress_output=suppress_output,
        blocking=blocking,
        wrapper=wrapper,
        output_file=output_file,
        tee=tee,
        dry_run=dry_run,
        log_level=log_level,
    )
    return rc


# def _system_to_string(cmd):
#     py_ver = sys.version_info[0]
#     if py_ver == 2:
#         txt = subprocess.check_output(cmd)
#     elif py_ver == 3:
#         txt = subprocess.getoutput(cmd)
#     else:
#         raise RuntimeError("Invalid py_ver=%s" % py_ver)
#     txt = [f for f in txt.split("\n") if f]
#     dbg.dassert_eq(len(txt), 1)
#     return txt[0]


def system_to_string(
    cmd: str,
    abort_on_error: bool = True,
    wrapper: Optional[Any] = None,
    dry_run: bool = False,
    log_level: int = logging.DEBUG,
) -> Tuple[int, str]:
    """
    Execute a shell command and capture its output.
    See _system() for options.
    """
    rc, output = _system(
        cmd,
        abort_on_error=abort_on_error,
        suppress_error=None,
        suppress_output=True,
        # If we want to see the output the system call must be blocking.
        blocking=True,
        wrapper=wrapper,
        output_file=None,
        tee=False,
        dry_run=dry_run,
        log_level=log_level,
    )
    output = output.rstrip("\n")
    return rc, output


def get_first_line(output: str) -> str:
    """
    Return the first (and only) line from a string.

    This is used when calling system_to_string() and expecting a single line
    output.
    """
    output_as_arr = prnt.remove_empty_lines(output)
    dbg.dassert_eq(len(output_as_arr), 1, "output='%s'", output)
    return output_as_arr[0].rstrip().lstrip()


def system_to_one_line_string(cmd, *args, **kwargs) -> Tuple[int, str]:
    """
    Execute a shell command and capture its output (expected to be a single line).

    This is a thin wrapper around system_to_string().
    """
    rc, output = system_to_string(cmd, *args, **kwargs)
    output = get_first_line(output)
    return rc, output


# #############################################################################


def get_process_pids(keep_line) -> Tuple[List[int], List[str]]:
    """
    Find all the processes corresponding to `ps ax` filtered line by line with
    `keep_line()`.

    :return: list of pids and filtered output of `ps ax`
    """
    cmd = "ps ax"
    rc, txt = system_to_string(cmd, abort_on_error=False)
    _LOG.debug("txt=\n%s", txt)
    pids: List[int] = []
    txt_out: List[str] = []
    if rc == 0:
        for line in txt.split("\n"):
            _LOG.debug("line=%s", line)
            # PID   TT  STAT      TIME COMMAND
            if "PID" in line and "TT" in line and "STAT" in line:
                txt_out.append(line)
                continue
            keep = keep_line(line)
            _LOG.debug("  keep=%s", keep)
            if not keep:
                continue
            # > ps ax | grep 'ssh -i' | grep localhost
            # 19417   ??  Ss     0:00.39 ssh -i /Users/gp/.ssh/id_rsa -f -nNT \
            #           -L 19999:localhost:19999 gp@54.172.40.4
            fields = line.split()
            try:
                pid = int(fields[0])
            except ValueError as e:
                _LOG.error("Can't parse fields '%s' from line '%s'", fields, line)
                raise e
            _LOG.debug("pid=%s", pid)
            pids.append(pid)
            txt_out.append(line)
    return pids, txt_out


def kill_process(get_pids, timeout_in_secs=5, polltime_in_secs=0.1):
    """
    Kill all the processes returned by the function `get_pids()`.

    :param timeout_in_secs: how many seconds to wait at most before giving up
    :param polltime_in_secs: how often to check for dead processes
    """
    pids, txt = get_pids()
    _LOG.info("Killing %d pids (%s)\n%s", len(pids), pids, "\n".join(txt))
    if not pids:
        return
    for pid in pids:
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError as e:
            _LOG.warning(str(e))
    #
    _LOG.info("Waiting %d processes (%s) to die", len(pids), pids)
    import tqdm

    for _ in tqdm.tqdm(range(int(timeout_in_secs / polltime_in_secs))):
        time.sleep(polltime_in_secs)
        pids, _ = get_pids()
        if not pids:
            break
    pids, txt = get_pids()
    dbg.dassert_eq(len(pids), 0, "Processes are still alive:%s", "\n".join(txt))
    _LOG.info("Processes dead")


# #############################################################################


def query_yes_no(question: str, abort_on_no: bool):
    """
    Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".
    """
    valid = {
        "yes": True,
        "y": True,
        #
        "no": False,
        "n": False,
    }
    prompt = " [y/n] "
    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if choice in valid:
            ret = valid[choice]
            break
    _LOG.debug("ret=%s", ret)
    if abort_on_no:
        if not ret:
            print("You answer no: exiting")
            sys.exit(-1)
    return ret


# #############################################################################

# TODO(gp): Move it helpers/tools_interaction.py ?


def pytest_show_artifacts(dir_name: str, tag: Optional[str] = None) -> List[str]:
    dbg.dassert_ne(dir_name, "")
    dbg.dassert_dir_exists(dir_name)
    cd_cmd = "cd %s && " % dir_name
    # There might be no pytest artifacts.
    abort_on_error = False
    file_names: List[str] = []
    # Find pytest artifacts.
    cmd = 'find . -name ".pytest_cache" -type d'
    _, output_tmp = system_to_string(cd_cmd + cmd, abort_on_error=abort_on_error)
    file_names.extend(output_tmp.split())
    #
    cmd = 'find . -name "__pycache__" -type d'
    _, output_tmp = system_to_string(cd_cmd + cmd, abort_on_error=abort_on_error)
    file_names.extend(output_tmp.split())
    # Find .pyc artifacts.
    cmd = 'find . -name "*.pyc" -type f'
    _, output_tmp = system_to_string(cd_cmd + cmd, abort_on_error=abort_on_error)
    file_names.extend(output_tmp.split())
    # Remove empty lines.
    file_names = prnt.remove_empty_lines_from_string_list(file_names)
    #
    if tag is not None:
        num_files = len(file_names)
        _LOG.info("%s: %d", tag, num_files)
        _LOG.debug("\n%s", prnt.space("\n".join(file_names)))
    return file_names


def pytest_clean_artifacts(dir_name: str, preview: bool = False):
    _LOG.warning("Cleaning pytest artifacts")
    dbg.dassert_ne(dir_name, "")
    dbg.dassert_dir_exists(dir_name)
    if preview:
        _LOG.warning("Preview only: nothing will be deleted")
    # Show before cleaning.
    file_names = pytest_show_artifacts(dir_name, tag="Before cleaning")
    # Clean.
    for f in file_names:
        exists = os.path.exists(f)
        _LOG.debug("%s -> exists=%s", f, exists)
        if exists:
            if not preview:
                if os.path.isdir(f):
                    shutil.rmtree(f)
                elif os.path.isfile(f):
                    os.remove(f)
                else:
                    raise ValueError("Can't delete %s" % f)
            else:
                _LOG.debug("rm %s", f)
    # Show after cleaning.
    file_names = pytest_show_artifacts(dir_name, tag="After cleaning")
    dbg.dassert_eq(len(file_names), 0)
