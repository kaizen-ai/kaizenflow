#!/usr/bin/env python

import logging
import os
import subprocess
import sys
import time

import helpers.dbg as dbg
import helpers.printing as pri

_LOG = logging.getLogger(__name__)


# pylint: disable=R0912, R0913, R0914, R0915
# [R0912(too-many-branches), _system] Too many branches
# [R0915(too-many-statements), _system] Too many statements
# [R0913(too-many-arguments), _system] Too many arguments (10/5) [pylint]
# [R0914(too-many-locals), _system] Too many local variables (21/15) [pylint]
def _system(
    cmd,
    abort_on_error,
    suppress_error,
    suppress_output,
    blocking,
    wrapper,
    output_file,
    tee,
    dry_run,
    log_level,
):
    """
    Execute a shell command.

    :param cmd: string with command to execute
    :param abort_on_error: whether we should assert in case of error or not
    :param suppress_error: set of error codes to suppress
    :param suppress_output: whether to print the output or not
    :param blocking: blocking system call or not
    :param wrapper: another command to prepend the execution of cmd
    :param output_file: redirect stdout and stderr to this file
    :param tee: if True, tee stdout and stderr to output_file
    :param dry_run: just print the final command but not execute it
    :param log_level: print the command to execute at level "log_level". If
        it is equal to "echo" then just print to screen.
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
    if log_level == "echo":
        print("> %s" % orig_cmd)
        _LOG.debug(log_level, "> %s", cmd)
    else:
        _LOG.log(log_level, "> %s", cmd)
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
            + pri.frame("cmd='%s' failed with rc='%s'" % (cmd, rc))
            + "\nOutput of the failing command is:\n%s\n%s\n%s"
            % (pri.line(">"), output, pri.line("<"))
        )
        _LOG.error("%s", msg)
        raise RuntimeError("cmd='%s' failed with rc='%s'" % (cmd, rc))
    # dbg.dassert_type_in(output, (str, ))
    return rc, output


# pylint: disable=R0913
# [R0913(too-many-arguments), system] Too many arguments (10/5) [pylint]
def system(
    cmd,
    abort_on_error=True,
    suppressed_error=None,
    suppress_output=True,
    blocking=True,
    wrapper=None,
    output_file=None,
    tee=False,
    dry_run=False,
    log_level=logging.DEBUG,
):
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
    cmd, abort_on_error=True, wrapper=None, dry_run=False, log_level=logging.DEBUG
):
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


# ##############################################################################


# TODO(gp): Maybe move to helpers.env or merge helpers.env back here?
def get_user_name():
    import getpass

    res = getpass.getuser()
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


# ##############################################################################


def get_process_pids(keep_line):
    """
    Find all the processes corresponding to `ps ax` filtered line by line with
    `keep_line()`.
    """
    cmd = "ps ax"
    rc, txt = system_to_string(cmd, abort_on_error=False)
    _LOG.debug("txt=\n%s", txt)
    pids = []
    if rc == 0:
        for line in txt.split("\n"):
            _LOG.debug("line=%s", line)
            # PID   TT  STAT      TIME COMMAND
            if "PID" in line and "TT" in line and "STAT" in line:
                continue
            # > ps ax | grep 'ssh -i' | grep localhost
            # 19417   ??  Ss     0:00.39 ssh -i /Users/gp/.ssh/id_rsa -f -nNT \
            #           -L 19999:localhost:19999 gp@54.172.40.4
            keep = keep_line(line)
            _LOG.debug("  keep=%s", keep)
            if not keep:
                continue
            fields = line.split()
            try:
                pid = int(fields[0])
            except ValueError as e:
                _LOG.error("Can't parse fields '%s' from line '%s'", fields, line)
                raise e
            _LOG.debug("pid=%s", pid)
            pids.append(pid)
    return pids, txt


# ##############################################################################


def query_yes_no(question, abort_on_no=True):
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
