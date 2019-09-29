"""
Functions to handle filesystem operations.

Import as:

import helpers.io_ as io_
"""

import fnmatch
import logging
import os
import shutil
import time

import helpers.dbg as dbg
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# #############################################################################
# Glob.
# #############################################################################


def find_files(directory, pattern):
    """
    Recursive glob.

    :param pattern: pattern to match a filename against
    """
    file_names = []
    for root, _, files in os.walk(directory):
        for basename in files:
            if fnmatch.fnmatch(basename, pattern):
                file_name = os.path.join(root, basename)
                file_names.append(file_name)
    return file_names


def find_regex_files(src_dir, regex):
    cmd = 'find %s -name "%s"' % (src_dir, regex)
    _, output = si.system_to_string(cmd)
    file_names = [f for f in output.split("\n") if f != ""]
    _LOG.debug("Found %s files in %s", len(file_names), src_dir)
    _LOG.debug("\n".join(file_names))
    return file_names


# #############################################################################
# Filesystem.
# #############################################################################


def create_soft_link(src, dst):
    """
    Create a soft-link to <src> called <dst> (where <src> and <dst> are files
    or directories as in a Linux ln command). This is equivalent to a command
    like
    "cp <src> <dst>" but creating a soft link.
    """
    _LOG.debug("# CreateSoftLink")
    # Create the enclosing directory, if needed.
    enclosing_dir = os.path.dirname(dst)
    _LOG.debug("enclosing_dir=%s", enclosing_dir)
    create_dir(enclosing_dir, incremental=True)
    # Create the link. Note that the link source needs to be an absolute path.
    src = os.path.abspath(src)
    cmd = "ln -s %s %s" % (src, dst)
    si.system(cmd)


def delete_file(file_name):
    _LOG.debug("Deleting file '%s'", file_name)
    if not os.path.exists(file_name) or file_name == "/dev/null":
        # Nothing to delete.
        return
    try:
        os.unlink(file_name)
    except OSError as e:
        # It can happen that we try to delete the file, while somebody already
        # deleted it, so we neutralize the corresponding exception.
        if e.errno == 2:
            # OSError: [Errno 2] No such file or directory.
            pass
        else:
            raise e


def delete_dir(
    dir_,
    change_perms=False,
    errnum_to_retry_on=16,
    num_retries=1,
    num_secs_retry=1,
):
    """
    Delete a directory.
    - change_perms: change permissions to -R rwx before deleting to deal with
      incorrect permissions left over
    - errnum_to_retry_on: specify the error to retry on
      OSError: [Errno 16] Device or resource busy:
        'gridTmp/.nfs0000000002c8c10b00056e57'
    """
    _LOG.debug("Deleting dir '%s'", dir_)
    if not os.path.isdir(dir_):
        # No directory so nothing to do.
        return
    if change_perms and os.path.isdir(dir_):
        cmd = "chmod -R +rwx " + dir_
        si.system(cmd)
    i = 1
    while True:
        try:
            shutil.rmtree(dir_)
            # Command succeeded: exit.
            break
        except OSError as e:
            if errnum_to_retry_on is not None and e.errno == errnum_to_retry_on:
                # TODO(saggese): Make it less verbose once we know it's working
                # properly.
                _LOG.warning(
                    "Couldn't delete %s: attempt=%s / %s", dir_, i, num_retries
                )
                i += 1
                if i > num_retries:
                    dbg.dfatal(
                        "Couldn't delete %s after %s attempts (%s)"
                        % (dir_, num_retries, dbg.get_exception_as_string())
                    )
                else:
                    time.sleep(num_secs_retry)
            else:
                # Unforeseen error: just propagate it.
                raise e


def create_dir(dir_name, incremental, abort_if_exists=False, ask_to_delete=False):
    """
    Create a directory `dir_name` if it doesn't exist.

    - param incremental: if False then the directory is deleted and
        re-created, otherwise it skips
    - param abort_if_exists:
    - param ask_to_delete: if it is not incremental and the dir exists,
        asks before deleting
    """
    dbg.dassert_is_not(dir_name, None)
    dbg.dassert(
        os.path.normpath(dir_name) != ".", msg="Can't create the current dir"
    )
    if abort_if_exists:
        dbg.dassert_not_exists(dir_name)
    #                   dir exists / dir does not exist
    # incremental       no-op        mkdir
    # not incremental   rm+mkdir     mkdir
    if os.path.isdir(dir_name):
        if incremental:
            # The dir exists and we want to keep it it exists (i.e.,
            # incremental), so we are done.
            # os.chmod(dir_name, 0755)
            return
        else:
            if ask_to_delete:
                si.query_yes_no(
                    "Do you really want to delete dir '%s'?" % dir_name,
                    abort_on_no=True,
                )
            # The dir exists and we want to create it from scratch (i.e., not
            # incremental), so we need to delete the dir.
            _LOG.debug("Deleting dir '%s'", dir_name)
            if os.path.islink(dir_name):
                delete_file(dir_name)
            else:
                shutil.rmtree(dir_name)
    _LOG.debug("Creating directory '%s'", dir_name)
    # Note that makedirs raises OSError if the target directory already exists.
    # A race condition can happen when another process creates our target
    # directory, while we have just found that it doesn't exist, so we need to
    # handle this situation gracefully.
    try:
        os.makedirs(dir_name)
    except OSError as e:
        # It can happen that we try to create the directory while somebody else
        # created it, so we neutralize the corresponding exception.
        if e.errno == 17:
            # OSError: [Errno 17] File exists.
            pass
        else:
            raise e


# TODO(gp): Shouldn't be always incremental=False?
def create_enclosing_dir(file_name, incremental=False):
    """
    Create the dir enclosing file_name, if needed. <incremental> has the same
    meaning as in create_dir().
    """
    dbg.dassert_is_not(file_name, None)
    # dbg.dassert_isinstance(file_name, str)
    dir_name = os.path.dirname(file_name)
    if dir_name != "" and not os.path.isdir(dir_name):
        create_dir(dir_name, incremental)
    return dir_name


# #############################################################################
# File.
# #############################################################################


# TODO(saggese): We should have lines first since it is an input param.
def to_file(file_name, lines, mode="w", force_flush=False):
    """
    Write the content of lines into file_name, creating the enclosing directory
    if needed.
    """
    # TODO(gp): create_enclosing_dir().
    dbg.dassert_is_not(file_name, None)
    # dbg.dassert_in(type(file_name), (str, unicode))
    # Create the enclosing dir, if needed.
    dir_name = os.path.dirname(file_name)
    if dir_name != "" and not os.path.isdir(dir_name):
        create_dir(dir_name, incremental=True)
    with open(file_name, mode, buffering=0 if mode == "a" else -1) as f:
        f.writelines(lines)
        if force_flush:
            f.flush()
            os.fsync(f.fileno())


# TODO(saggese): Remove the split param.
def from_file(file_name, split=True, encoding=None):
    dbg.dassert_ne(file_name, "")
    with open(file_name, "r", encoding=encoding) as f:
        try:
            data = f.read()
        except UnicodeDecodeError as e:
            msg = "error=%s" % e
            msg = "\nfile_name='%s'" % file_name
            _LOG.error(msg)
            raise RuntimeError(msg)
        dbg.dassert_isinstance(data, str)
        if split:
            data = data.split("\n")
    return data


def get_size_as_str(file_name):
    if os.path.exists(file_name):
        size_in_bytes = os.path.getsize(file_name)
        if size_in_bytes < (1024 ** 2):
            size_in_kb = size_in_bytes / 1024.0
            res = "%.1f KB" % size_in_kb
        if size_in_bytes < (1024 ** 3):
            size_in_mb = size_in_bytes / (1024.0 ** 2)
            res = "%.1f MB" % size_in_mb
        else:
            size_in_gb = size_in_bytes / (1024.0 ** 3)
            res = "%.1f GB" % size_in_gb
    else:
        res = "nan"
    return res
