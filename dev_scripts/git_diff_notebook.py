#!/usr/bin/env python
"""
Diff a notebook against the HEAD version in git, removing notebook artifacts
to make the differences easier to spot using vimdiff.
"""

# TODO(gp)
# - It should have switches to decide what to show:
#   - all changes using the appropriate diff (e.g., notebook vs git difftool)
#   - only and all python code (replacing git_diff_code.sh)
#   - only and all notebooks
# - Use a single tmp dir for all notebooks reusing the same basename for
#   clarity.
# - Add switch to run linter on both py files.

import argparse
import logging
import os

import helpers.dbg as dbg
import helpers.git as git
import helpers.helper_io as io
import helpers.printing as printing
import helpers.system_interaction as hsi

_log = logging


# TODO(gp): Use hsi.system.
def _system(cmd):
    _log.debug("> %s", cmd)
    hsi.system(cmd)


def _convert(dir_name, ipynb_file, py_file):
    """
    Convert jupyter notebook into a python file suitable for diff-ing.
    :param dir_name: destination directory
    :param ipynb_file: absolute path of src file name
    :param py_file: basename of the dst python file (e.g., "notebook_new.py")
    :return: path of dst file
    """
    _log.debug("dir_name=%s ipynb_file=%s py_file=%s", dir_name, ipynb_file,
               py_file)
    # TODO(gp): Use dir_name for --output-dir.
    dbg.dassert_exists(ipynb_file)
    cmd = "jupyter nbconvert %s --to python --output %s >/dev/null" % (
        ipynb_file, py_file)
    hsi.system(cmd)
    # Purify output removing the [\d+].
    dir_name = os.path.dirname(ipynb_file)
    dst_py_file = dir_name + "/" + py_file
    dbg.dassert_exists(dst_py_file)
    cmd = r"perl -p -i -e 's/# In\s*\[.*]/# In[]/g' %s" % dst_py_file
    _system(cmd)
    return dst_py_file


def _diff_notebook(dir_name, abs_file_name, git_client_root, brief):
    """
    Diff notebook against the HEAD git version.

    :param dir_name: directory to use as tmp dir
    :param abs_file_name: absolute path of the file
    :param git_client_root: path of git client
    :return:
    """
    _log.debug("dir_name=%s abs_file_name=%s", dir_name, abs_file_name)
    # Make sure the file exists and it's a python notebook.
    dbg.dassert_exists(abs_file_name)
    # Retrieve HEAD file and save it.
    old_ipynb = dir_name + "/notebook_old.ipynb"
    # Convert abs file into file relative to git root:
    # E.g.,
    #   agriculture/notebooks/data_explorations/ThomsonReuters_db.ipynb
    # into
    #   altdata/python/src/agriculture/notebooks/data_explorations/
    #       ThomsonReuters_db.ipynb
    git_file_name = abs_file_name.replace(git_client_root, "")[1:]
    _log.info("git_file_name=%s", git_file_name)
    cmd = "git show HEAD:%s >%s" % (git_file_name, old_ipynb)
    hsi.system(cmd)
    dbg.dassert_exists(old_ipynb)
    #
    old_py = "notebook_old.py"
    old_py = _convert(dir_name, old_ipynb, old_py)
    #
    new_py = "notebook_new.py"
    new_py = _convert(dir_name, abs_file_name, new_py)
    #
    for f in (old_py, new_py):
        dbg.dassert(os.path.exists(f), msg="Can't find %s" % f)
    is_ipynb_diff = None
    if brief:
        cmd = "diff --brief %s %s" % (old_py, new_py)
        # Do not break on error, but return the error code.
        rc = hsi.system(cmd, abort_on_error=False)
        is_ipynb_diff = rc != 0
        if is_ipynb_diff:
            _log.warning("Notebooks %s are different", abs_file_name)
        else:
            _log.info("Notebooks %s are equal", abs_file_name)
    else:
        cmd = "vimdiff %s %s" % (old_py, new_py)
        _log.debug(">> %s", cmd)
        # Do not redirect to file when using vimdiff.
        os.system(cmd)
    # Clean up.
    cmd = "rm %s %s" % (old_py, new_py)
    _system(cmd)
    return is_ipynb_diff


def _get_files(args):
    # Get the files.
    file_names = args.files
    if not file_names:
        if args.current_git_files:
            file_names = git.get_modified_files()
        elif args.previous_git_commit_files:
            file_names = git.get_previous_committed_files()
    _log.debug("file_names=%s", file_names)
    if not file_names:
        msg = "No files were selected"
        _log.error(msg)
        raise ValueError(msg)
    return file_names


def _main(args):
    dbg.init_logger2(args.log_level)
    # Get the files.
    file_names = _get_files(args)
    # Select the ipynb files.
    file_names_tmp = file_names[:]
    file_names = []
    for f in file_names_tmp:
        if os.path.splitext(f)[1] == ".ipynb":
            file_names.append(f)
        else:
            _log.warning("File '%s' is not a jupyter notebook: skipping", f)
    dbg.dassert_lte(1, len(file_names))
    #
    # TODO(gp): Use get_path_from_git_root().
    git_client_root = git.get_client_root()
    _log.info("git_client_root=%s", git_client_root)
    client_root = os.getcwd()
    _log.info("client_root=%s", client_root)
    #
    file_names_tmp = file_names[:]
    file_names = []
    for f in file_names_tmp:
        _log.debug("f=%s", f)
        # pylint: disable=C0301
        # Make files wrt git client become absolute.
        # E.g.,
        #   altdata/python/src/agriculture/notebooks/data_explorations/
        #       ThomsonReuters_db.ipynb
        # to:
        #   /Users/gp/src/mac_tcm2/altdata/python/src/agriculture/
        #       notebooks/data_explorations/ThomsonReuters_db.ipynb
        abs_file_name = git_client_root + "/" + f
        dbg.dassert_exists(abs_file_name)
        # pylint: disable=C0301
        # Remove the cwd path to get the path relative to the current dir.
        # E.g.,
        #   /Users/gp/src/mac_tcm2/altdata/python/src/agriculture/
        #       notebooks/data_explorations/ThomsonReuters_db.ipynb
        # to:
        #   agriculture/notebooks/data_explorations/ThomsonReuters_db.ipynb
        dbg.dassert_ne(abs_file_name.find(client_root), -1)
        cwd_file_name = abs_file_name.replace(client_root + "/", "")
        file_names.append((abs_file_name, cwd_file_name))
    _log.info(
        "file_names=%s\n%s", len(file_names), "\n".join(
            ["%s -> %s" % (cwd_f, abs_f) for (abs_f, cwd_f) in file_names]))
    dbg.dassert_lte(1, len(file_names))
    # Create tmp dir.
    dir_name = os.path.abspath("./tmp.git_diff_notebook")
    io.create_dir(dir_name, incremental=False)
    notebooks_diff = []
    notebooks_equal = []
    # Diff the files.
    for abs_file_name, cwd_file_name in file_names:
        print(("\n" + printing.frame("file_name=%s" % cwd_file_name).rstrip("\n")))
        is_ipynb_diff = _diff_notebook(dir_name, abs_file_name, git_client_root,
                                       args.brief)
        if args.brief:
            if is_ipynb_diff:
                notebooks_diff.append(cwd_file_name)
            else:
                notebooks_equal.append(cwd_file_name)
    #
    if args.brief:
        print(("\nDifferent notebooks are: (%s) %s" % (len(notebooks_diff),
                                                      " ".join(notebooks_diff))))
        print(("\nEqual notebooks are: (%s) %s" % (len(notebooks_equal),
                                                  " ".join(notebooks_equal))))


def _parse():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("-f", "--files", nargs='+', help="Files to process")
    parser.add_argument(
        '-c',
        '--current_git_files',
        action="store_true",
        help="Select all files modified in the current git client")
    parser.add_argument(
        '-p',
        '--previous_git_commit_files',
        action="store_true",
        help="Select all files modified in previous user git commit")
    parser.add_argument(
        '-b',
        '--brief',
        action="store_true",
        help="Just report if a notebook is changed or not")
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help="Set the logging level")
    args = parser.parse_args()
    _main(args)


if __name__ == "__main__":
    _parse()
