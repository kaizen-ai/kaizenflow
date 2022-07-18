"""
Import as:

import helpers.lib_tasks_integrate as hlitaint
"""

import datetime
import logging
import os
from typing import List, Optional, Set, Tuple

from invoke import task

# We want to minimize the dependencies from non-standard Python packages since
# this code needs to run with minimal dependencies and without Docker.
import helpers.hdbg as hdbg
import helpers.hgit as hgit
import helpers.hio as hio
import helpers.hprint as hprint
import helpers.hsystem as hsystem
import helpers.lib_tasks_utils as hlitauti

_LOG = logging.getLogger(__name__)

# pylint: disable=protected-access

# #############################################################################
# Integrate.
# #############################################################################

# pylint: disable=line-too-long

# ## Concepts
#
# - We have two dirs storing two forks of the same repo
#   - Files are touched, e.g., added, modified, deleted in each forks
#   - The most problematic files are the files that are modified in both forks
#   - Files that are added or deleted in one fork, should be added / deleted also
#     in the other fork
# - Often we can integrate "by directory", i.e., finding entire directories that
#   we were touched in one branch but not the other
#   - In this case we can simply copy the entire dir from one dir to the other
# - Other times we need to integrate "by file"
#
# - There are various interesting Git reference points:
#   1) the branch point for each branch, at which the integration branch was started
#   2) the last integration point for each branch, at which the repos are the same,
#      or at least aligned

# ## Create integration branches
#
# - Pull master
#
# - Align `lib_tasks.py`:
#   ```
#   > vimdiff ~/src/{amp1,cmamp1}/tasks.py; vimdiff ~/src/{amp1,cmamp1}/helpers/lib_tasks.py
#   ```
#
# - Create the integration branches
#   ```
#   > cd amp1
#   > i integrate_create_branch --dir-basename amp1
#   > cd cmamp1
#   > i integrate_create_branch --dir-basename cmamp1
#   ```

# ## Preparation
#
# - Lint both dirs:
#   ```
#   > cd amp1
#   > i lint --dir-name . --only-format
#   > cd cmamp1
#   > i lint --dir-name . --only-format
#   ```
#   or at least the files touched by both repos:
#   ```
#   > i integrate_files --file-direction only_files_in_src
#   > cat tmp.integrate_find_files_touched_since_last_integration.cmamp1.txt tmp.integrate_find_files_touched_since_last_integration.amp1.txt | sort | uniq >files.txt
#   > FILES=$(cat files.txt)
#   > i lint --only-format -f "$FILES"
#   ```
#
# - Add end-of-file:
#   ```
#   > find . -name "*.py" -o -name "*.txt" -o -name "*.json" | xargs sed -i '' -e '$a\'
#
#   # Remove end-of-file.
#   > find . -name "*.txt" | xargs perl -pi -e 'chomp if eof'
#   ```

# ## Integration
#
# - Check what files were modified since the last integration in each fork:
#   ```
#   > i integrate_files --file-direction common_files
#   > i integrate_files --file-direction only_files_in_src
#   > i integrate_files --file-direction only_files_in_dst
#   ```
#
# - Look for directory touched on only one branch:
#   ```
#   > i integrate_files --file-direction common_files --mode "print_dirs"
#   > i integrate_files --file-direction only_files_in_src --mode "print_dirs"
#   > i integrate_files --file-direction only_files_in_dst --mode "print_dirs"
#   ```
# - If we find dirs that are touched in one branch but not in the other
#   we can copy / merge without running risks
#   ```
#   > i integrate_diff_dirs --subdir $SUBDIR -c
#   ```
#
# - Check which files are different between the dirs:
#   ```
#   > i integrate_diff_dirs
#   ```
#
# - Diff dir by dir
#   ```
#   > i integrate_diff_dirs --subdir dataflow/system
#   ```
#
# - Copy by dir
#   ```
#   > i integrate_diff_dirs --subdir market_data -c
#   ```
#
# - Remove the empty files
#   ```
#   > find . -type f -empty -print | grep -v .git | grep -v __init__ | grep -v ".log$" | grep -v ".txt$" | xargs git rm
#   ```
#
# - Copy a dir
#   ```
#   > rsync --delete -a -r /Users/saggese/src/cmamp1/research_amp/ /Users/saggese/src/amp1/research_amp
#   ```

# ## Double check the integration
#
# - Check that the regressions are passing on GH
#   ```
#   > i gh_create_pr --no-draft
#   ```
#
# - Check the files that were changed in both branches (i.e., the "problematic ones")
#   since the last integration and compare them to the base in each branch
#   ```
#   > cd amp1
#   > i integrate_diff_overlapping_files --src-dir-basename "amp1" --dst-dir-basename "cmamp1"
#   > cd cmamp1
#   > i integrate_diff_overlapping_files --src-dir-basename "cmamp1" --dst-dir-basename "amp1"
#   ```
#
# - Quickly scan all the changes in the branch compared to the base
#   ```
#   > cd amp1
#   > i git_branch_diff_with_base
#   > cd cmamp1
#   > i git_branch_diff_with_base
#   ```


# Invariants for the integration set-up
#
# - The user runs commands in a abs_dir, e.g., `/Users/saggese/src/{amp1,cmamp1}`
# - The user refers in the command line to `dir_basename`, which is the basename of
#   the integration directories (e.g., `amp1`, `cmamp1`)
#   - The "src_dir_basename" is the one where the command is issued
#   - The "dst_dir_basename" is assumed to be parallel to the "src_dir_basename"
# - The dirs are then transformed in absolute dirs "abs_src_dir"


def _dassert_current_dir_matches(expected_dir_basename: str) -> None:
    """
    Ensure that the name of the current dir is the one expected.

    E.g., `/Users/saggese/src/cmamp1` is a valid dir for an integration
    branch for `cmamp1`.
    """
    _LOG.debug(hprint.to_str("expected_dir_basename"))
    # Get the basename of the current dir.
    curr_dir_basename = os.path.basename(os.getcwd())
    # Check that it's what is expected.
    hdbg.dassert_eq(
        curr_dir_basename,
        expected_dir_basename,
        "The current dir '%s' doesn't match the expected dir '%s'",
        curr_dir_basename,
        expected_dir_basename,
    )


# TODO(gp): -> _dassert_is_integration_dir
def _dassert_is_integration_branch(abs_dir: str) -> None:
    """
    Ensure that the branch in `abs_dir` is a valid integration or lint branch.

    E.g., `AmpTask1786_Integrate_20220402` is a valid integration
    branch.
    """
    _LOG.debug(hprint.to_str("abs_dir"))
    branch_name = hgit.get_branch_name(dir_name=abs_dir)
    hdbg.dassert_ne(branch_name, "master")
    hdbg.dassert(
        ("_Integrate_" in branch_name) or ("_Lint_" in branch_name),
        "Invalid branch_name='%s' in abs_dir='%s'",
        branch_name,
        abs_dir,
    )


def _clean_both_integration_dirs(abs_dir1: str, abs_dir2: str) -> None:
    """
    Run `i git_clean` on the passed dirs.

    :param abs_dir1, abs_dir2: full paths of the dirs to clean
    """
    _LOG.debug(hprint.to_str("abs_dir1 abs_dir2"))
    #
    cmd = f"cd {abs_dir1} && invoke git_clean"
    hsystem.system(cmd)
    #
    cmd = f"cd {abs_dir2} && invoke git_clean"
    hsystem.system(cmd)


@task
def integrate_create_branch(ctx, dir_basename, dry_run=False):  # type: ignore
    """
    Create the branch for integration of `dir_basename` (e.g., amp1) in the
    current dir.

    :param dir_basename: specify the dir name (e.g., `amp1`) to ensure the set-up is
        correct.
    """
    hlitauti._report_task()
    # Check that the current dir has the name `dir_basename`.
    _dassert_current_dir_matches(dir_basename)
    # Create the integration branch with the current date, e.g.,
    # `AmpTask1786_Integrate_20211231`.
    date = datetime.datetime.now().date()
    date_as_str = date.strftime("%Y%m%d")
    branch_name = f"AmpTask1786_Integrate_{date_as_str}"
    # query_yes_no("Are you sure you want to create the branch ")
    _LOG.info("Creating branch '%s'", branch_name)
    cmd = f"invoke git_create_branch -b '{branch_name}'"
    hlitauti._run(ctx, cmd, dry_run=dry_run)


# //////////////////////////////////////////////////////////////////////////////


def _resolve_src_dst_names(
    src_dir_basename: str, dst_dir_basename: str, subdir: str
) -> Tuple[str, str]:
    """
    Return the full path of `src_dir_basename` and `dst_dir_basename`.

    :param src_dir_basename: the current dir (e.g., `amp1`)
    :param dst_dir_basename: a dir parallel to the current one (`cmamp1`)

    :return: absolute paths of both directories
    """
    curr_parent_dir = os.path.dirname(os.getcwd())
    #
    abs_src_dir = os.path.join(curr_parent_dir, src_dir_basename, subdir)
    abs_src_dir = os.path.normpath(abs_src_dir)
    hdbg.dassert_dir_exists(abs_src_dir)
    #
    abs_dst_dir = os.path.join(curr_parent_dir, dst_dir_basename, subdir)
    abs_dst_dir = os.path.normpath(abs_dst_dir)
    hdbg.dassert_dir_exists(abs_dst_dir)
    return abs_src_dir, abs_dst_dir


@task
def integrate_diff_dirs(  # type: ignore
    ctx,
    src_dir_basename="amp1",
    dst_dir_basename="cmamp1",
    reverse=False,
    subdir="",
    copy=False,
    use_linux_diff=False,
    check_branches=True,
    clean_branches=True,
    remove_usual=False,
    dry_run=False,
):
    """
    Integrate repos from dirs `src_dir_basename` to `dst_dir_basename` by diffing
    or copying all the files with differences.

    ```
    # Use the default values for src / dst dirs to represent the usual set-up.
    > i integrate_diff_dirs \
        --src-dir-basename amp1 \
        --dst-dir-basename cmamp1 \
        --subdir .
    ```

    :param src_dir_basename: dir with the source branch (e.g., amp1)
    :param dst_dir_basename: dir with the destination branch (e.g., cmamp1)
    :param reverse: switch the roles of the default source and destination branches
    :param subdir: filter to the given subdir for both dirs (e.g.,
        `src_dir_basename/subdir` and `dst_dir_basename/subdir`)
    :param copy: copy the files instead of diffing
    :param use_linux_diff: use Linux `diff` instead of `diff_to_vimdiff.py`
    :param remove_usual: remove the usual mismatching files (e.g., `.github`)
    """
    hlitauti._report_task()
    if reverse:
        src_dir_basename, dst_dir_basename = dst_dir_basename, src_dir_basename
        _LOG.warning(
            "Reversing dirs: %s",
            hprint.to_str2(src_dir_basename, dst_dir_basename),
        )
    # Check that the integration branches are in the expected state.
    # _dassert_current_dir_matches(src_dir_basename)
    abs_src_dir, abs_dst_dir = _resolve_src_dst_names(
        src_dir_basename, dst_dir_basename, subdir
    )
    if check_branches:
        _dassert_is_integration_branch(abs_src_dir)
        _dassert_is_integration_branch(abs_dst_dir)
    else:
        _LOG.warning("Skipping integration branch check")
    # Clean branches if needed.
    if clean_branches:
        # We can clean up only the root dir.
        if subdir == "":
            _clean_both_integration_dirs(abs_src_dir, abs_dst_dir)
    else:
        _LOG.warning("Skipping integration branch cleaning")
    # Copy or diff dirs.
    _LOG.info("abs_src_dir=%s", abs_src_dir)
    _LOG.info("abs_dst_dir=%s", abs_dst_dir)
    hdbg.dassert_ne(abs_src_dir, abs_dst_dir)
    if copy:
        # Copy the files.
        if dry_run:
            cmd = f"diff -r --brief {abs_src_dir} {abs_dst_dir}"
        else:
            rsync_opts = "--delete -a"
            cmd = f"rsync {rsync_opts} {abs_src_dir}/ {abs_dst_dir}"
    else:
        # Diff the files.
        if use_linux_diff:
            cmd = f"diff -r --brief {abs_src_dir} {abs_dst_dir}"
        else:
            cmd = f"dev_scripts/diff_to_vimdiff.py --dir1 {abs_src_dir} --dir2 {abs_dst_dir}"
            if remove_usual:
                vals = [
                    r"\/\.github\/",
                ]
                regex = "|".join(vals)
                cmd += f" --ignore_files='{regex}'"
    hlitauti._run(ctx, cmd, dry_run=dry_run, print_cmd=True)


# //////////////////////////////////////////////////////////////////////////////


def _find_files_touched_since_last_integration(
    abs_dir: str, subdir: str
) -> List[str]:
    """
    Return the list of files modified since the last integration for `abs_dir`.

    :param abs_dir: directory to cd before executing this script
    :param subdir: consider only the files under `subdir`
    """
    _LOG.debug(hprint.to_str2(abs_dir))
    dir_basename = os.path.basename(abs_dir)
    # TODO(gp): dir_basename can be computed from abs_dir_name to simplify the
    #  interface.
    # Change the dir to the correct one.
    old_dir = os.getcwd()
    try:
        os.chdir(abs_dir)
        # Find the hash of all integration commits.
        cmd = "git log --date=local --oneline --date-order | grep AmpTask1786_Integrate"
        # Remove integrations like "'... Merge branch 'master' into AmpTask1786_Integrate_20220113'"
        cmd += " | grep -v \"Merge branch 'master' into \""
        _, txt = hsystem.system_to_string(cmd)
        _LOG.debug("integration commits=\n%s", txt)
        txt = txt.split("\n")
        # > git log --date=local --oneline --date-order | grep AmpTask1786_Integrate
        # 72a1a101 AmpTask1786_Integrate_20211218 (#1975)
        # 2acfd6d7 AmpTask1786_Integrate_20211214 (#1950)
        # 318ab0ff AmpTask1786_Integrate_20211210 (#1933)
        hdbg.dassert_lte(2, len(txt))
        print(f"# last_integration: '{txt[0]}'")
        last_integration_hash = txt[0].split()[0]
        print("* " + hprint.to_str("last_integration_hash"))
        # Find the first commit after the commit with the last integration.
        cmd = f"git log --oneline --reverse --ancestry-path {last_integration_hash}^..master"
        _, txt = hsystem.system_to_string(cmd)
        print(f"* commits after last integration=\n{txt}")
        txt = txt.split("\n")
        # > git log --oneline --reverse --ancestry-path 72a1a101^..master
        # 72a1a101 AmpTask1786_Integrate_20211218 (#1975)
        # 90e90353 AmpTask1955_Lint_20211218 (#1976)
        # 4a2b45c6 AmpTask1858_Implement_buildmeister_workflows_in_invoke (#1860)
        hdbg.dassert_lte(2, len(txt))
        first_commit_hash = txt[1].split()[0]
        _LOG.debug("first_commit: '%s'", txt[1])
        _LOG.debug(hprint.to_str("first_commit_hash"))
        # Find all the files touched in each branch.
        cmd = f"git diff --name-only {first_commit_hash}..HEAD"
        _, txt = hsystem.system_to_string(cmd)
        files: List[str] = txt.split("\n")
    finally:
        os.chdir(old_dir)
    _LOG.debug("Files modified since the integration=\n%s", "\n".join(files))
    # Filter files by subdir, if needed.
    if subdir:
        filtered_files = []
        for file in files:
            if file.startswith(subdir):
                filtered_files.append(file)
        files = filtered_files
    # Reorganize the files.
    hdbg.dassert_no_duplicates(files)
    files = sorted(files)
    # Save to file for debugging.
    file_name = os.path.join(
        f"tmp.integrate_find_files_touched_since_last_integration.{dir_basename}.txt"
    )
    hio.to_file(file_name, "\n".join(files))
    _LOG.debug("Saved file to '%s'", file_name)
    return files


@task
def integrate_find_files_touched_since_last_integration(  # type: ignore
    ctx,
    subdir="",
):
    """
    Print the list of files modified since the last integration for this dir.
    """
    hlitauti._report_task()
    abs_dir = os.getcwd()
    _ = ctx
    files = _find_files_touched_since_last_integration(abs_dir, subdir)
    # Print the result.
    tag = "Files modified since the integration"
    print(hprint.frame(tag))
    print("\n".join(files))


# //////////////////////////////////////////////////////////////////////////////


def _integrate_files(
    files: Set[str],
    abs_left_dir: str,
    abs_right_dir: str,
    only_different_files: bool,
) -> List[Tuple[str, str, str]]:
    """
    Build a list of files to compare based on the pattern.

    :param files: relative path of the files to compare
    :param abs_left_dir, abs_right_dir: path of the left / right dir
    :param only_different_files: include in the script only the files that are
        different
    :return: list of files to compare
    """
    _LOG.debug(hprint.to_str("abs_left_dir abs_right_dir only_different_files"))
    files_to_diff: List[Tuple[str, str, str]] = []
    for file in sorted(list(files)):
        _LOG.debug(hprint.to_str("file"))
        left_file = os.path.join(abs_left_dir, file)
        right_file = os.path.join(abs_right_dir, file)
        # Check if both the files exist and are the same.
        both_exist = os.path.exists(left_file) and os.path.exists(right_file)
        if not both_exist:
            # Both files don't exist: nothing to do.
            equal: Optional[bool] = False
            skip: Optional[bool] = True
        else:
            # They both exist.
            if only_different_files:
                # We want to check if they are the same.
                equal = hio.from_file(left_file) == hio.from_file(right_file)
                skip = equal
            else:
                # They both exists and we want to process even if they are the same.
                equal = None
                skip = False
        _ = left_file, right_file, both_exist, equal, skip
        _LOG.debug(hprint.to_str("left_file right_file both_exist equal skip"))
        # Execute the action on the 2 files.
        if skip:
            _LOG.debug("  Skip %s", file)
        else:
            _LOG.debug("  -> (%s, %s)", left_file, right_file)
            files_to_diff.append((file, left_file, right_file))
    return files_to_diff


@task
def integrate_files(  # type: ignore
    ctx,
    src_dir_basename="amp1",
    dst_dir_basename="cmamp1",
    reverse=False,
    subdir="",
    mode="vimdiff",
    file_direction="",
    only_different_files=True,
    check_branches=True,
):
    """
    Find and copy the files that are touched only in one branch or in both.

    :param src_dir_basename: dir with the source branch (e.g., amp1)
    :param dst_dir_basename: dir with the destination branch (e.g., cmamp1)
    :param reverse: switch the roles of the default source and destination branches
    :param mode:
        - "print_dirs": print the directories
        - "vimdiff": diff the files
        - "copy": copy the files
    :param file_direction: which files to diff / copy:
        - "common_files": files touched in both branches
        - "union_files": files touched in either branch
        - "only_files_in_src": files touched only in the src dir
        - "only_files_in_dst": files touched only in the dst dir
    :param only_different_files: consider only the files that are different among
        the branches
    """
    hlitauti._report_task()
    _ = ctx
    if reverse:
        src_dir_basename, dst_dir_basename = dst_dir_basename, src_dir_basename
        _LOG.warning(
            "Reversing dirs: %s",
            hprint.to_str2(src_dir_basename, dst_dir_basename),
        )
    # Check that the integration branches are in the expected state.
    _dassert_current_dir_matches(src_dir_basename)
    # We want to stay at the top level dir, since the subdir is handled by
    # `integrate_find_files_touched_since_last_integration`.
    abs_src_dir, abs_dst_dir = _resolve_src_dst_names(
        src_dir_basename, dst_dir_basename, subdir=""
    )
    if check_branches:
        _dassert_is_integration_branch(abs_src_dir)
        _dassert_is_integration_branch(abs_dst_dir)
    else:
        _LOG.warning("Skipping integration branch check")
    # Find the files touched in each branch since the last integration.
    src_files = set(
        _find_files_touched_since_last_integration(abs_src_dir, subdir)
    )
    dst_files = set(
        _find_files_touched_since_last_integration(abs_dst_dir, subdir)
    )
    #
    if file_direction == "common_files":
        files = src_files.intersection(dst_files)
    elif file_direction == "only_files_in_src":
        files = src_files - dst_files
    elif file_direction == "only_files_in_dst":
        files = dst_files - src_files
    elif file_direction == "union_files":
        files = src_files.union(dst_files)
    else:
        raise ValueError(f"Invalid file_direction='{file_direction}'")
    #
    files_to_diff = _integrate_files(
        files,
        abs_src_dir,
        abs_dst_dir,
        only_different_files,
    )
    # Print the files.
    print(hprint.frame(file_direction))
    _LOG.debug(hprint.to_str("files_to_diff"))
    files_set = list(zip(*files_to_diff))
    if not files_set:
        _LOG.warning("No file found: skipping")
        return
    files_set = sorted(list(files_set[0]))
    txt = "\n".join(files_set)
    print(hprint.indent(txt))
    # Process the files touched.
    if mode == "print_dirs":
        files_lst = []
        for file, left_file, right_file in files_to_diff:
            dir_name = os.path.dirname(file)
            # Skip empty dir, e.g., for `pytest.ini`.
            if dir_name != "":
                files_lst.append(dir_name)
        files_lst = sorted(list(set(files_lst)))
        print(hprint.frame("Dirs changed"))
        print("\n".join(files_lst))
    else:
        # Build the script with the operations to perform.
        if mode == "copy" and file_direction == "only_files_in_dst":
            raise ValueError("Can't copy files from destination")
        script_txt = []
        for file, left_file, right_file in files_to_diff:
            if mode == "copy":
                cmd = f"cp -f {left_file} {right_file}"
            elif mode == "vimdiff":
                cmd = f"vimdiff {left_file} {right_file}"
            else:
                raise ValueError(f"Invalid mode='{mode}'")
            _LOG.debug("  -> %s", cmd)
            script_txt.append(cmd)
        script_txt = "\n".join(script_txt)
        # Execute / save the script.
        if mode == "copy":
            for cmd in script_txt.split("\n"):
                hsystem.system(cmd)
        elif mode == "vimdiff":
            # Save the diff script.
            script_file_name = f"./tmp.vimdiff.{file_direction}.sh"
            hio.create_executable_script(script_file_name, script_txt)
            print(f"# To diff run:\n> {script_file_name}")
        else:
            raise ValueError(f"Invalid mode='{mode}'")


@task
def integrate_find_files(  # type: ignore
    ctx,
    subdir="",
):
    """
    Find the files that are touched in the current branch since last
    integration.
    """
    hlitauti._report_task()
    _ = ctx
    #
    abs_src_dir = "."
    abs_src_dir = os.path.normpath(abs_src_dir)
    hdbg.dassert_dir_exists(abs_src_dir)
    # Find the files touched in each branch since the last integration.
    src_files = sorted(
        _find_files_touched_since_last_integration(abs_src_dir, subdir)
    )
    print("* Files touched:\n%s" % "\n".join(src_files))


@task
def integrate_diff_overlapping_files(  # type: ignore
    ctx, src_dir_basename, dst_dir_basename, subdir=""
):
    """
    Find the files modified in both branches `src_dir_basename` and
    `dst_dir_basename` Compare these files from HEAD to master version before
    the branch point.

    This is used to check what changes were made to files modified by
    both branches.
    """
    hlitauti._report_task()
    _ = ctx
    # Check that the integration branches are in the expected state.
    _dassert_current_dir_matches(src_dir_basename)
    src_dir_basename, dst_dir_basename = _resolve_src_dst_names(
        src_dir_basename, dst_dir_basename, subdir
    )
    _dassert_is_integration_branch(src_dir_basename)
    _dassert_is_integration_branch(dst_dir_basename)
    _clean_both_integration_dirs(src_dir_basename, dst_dir_basename)
    # Find the files modified in both branches.
    src_hash = hgit.get_branch_hash(src_dir_basename)
    _LOG.info("src_hash=%s", src_hash)
    dst_hash = hgit.get_branch_hash(dst_dir_basename)
    _LOG.info("dst_hash=%s", dst_hash)
    diff_files1 = os.path.abspath("./tmp.files_modified1.txt")
    diff_files2 = os.path.abspath("./tmp.files_modified2.txt")
    cmd = f"cd {src_dir_basename} && git diff --name-only {src_hash} HEAD >{diff_files1}"
    hsystem.system(cmd)
    cmd = f"cd {dst_dir_basename} && git diff --name-only {dst_hash} HEAD >{diff_files2}"
    hsystem.system(cmd)
    common_files = "./tmp.common_files.txt"
    cmd = f"comm -12 {diff_files1} {diff_files2} >{common_files}"
    hsystem.system(cmd)
    # Get the base files to diff.
    files = hio.from_file(common_files).split("\n")
    files = [f for f in files if f != ""]
    _LOG.info("Found %d files to diff:\n%s", len(files), "\n".join(files))
    # Retrieve the original file and create the diff command.
    script_txt = []
    for src_file in files:
        hdbg.dassert_file_exists(src_file)
        # TODO(gp): Add function to add a suffix to a name, using
        #  os.path.dirname(), os.path.basename(), os.path.split_extension().
        dst_file = src_file.replace(".py", ".base.py")
        # Save the base file.
        cmd = f"git show {src_hash}:{src_file} >{dst_file}"
        rc = hsystem.system(cmd, abort_on_error=False)
        if rc == 0:
            # The file was created: nothing to do.
            pass
        elif rc == 128:
            # Note that the file potentially could not exist, i.e., it was added
            # in the branch. In this case Git returns:
            # ```
            # rc=128 fatal: path 'dataflow/pipelines/real_time/test/
            # test_dataflow_pipelines_real_time_pipeline.py' exists on disk, but
            # not in 'ce54877016204315766e90df7c45192bec1fbf20'
            src_file = "/dev/null"
        else:
            raise ValueError(f"cmd='{cmd}' returned {rc}")
        # Update the script to diff.
        script_txt.append(f"vimdiff {dst_file} {src_file}")
    # Save the script to compare.
    script_file_name = "./tmp.vimdiff_overlapping_files.sh"
    script_txt = "\n".join(script_txt)
    hio.create_executable_script(script_file_name, script_txt)
    print(f"# To diff against the base run:\n> {script_file_name}")
