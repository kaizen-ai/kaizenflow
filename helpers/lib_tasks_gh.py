"""
Import as:

import helpers.lib_tasks_gh as hlitagh
"""

import json
import logging
import os
import re
from typing import Optional, Tuple

from invoke import task

# We want to minimize the dependencies from non-standard Python packages since
# this code needs to run with minimal dependencies and without Docker.
import helpers.hdbg as hdbg
import helpers.hgit as hgit
import helpers.hprint as hprint
import helpers.hsystem as hsystem
import helpers.htable as htable
import helpers.lib_tasks_utils as hlitauti

_LOG = logging.getLogger(__name__)

# pylint: disable=protected-access

# #############################################################################
# GitHub CLI.
# #############################################################################


@task
def gh_login(  # type: ignore
    ctx,
    account="",
    print_status=False,
):
    hlitauti.report_task()
    #
    if not account:
        # Retrieve the name of the repo, e.g., "alphamatic/amp".
        full_repo_name = hgit.get_repo_full_name_from_dirname(
            ".", include_host_name=False
        )
        _LOG.debug(hprint.to_str("full_repo_name"))
        account = full_repo_name.split("/")[0]
    _LOG.info(hprint.to_str("account"))
    #
    ssh_filename = os.path.expanduser(f"~/.ssh/id_rsa.{account}.github")
    _LOG.debug(hprint.to_str("ssh_filename"))
    if os.path.exists(ssh_filename):
        cmd = f"export GIT_SSH_COMMAND='ssh -i {ssh_filename}'"
        print(cmd)
    else:
        _LOG.warning("Can't find file '%s'", ssh_filename)
    #
    if print_status:
        cmd = "gh auth status"
        hlitauti.run(ctx, cmd)
    #
    github_pat_filename = os.path.expanduser(f"~/.ssh/github_pat.{account}.txt")
    if os.path.exists(github_pat_filename):
        cmd = f"gh auth login --with-token <{github_pat_filename}"
        hlitauti.run(ctx, cmd)
    else:
        _LOG.warning("Can't find file '%s'", github_pat_filename)
    #
    if print_status:
        cmd = "gh auth status"
        hlitauti.run(ctx, cmd)


def _get_branch_name(branch_mode: str) -> Optional[str]:
    if branch_mode == "current_branch":
        branch_name: Optional[str] = hgit.get_branch_name()
    elif branch_mode == "master":
        branch_name = "master"
    elif branch_mode == "all":
        branch_name = None
    else:
        raise ValueError(f"Invalid branch='{branch_mode}'")
    return branch_name


def _get_workflow_table() -> htable.TableType:
    """
    Get a table with the status of the GH workflow for the current repo.
    """
    # Get the workflow status from GH.
    cmd = "export NO_COLOR=1; gh run list"
    _, txt = hsystem.system_to_string(cmd)
    _LOG.debug(hprint.to_str("txt"))
    # pylint: disable=line-too-long
    # > gh run list
    # STATUS  TITLE                                                          WORKFLOW    BRANCH                                                         EVENT         ID          ELAPSED  AGE
    # *       AmpTask1786_Integrate_20230518_2                               Fast tests  AmpTask1786_Integrate_20230518_2                               pull_request  5027911519  4m49s    4m
    # > gh run list | more
    # completed       success AmpTask1786_Integrate_20230518_2        Fast tests      AmpTask1786_Integrate_20230518_2        pull_request    5027911519      7m17s   10m
    # in_progress             AmpTask1786_Integrate_20230518_2        Slow tests      AmpTask1786_Integrate_20230518_2        pull_request    5027911518      10m9s   10m

    # pylint: enable=line-too-long
    # The output is tab separated, so convert it into CSV.
    first_line = txt.split("\n")[0]
    _LOG.debug("first_line=%s", first_line.replace("\t", ","))
    num_cols = len(first_line.split("\t"))
    _LOG.debug(hprint.to_str("first_line num_cols"))
    cols = [
        "completed",  # E.g., completed, in_progress
        "status",   # E.g., success, failure
        "name",  # Aka title
        "workflow",
        "branch",
        "event",
        "id",
        "elapsed",
        "age",
    ]
    hdbg.dassert_eq(num_cols, len(cols))
    # Build the table.
    table = htable.Table.from_text(cols, txt, delimiter="\t")
    _LOG.debug(hprint.to_str("table"))
    return table


@task
def gh_workflow_list(  # type: ignore
    ctx,
    filter_by_branch="current_branch",
    filter_by_completed="all",
    report_only_status=True,
    show_stack_trace=False,
):
    """
    Report the status of the GH workflows.

    :param filter_by_branch: name of the branch to check
        - `current_branch` for the current Git branch
        - `master` for master branch
        - `all` for all branches
    :param filter_by_completed: filter table by the status of the workflow
        - E.g., "failure", "success"
    :param show_stack_trace: in case of error run `pytest_repro` reporting also
        the stack trace
    """
    hlitauti.report_task(txt=hprint.to_str("filter_by_branch filter_by_completed"))
    # Login.
    gh_login(ctx)
    # Get the table.
    table = _get_workflow_table()
    # Filter table based on the branch.
    if filter_by_branch != "all":
        field = "branch"
        value = _get_branch_name(filter_by_branch)
        print(f"Filtering table by {field}={value}")
        table = table.filter_rows(field, value)
    # Filter table by the workflow status.
    if filter_by_completed != "all":
        field = "completed"
        value = filter_by_completed
        print(f"Filtering table by {field}={value}")
        table = table.filter_rows(field, value)
    if (
        filter_by_branch not in ("current_branch", "master")
        or not report_only_status
    ):
        print(str(table))
        return
    # For each workflow find the last success.
    branch_name = hgit.get_branch_name()
    workflows = table.unique("workflow")
    print(f"workflows={workflows}")
    for workflow in workflows:
        print(hprint.frame(workflow))
        table_tmp = table.filter_rows("workflow", workflow)
        # Report the full status.
        print(table_tmp)
        # Find the first success.
        num_rows = table.size()[0]
        _LOG.debug("num_rows=%s", num_rows)
        for i in range(num_rows):
            status_column = table_tmp.get_column("status")
            _LOG.debug("status_column=%s", str(status_column))
            hdbg.dassert_lt(i, len(status_column),
                            msg="status_column=%s" % status_column)
            status = status_column[i]
            if status == "success":
                print(f"Workflow '{workflow}' for '{branch_name}' is ok")
                break
            if status in ("failure", "startup_failure", "cancelled"):
                _LOG.error(
                    "Workflow '%s' for '%s' is broken", workflow, branch_name
                )
                # Get the output of the broken run.
                # > gh run view 1477484584 --log-failed
                workload_id = table_tmp.get_column("id")[i]
                log_file_name = f"tmp.failure.{workflow}.{branch_name}.txt"
                log_file_name = log_file_name.replace(" ", "_").lower()
                cmd = f"gh run view {workload_id} --log-failed >{log_file_name}"
                hsystem.system(cmd)
                print(f"# Log is in '{log_file_name}'")
                # Run_fast_tests  Run fast tests  2021-12-19T00:19:38.3394316Z FAILED data
                # cmd = rf"grep 'Z FAILED ' {log_file_name}"
                workflow_as_str = workflow.lower().replace(" ", "_")
                script_name = f"./tmp.pytest_repro.{workflow_as_str}.sh"
                cmd = f"invoke pytest_repro --file-name {log_file_name} --script-name {script_name}"
                if show_stack_trace:
                    cmd += " -s"
                hsystem.system(cmd, suppress_output=False, abort_on_error=False)
                break
            if status == "":
                # It's in progress.
                pass
            else:
                raise ValueError(f"Invalid status='{status}'")


@task
def gh_workflow_run(ctx, branch="current_branch", workflows="all"):  # type: ignore
    """
    Run GH workflows in a branch.
    """
    hlitauti.report_task(txt=hprint.to_str("branch workflows"))
    # Login.
    gh_login(ctx)
    # Get the branch name.
    if branch == "current_branch":
        branch_name = hgit.get_branch_name()
    elif branch == "master":
        branch_name = "master"
    else:
        raise ValueError(f"Invalid branch='{branch}'")
    _LOG.debug(hprint.to_str("branch_name"))
    # Get the workflows.
    if workflows == "all":
        gh_tests = ["fast_tests", "slow_tests"]
    else:
        gh_tests = [workflows]
    _LOG.debug(hprint.to_str("workflows"))
    # Run.
    for gh_test in gh_tests:
        gh_test += ".yml"
        # gh workflow run fast_tests.yml --ref AmpTask1251_Update_GH_actions_for_amp
        cmd = f"gh workflow run {gh_test} --ref {branch_name}"
        hlitauti.run(ctx, cmd)


def _get_repo_full_name_from_cmd(repo_short_name: str) -> Tuple[str, str]:
    """
    Convert the `repo_short_name` from command line (e.g., "current", "amp",
    "lm") to the repo_short_name full name without host name.
    """
    repo_full_name_with_host: str
    if repo_short_name == "current":
        # Get the repo name from the current repo.
        repo_full_name_with_host = hgit.get_repo_full_name_from_dirname(
            ".", include_host_name=True
        )
        # Compute the short repo name corresponding to "current".
        repo_full_name = hgit.get_repo_full_name_from_dirname(
            ".", include_host_name=False
        )
        ret_repo_short_name = hgit.get_repo_name(
            repo_full_name, in_mode="full_name", include_host_name=False
        )

    else:
        # Get the repo name using the short -> full name mapping.
        repo_full_name_with_host = hgit.get_repo_name(
            repo_short_name, in_mode="short_name", include_host_name=True
        )
        ret_repo_short_name = repo_short_name
    _LOG.debug(
        "repo_short_name=%s -> repo_full_name_with_host=%s ret_repo_short_name=%s",
        repo_short_name,
        repo_full_name_with_host,
        ret_repo_short_name,
    )
    return repo_full_name_with_host, ret_repo_short_name


# #############################################################################


def _get_gh_issue_title(issue_id: int, repo_short_name: str) -> Tuple[str, str]:
    """
    Get the title of a GitHub issue.

    :param repo_short_name: `current` refer to the repo_short_name where we are,
        otherwise a repo_short_name short name (e.g., "amp")
    """
    repo_full_name_with_host, repo_short_name = _get_repo_full_name_from_cmd(
        repo_short_name
    )
    # > (export NO_COLOR=1; gh issue view 1251 --json title)
    # {"title":"Update GH actions for amp"}
    hdbg.dassert_lte(1, issue_id)
    cmd = f"gh issue view {issue_id} --repo {repo_full_name_with_host} --json title,url"
    _, txt = hsystem.system_to_string(cmd)
    _LOG.debug("txt=\n%s", txt)
    # Parse json.
    dict_ = json.loads(txt)
    _LOG.debug("dict_=\n%s", dict_)
    title = dict_["title"]
    _LOG.debug("title=%s", title)
    url = dict_["url"]
    _LOG.debug("url=%s", url)
    # Remove some annoying chars.
    for char in ": + ( ) / ` *".split():
        title = title.replace(char, "")
    # Replace multiple spaces with one.
    title = re.sub(r"\s+", " ", title)
    #
    title = title.replace(" ", "_")
    title = title.replace("-", "_")
    title = title.replace("'", "_")
    title = title.replace("`", "_")
    title = title.replace('"', "_")
    # Add the prefix `AmpTaskXYZ_...`
    task_prefix = hgit.get_task_prefix_from_repo_short_name(repo_short_name)
    _LOG.debug("task_prefix=%s", task_prefix)
    title = f"{task_prefix}{issue_id}_{title}"
    return title, url


@task
def gh_issue_title(ctx, issue_id, repo_short_name="current", pbcopy=True):  # type: ignore
    """
    Print the title that corresponds to the given issue and repo_short_name.
    E.g., AmpTask1251_Update_GH_actions_for_amp.

    :param pbcopy: save the result into the system clipboard (only on macOS)
    """
    hlitauti.report_task(txt=hprint.to_str("issue_id repo_short_name"))
    # Login.
    gh_login(ctx)
    #
    issue_id = int(issue_id)
    hdbg.dassert_lte(1, issue_id)
    title, url = _get_gh_issue_title(issue_id, repo_short_name)
    # Print or copy to clipboard.
    msg = f"{title}: {url}"
    hlitauti._to_pbcopy(msg, pbcopy)


def _check_if_pr_exists(title: str) -> bool:
    """
    Return whether a PR exists or not.
    """
    # > gh pr diff AmpTask1955_Lint_20211219
    # no pull requests found for branch "AmpTask1955_Lint_20211219"
    cmd = f"gh pr diff {title}"
    rc = hsystem.system(cmd, abort_on_error=False)
    pr_exists: bool = rc == 0
    return pr_exists


@task
def gh_create_pr(  # type: ignore
    ctx,
    body="",
    draft=True,
    auto_merge=False,
    repo_short_name="current",
    title="",
):
    """
    Create a draft PR for the current branch in the corresponding
    repo_short_name.

    ```
    # To open a PR in the web browser
    > gh pr view --web

    # To see the status of the checks
    > gh pr checks
    ```

    :param body: the body of the PR
    :param draft: draft or ready-to-review PR
    :param auto_merge: enable auto merging PR
    :param title: title of the PR or the branch name, if title is empty
    """
    hlitauti.report_task()
    # Login.
    gh_login(ctx)
    #
    branch_name = hgit.get_branch_name()
    if not title:
        # Use the branch name as title.
        title = branch_name
    repo_full_name_with_host, repo_short_name = _get_repo_full_name_from_cmd(
        repo_short_name
    )
    _LOG.info(
        "Creating PR with title '%s' for '%s' in %s",
        title,
        branch_name,
        repo_full_name_with_host,
    )
    if auto_merge:
        hdbg.dassert(
            not draft, "The PR can't be a draft in order to auto merge it"
        )
    pr_exists = _check_if_pr_exists(title)
    _LOG.debug(hprint.to_str("pr_exists"))
    if pr_exists:
        _LOG.warning("PR '%s' already exists: skipping creation", title)
    else:
        cmd = (
            "gh pr create"
            + f" --repo {repo_full_name_with_host}"
            + (" --draft" if draft else "")
            + f' --title "{title}"'
            + f' --body "{body}"'
        )
        # TODO(gp): Use _to_single_line_cmd
        hlitauti.run(ctx, cmd)
    if auto_merge:
        cmd = f"gh pr ready {title}"
        hlitauti.run(ctx, cmd)
        cmd = f"gh pr merge {title} --auto --delete-branch --squash"
        hlitauti.run(ctx, cmd)


# TODO(gp): Add gh_open_pr to jump to the PR from this branch.
