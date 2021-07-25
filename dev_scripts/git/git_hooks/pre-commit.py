#!/usr/bin/env python3

"""
This is a git commit-hook used to check that a commit follows certain invariants.

In case of violations the script will exit non-zero and abort the commit.
User can ignore the checks with `git commit --no-verify '...'`.
"""

# TODO(gp): Check these hooks
# https://github.com/pre-commit/pre-commit-hooks/tree/master/pre_commit_hooks
# https://github.com/pre-commit/pre-commit-hooks/blob/master/pre_commit_hooks/check_ast.py
# https://github.com/pre-commit/pre-commit-hooks/blob/master/pre_commit_hooks/check_added_large_files.py
# https://github.com/pre-commit/pre-commit-hooks/blob/master/pre_commit_hooks/check_merge_conflict.py
# https://code-maven.com/enforcing-commit-message-format-in-git

# NOTE: This file should depend only on Python standard libraries.
import logging
import sys

import dev_scripts.git.git_hooks.utils as ghutils

_LOG = logging.getLogger(__name__)


# #############################################################################


if __name__ == "__main__":
    print("# Running git pre-commit hook ...")
    ghutils.check_master()
    ghutils.check_author()
    ghutils.check_file_size()
    ghutils.check_words()
    print(
        "\n"
        + ghutils.color_highlight(
            "##### All pre-commit hooks passed: committing ######", "purple"
        )
    )
    sys.exit(0)
