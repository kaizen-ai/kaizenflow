#!/usr/bin/env python

"""
This is a git commit-hook which can be used to check if huge files
where accidentally added to the staging area and are about to be
committed.
If there is a file which is bigger then the given "max_file_size"-
variable, the script will exit non-zero and abort the commit.
"""

import os
import subprocess
import sys


def sizeof_fmt(num):
    """
    This function will return a human-readable filesize-string
    like "3.5 MB" for it's given 'num'-parameter.
    From http://stackoverflow.com/questions/1094841
    """
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0


def _main():
    # The maximum file-size in KB (= 1024 byte) for a file to be committed:
    max_file_size = 512
    # The path to the git-binary:
    git_binary_path = "git"
    try:
        print(("Checking for files bigger then " + sizeof_fmt(max_file_size * 1024)))
        # Check all files in the staging-area, i.e., everything but un-staged files.
        # TODO(gp): Check only staged files.
        text = subprocess.check_output(
            [git_binary_path, "status", "--porcelain", "-uno"],
            stderr=subprocess.STDOUT).decode("utf-8")
        file_list = text.splitlines()
        print(file_list)
        # Check all files:
        for file_s in file_list:
            if os.path.exists(file_s):
                stat = os.stat(file_s[3:])
                if stat.st_size > (max_file_size * 1024):
                    # File is to big, abort the commit:
                    print(("'" + file_s[3:] + "' is too huge to be commited!",
                          "(" + sizeof_fmt(stat.st_size) + ")"))
                    sys.exit(1)
        # Everything seams to be okay:
        print("No huge files found.")
        #sys.exit(12)
        sys.exit(0)
    except subprocess.CalledProcessError:
        # There was a problem calling "git status".
        print("Oops...")
        sys.exit(12)

    # https://github.com/pre-commit/pre-commit-hooks/tree/master/pre_commit_hooks
    # https://github.com/pre-commit/pre-commit-hooks/blob/master/pre_commit_hooks/check_ast.py
    # https://github.com/pre-commit/pre-commit-hooks/blob/master/pre_commit_hooks/check_added_large_files.py
    # https://github.com/pre-commit/pre-commit-hooks/blob/master/pre_commit_hooks/check_merge_conflict.py

    # https://code-maven.com/enforcing-commit-message-format-in-git


if __name__ == '__main__':
    print("git pre-commit hook ...")
    #_main()
    sys.exit(0)
