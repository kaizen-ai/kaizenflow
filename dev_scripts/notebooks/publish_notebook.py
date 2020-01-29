#!/usr/bin/env python

"""
Given a notebook specified as:
- a ipynb file, e.g.,
    data/web_path_two/Minute_distribution_20180802_182656.ipynb
- a jupyter url, e.g.,
    https://github.com/...ipynb
- a github url

- Backup a notebook and publish notebook on shared space;
> publish_notebook.py --file xyz.ipynb --action publish

- Open a notebook in Chrome
> publish_notebook.py --file xyz.ipynb --action open
"""

import argparse
import logging
import os
import sys
import datetime

import helpers.dbg as dbg
import helpers.parser as prsr
import helpers.system_interaction as si
import helpers.user_credentials as usc

_LOG = logging.getLogger(__name__)


def _add_tag(file_path, tag=None):
    """
    By default add timestamp in filename
    :param file_path:
    :param tag:
    :return: file na
    """
    name, extension = os.path.splitext(os.path.basename(file_path))
    if not tag:
        tag = datetime.datetime.now().strftime("_%Y%m%d_%H%M%S")
    return "".join([name, tag, extension])


def _export_html(path_to_notebook):
    """
    Accept ipynb, exports to html, adds a timestamp to the file name, and
    returns the name of the created file
    :param path_to_notebook: The path to the file of the notebook e.g.:
        _data/relevance_and_event_relevance_exploration.ipynb
    :return: The name of the html file with a timestamp e.g.:
        test_notebook_20180802_162438.html
    """
    # Get file name and path to file.
    dir_path = os.path.dirname(os.path.realpath(path_to_notebook))
    file_name = os.path.splitext(os.path.basename(path_to_notebook))[0]
    # Create file name and timestamp.
    file_name_html = file_name + ".html"
    file_name_html = _add_tag(file_name_html)
    dst_path = os.path.join(dir_path, file_name_html)
    # Export ipynb to html format.
    cmd = (
        "jupyter nbconvert {path_to_file} --to html"
        " --output {dst_path}".format(
            path_to_file=path_to_notebook, dst_path=dst_path
        )
    )
    si.system(cmd)
    _LOG.debug("Export %s to html", file_name)
    return dst_path


def _copy_to_folder(path_to_notebook, dst_dir):
    """
    Copy file to another directory
    :param path_to_notebook: The path to the file of the notebook
        e.g.: _data/relevance_and_event_relevance_exploration.ipynb
    :param dst_dir: The folder in which the file will be copied e.g.: _data/
    :return: None
    """
    # file_name = os.path.basename(path_to_notebook)
    dst_f_name = os.path.join(dst_dir, _add_tag(path_to_notebook))
    # If there is no such directory, create it.
    if not os.path.isdir(dst_dir):
        os.makedirs(dst_dir)
    # File copying.
    cmd = "cp {src} {dst}".format(src=path_to_notebook, dst=dst_f_name)
    si.system(cmd)
    path_to_notebook = os.path.basename(path_to_notebook)
    _LOG.debug("Copy '%s' to '%s'", path_to_notebook, dst_dir)


def _export_to_webpath(path_to_notebook, dst_dir):
    """
    Create a folder if it does not exist. Export ipynb to html, to add a
    timestamp, moves to dst_dir
    :param path_to_notebook: The path to the file of the notebook
        e.g.: _data/relevance_and_event_relevance_exploration.ipynb
    :param dst_dir: destination folder to move
    :return: None
    """
    html_src_path = _export_html(path_to_notebook)
    html_name = os.path.basename(html_src_path)
    html_dst_path = os.path.join(dst_dir, html_name)
    # If there is no such directory, create it.
    if not os.path.isdir(dst_dir):
        os.makedirs(dst_dir)
    # Move html.
    _LOG.debug("Export '%s' to '%s'", html_src_path, html_dst_path)
    cmd = "mv {src} {dst}".format(src=html_src_path, dst=html_dst_path)
    si.system(cmd)
    return html_dst_path


def _show_file_in_folder(folder_path):
    """
    Print all files in a folder
    :param folder_path:
    :return: None
    """
    # Check the correctness of the entered path
    if not folder_path.endswith("/"):
        folder_path = folder_path + "/"
    only_files = [
        _file
        for _file in os.listdir(folder_path)
        if os.path.isfile(os.path.join(folder_path, _file))
    ]
    for _one_file in only_files:
        print(folder_path + _one_file)


# TODO(gp): Reuse url.py code.
def _get_path(path_or_url):
    """
    Get path from file, local link or github link
    :param path_or_url: url to notebook/github, local path,
        e.g.: https://github.com/...ipynb
    :return: Path to file
        e.g.: UnderstandingAnalysts.ipynb
    """
    ret = ""
    if "https://github" in path_or_url:
        ret = "/".join(path_or_url.split("/")[7:])
    elif "http://" in path_or_url:
        ret = "/".join(path_or_url.split("/")[4:])
        dbg.dassert_exists(ret)
        if not os.path.exists(path_or_url):
            # Try to find the file with find basename(ret) in the current
            # client.
            pass
    elif path_or_url.endswith(".ipynb") and os.path.exists(path_or_url):
        ret = path_or_url
    else:
        raise ValueError(
            "Incorrect link to git or local jupiter notebook or file path"
        )
    return ret


# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--branch",
        action="store",
        required=False,
        type=str,
        help="The branch, from which the notebook file will be checked out.",
    )
    parser.add_argument(
        "--file",
        action="store",
        required=True,
        type=str,
        help="The path to the file ipynb, jupyter url, or github url.",
    )
    #
    parser.add_argument(
        "--action",
        action="store",
        default="publish",
        choices=["open", "publish"],
        help="Open with Chrome without publish, or archive / publish as html",
    )
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level)
    src_file_name = _get_path(args.file)
    # Export to html, add timestamp, archive html.
    if args.action == "open":
        html_path = _export_html(src_file_name)
        # Open with browser locally.
        # TODO(gp): Check of Mac.
        cmd = "open %s" % html_path
        si.system(cmd)
        sys.exit(0)
    elif args.action == "publish":
        user_credentials = usc.get_credentials()
        html_path = user_credentials["notebook_html_path"]
        dbg.dassert_is_not(html_path, None)
        backup_path = user_credentials["notebook_backup_path"]
        dbg.dassert_is_not(html_path, None)
        if args.project is not None:
            html_path = os.path.join(html_path, args.project)
            backup_path = os.path.join(backup_path, args.project)
        _LOG.info("html_path=%s", html_path)
        _LOG.info("backup path=%s", backup_path)
        #
        _LOG.debug("# Backing up ipynb")
        _copy_to_folder(src_file_name, backup_path)
        #
        _LOG.debug("# Publishing html")
        html_file_name = _export_to_webpath(src_file_name, html_path)
        print("HTML file path is: %s" % html_file_name)
        dbg.dassert_exists(html_file_name)
        #
        print("\nTo visualize on Mac run:")
        cmd = (
            "dev_scripts/open_remote_html_mac.sh %s\n" % html_file_name
            + "FILE='%s'; scp 54.172.40.4:$FILE /tmp; open /tmp/$(basename $FILE)"
            % html_file_name
        )
        print(cmd)


if __name__ == "__main__":
    _main(_parse())
