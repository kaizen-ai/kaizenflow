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
import datetime
import logging
import os
import sys
import tempfile

import helpers.dbg as dbg
import helpers.parser as prsr
import helpers.system_interaction as si
import helpers.user_credentials as usc

# TODO(greg): consider moving this constant somewhere else.
_DEV_SERVER_NOTEBOOK_PUBLISHER_DIR = "/http/notebook_publisher"

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


def _copy_to_remote_folder(path_to_file: str, dst_dir: str) -> None:
    """
    Copy file to a directory on the remote server.
    :param path_to_file: The path to the local file
        e.g.: /tmp/relevance_and_event_relevance_exploration.html
    :param dst_dir: The folder in which the file will be copied
        e.g.: user@server_ip:/http/notebook_publisher
    """
    file_name = os.path.basename(path_to_file)
    dst_f_name = os.path.join(dst_dir, file_name)
    # File copying.
    cmd = f"scp {path_to_file} {dst_f_name}"
    si.system(cmd)
    _LOG.debug("Copy '%s' to '%s' over SSH", file_name, dst_dir)


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


def _get_file_from_git_branch(git_branch: str, git_path: str) -> str:
    """
    Checkout a file from a git branch and store it in a temporary location.
    :param git_branch: the branch name
        e.g. origin/PartTask302_download_eurostat_data
    :param git_path: the relative path to the file
        e.g. core/notebooks/gallery_signal_processing.ipynb
    :return: the path to the file retrieved
        e.g.: /tmp/gallery_signal_processing.ipynb
    """
    _LOG.debug("Create a temporary directory for a git worktree.")
    tmp_worktree_dir = tempfile.mkdtemp()
    #
    _LOG.debug("Add temporary git worktree in '%s'.", tmp_worktree_dir)
    si.system(f"git worktree add {tmp_worktree_dir}")
    si.system(f"cd {tmp_worktree_dir}")
    #
    _LOG.debug("Check out '%s/%s'.", git_branch, git_path)
    si.system(f"git checkout {git_branch} -- {git_path}")
    si.system("cd -")
    checked_out_file_path = os.path.join(tmp_worktree_dir, git_path)
    dst_file_path = os.path.join(
        tempfile.gettempdir(), os.path.basename(checked_out_file_path)
    )
    #
    _LOG.debug("Copy '%s' to '%s'.", checked_out_file_path, dst_file_path)
    si.system(f"cp {checked_out_file_path} {dst_file_path}")
    #
    _LOG.debug("Remove temporary git worktree '%s'.", tmp_worktree_dir)
    si.system(f"git worktree remove {tmp_worktree_dir}")
    si.system(f"git branch -d {os.path.basename(tmp_worktree_dir)}")
    return dst_file_path


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
    #
    if args.branch:
        src_file_name = _get_file_from_git_branch(args.branch, args.file)
    else:
        src_file_name = _get_path(args.file)
    # TODO(greg): make a special function for it, remove hardcoded server name.
    is_server = si.get_server_name() == "ip-172-31-16-23"
    # Detect the platform family.
    platform = sys.platform
    open_link_cmd = "start"  # MS Windows
    if platform == "linux":
        open_link_cmd = "xdg-open"
    elif platform == "darwin":
        open_link_cmd = "open"
    #
    if args.action == "open":
        # Convert the notebook to the HTML format and store in the TMP location.
        html_file_name = _export_html(src_file_name)
        #
        if is_server:
            # Just print the full file name for the HTML snapshot.
            print(f"HTML file path is: '{html_file_name}'")
        else:
            # Open with a browser locally.
            si.system(f"{open_link_cmd} {html_file_name}")
            sys.exit(0)
    elif args.action == "publish":
        # Convert the notebook to the HTML format and move to the PUB location.
        server_address = usc.get_p1_dev_server_ip()
        if is_server:
            pub_path = _DEV_SERVER_NOTEBOOK_PUBLISHER_DIR
            pub_html_file = _export_to_webpath(src_file_name, pub_path)
            pub_file_name = os.path.basename(pub_html_file)
            dbg.dassert_exists(pub_html_file)
        else:
            pub_path = f"{server_address}:{_DEV_SERVER_NOTEBOOK_PUBLISHER_DIR}"
            tmp_html_file_name = _export_html(src_file_name)
            pub_file_name = os.path.basename(tmp_html_file_name)
            _copy_to_remote_folder(tmp_html_file_name, pub_path)
        #
        _LOG.debug(
            "Notebook '%s' was converted to the HTML format and stored at '%s'",
            src_file_name,
            pub_path + pub_file_name,
        )
        print(
            f"HTML version of the notebook saved as '{pub_file_name}' "
            f"at the dev server publishing location. "
            f"You can view it using this command:"
        )
        print(
            f"(ssh -f -nNT -L 8877:localhost:8077 {server_address}; "
            f"{open_link_cmd} http://localhost:8877/{pub_file_name})"
        )


if __name__ == "__main__":
    _main(_parse())
