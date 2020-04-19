#!/usr/bin/env python
r"""
# #############################################################################
# Open a notebook.
# #############################################################################

> amp/dev_scripts/notebooks/publish_notebook.py \
    --file nlp/notebooks/PartTask768_event_filtering.ipynb \
    --action open

This command opens a local notebook as HTML into the browser, if possible.

- Detailed flow:
    1. Convert a locally available notebook to the HTML format.
    2. Save the HTML page to a temporary location, adding a timestamp to the
       name.
    3. On a local computer: open the HTML page using the system default browser.
       On the dev server: return the full path to the file as a result.

# #############################################################################
# Publish a notebook.
# #############################################################################

> amp/dev_scripts/notebooks/publish_notebook.py \
    --file nlp/notebooks/PartTask768_event_filtering.ipynb \
    --action publish

This command publishes a local notebook as HTML on the dev server.

- Detailed flow:
    - On a local computer:
    1. Convert a locally available notebook to the HTML format.
    2. Save the HTML page to a temporary location, adding a timestamp to the
       name.
    3. Copy it to the publishing location on the dev server.
    4. Print the path to the published HTML page and a command to open it using
       an ssh tunnel.

    - On the dev server:
    1. Convert a locally available notebook to the HTML format.
    2. Add a timestamp to the name.
    3. Copy the HTML page to the publishing location on the dev server.
    4. Print a link to the file, and a command to open it using ssh tunneling.

# #############################################################################
# Open or publish a notebook from a git branch.
# #############################################################################

>  amp/dev_scripts/notebooks/publish_notebook.py \
    --file nlp/notebooks/PartTask768_event_filtering.ipynb \
    --branch origin/master
    --action open

This command allows to open or publish a notebook that is in a branch different
from the one we already in.

The behavior is the same as described above, but before the first step:
- A new temporary worktree is added to a temporary directory.
- The file is checked out there from the given branch.
Then the above steps are followed.
"""

import argparse
import datetime
import logging
import os
import sys
import tempfile

import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.parser as prsr
import helpers.system_interaction as si
import helpers.user_credentials as usc

# TODO(greg): consider moving this constant somewhere else.
_DEV_SERVER_NOTEBOOK_PUBLISHER_DIR = "/http/notebook_publisher"

_LOG = logging.getLogger(__name__)


def _add_tag(file_path: str, tag: str = "") -> str:
    """
    By default, add current timestamp in the filename. Returns new filename.
    """
    name, extension = os.path.splitext(os.path.basename(file_path))
    if not tag:
        tag = datetime.datetime.now().strftime("_%Y%m%d_%H%M%S")
    return "".join([name, tag, extension])


def _export_html(path_to_notebook: str) -> str:
    """
    Accept ipynb, exports to html, adds a timestamp to the file name, and
    returns the name of the created file.
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
    _LOG.debug("Export %s to html.", file_name)
    return dst_path


def _create_remote_folder(server_address: str, dir_path: str) -> None:
    cmd = f"ssh {server_address} mkdir -p {dir_path}"
    si.system(cmd)
    _LOG.debug("Remote directory '%s' created.", dir_path)


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
    _LOG.debug("Copy '%s' to '%s' over SSH.", file_name, dst_dir)


def _export_to_webpath(path_to_notebook: str, dst_dir: str) -> str:
    """
    Create a folder if it does not exist. Export ipynb to html, to add a
    timestamp, moves to dst_dir.
    :param path_to_notebook: The path to the file of the notebook
        e.g.: _data/relevance_and_event_relevance_exploration.ipynb
    :param dst_dir: destination folder to move
    """
    html_src_path = _export_html(path_to_notebook)
    html_name = os.path.basename(html_src_path)
    html_dst_path = os.path.join(dst_dir, html_name)
    # If there is no such directory, create it.
    io_.create_dir(dst_dir, incremental=True)
    # Move html.
    _LOG.debug("Export '%s' to '%s'.", html_src_path, html_dst_path)
    cmd = "mv {src} {dst}".format(src=html_src_path, dst=html_dst_path)
    si.system(cmd)
    return html_dst_path


# TODO(gp): Reuse url.py code.
def _get_path(path_or_url: str) -> str:
    """
    Get path from file, local link or github link.
    :param path_or_url: url to notebook/github, local path,
        e.g.: https://github.com/...ipynb
    :return: Path to file
        e.g.: UnderstandingAnalysts.ipynb
    """
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
            "Incorrect link to git or local jupiter notebook or file path."
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
    dst_file_path = os.path.join(
        tempfile.gettempdir(), os.path.basename(git_path)
    )
    _LOG.debug("Check out '%s/%s' to '%s'.", git_branch, git_path, dst_file_path)
    si.system(f"git show {git_branch}:{git_path} > {dst_file_path}")
    return dst_file_path


# #############################################################################
_ACTION_PUBLISH = "publish"
_ACTION_OPEN = "open"


def _parse() -> argparse.ArgumentParser:
    actions = {
        _ACTION_PUBLISH: f"'--action {_ACTION_PUBLISH}' - Publish notebook.(default)",
        _ACTION_OPEN: f"'--action {_ACTION_OPEN}' - Open selected notebook in a browser.",
    }
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "-f",
        "--file",
        action="store",
        required=True,
        type=str,
        help="The path to the file ipynb, jupyter url, or github url.",
    )
    parser.add_argument(
        "-b",
        "--branch",
        action="store",
        required=False,
        type=str,
        help="The branch, from which the notebook file will be checked out.",
    )
    parser.add_argument(
        "--subdir",
        action="store",
        default="",
        help="The name of the enclosing folder with html file.",
    )
    #
    parser.add_argument(
        "-a",
        "--action",
        nargs="+",
        action="store",
        default=[_ACTION_PUBLISH],
        choices=actions.keys(),
        help="\n".join(actions.values()),
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
    # Detect the platform family.
    platform = sys.platform
    open_link_cmd = "start"  # MS Windows
    if platform == "linux":
        open_link_cmd = "xdg-open"
    elif platform == "darwin":
        open_link_cmd = "open"
    #
    html_file_name = _export_html(src_file_name)
    if _ACTION_OPEN in args.action:
        _LOG.debug("Action '%s' selected.", _ACTION_OPEN)
        # Convert the notebook to the HTML format and store in the TMP location.
        si.system(f"{open_link_cmd} {html_file_name}")
        print(f"You opened local file: {html_file_name}")
    if _ACTION_PUBLISH in args.action:
        _LOG.debug("Action '%s' selected.", _ACTION_PUBLISH)
        # Convert the notebook to the HTML format and move to the PUB location.
        server_address = usc.get_p1_dev_server_ip()
        dst_path = f"{_DEV_SERVER_NOTEBOOK_PUBLISHER_DIR}/{args.subdir}"
        _create_remote_folder(server_address, dst_path)
        pub_path = f"{server_address}:{dst_path}"
        pub_file_name = os.path.basename(html_file_name)
        _copy_to_remote_folder(html_file_name, pub_path)
        #
        _LOG.debug(
            "Notebook '%s' was converted to the HTML format and stored at '%s'",
            src_file_name,
            pub_path + pub_file_name,
        )
        print(
            f"HTML version of the notebook saved as '{pub_file_name}' "
            f"at the dev server publishing location. "
            f"You can view it using this url:"
        )
        print(f"http://172.31.16.23:8077/{pub_file_name}")


if __name__ == "__main__":
    _main(_parse())
