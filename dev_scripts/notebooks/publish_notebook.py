#!/usr/bin/env python
r"""
This script performs several actions on a Jupyter notebook, such as:
- opening a notebook in the browser
- publishing a notebook locally or remotely on an HTML server

# Open a notebook in the browser

- The following command opens an archived notebook as HTML into the browser:
  ```
  > publish_notebook.py \
      --file s3://.../notebooks/PTask768_event_filtering.html \
      --action open
  ```

# Publish a notebook

  ```
  > publish_notebook.py \
      --file nlp/notebooks/PTask768_event_filtering.ipynb \
      --action publish_on_s3
  ```
"""

import argparse
import logging
import os
import sys
import tempfile
from typing import BinaryIO, List, Tuple

import requests

import helpers.dbg as dbg
import helpers.io_ as hio
import helpers.open as opn
import helpers.parser as prsr
import helpers.printing as hprint
import helpers.s3 as hs3
import helpers.system_interaction as hsyste

_LOG = logging.getLogger(__name__)


# TODO(gp): Reuse url.py code.
def _get_path(path_or_url: str) -> str:
    """
    Get path from file, local link, or GitHub link.

    :param path_or_url: URL to notebook/github, local path,
        E.g., `https://github.com/...ipynb`
    :return: path to file
        E.g., `UnderstandingAnalysts.ipynb`
    """
    if "https://github" in path_or_url:
        ret = "/".join(path_or_url.split("/")[7:])
    elif "http://" in path_or_url:
        ret = "/".join(path_or_url.split("/")[4:])
        dbg.dassert_exists(ret)
        if not os.path.exists(path_or_url):
            # Try to find the file with find basename in the current client.
            pass
    elif path_or_url.endswith(".ipynb") and os.path.exists(path_or_url):
        ret = path_or_url
    else:
        raise ValueError(f"Incorrect file '{path_or_url}'")
    return ret


# TODO(gp): This can go in `git.py`.
def _get_file_from_git_branch(git_branch: str, git_path: str) -> str:
    """
    Checkout a file from a git branch and store it in a temporary location.

    :param git_branch: the branch name
        E.g., `origin/PTask302_download_eurostat_data`
    :param git_path: the relative path to the file
        E.g., `core/notebooks/gallery_signal_processing.ipynb`
    :return: the path to the file retrieved
        E.g., `/tmp/gallery_signal_processing.ipynb`
    """
    dst_file_name = os.path.join(
        tempfile.gettempdir(), os.path.basename(git_path)
    )
    _LOG.debug("Check out '%s/%s' to '%s'.", git_branch, git_path, dst_file_name)
    hsyste.system(f"git show {git_branch}:{git_path} > {dst_file_name}")
    return dst_file_name


def _export_notebook_to_html(ipynb_file_name: str, tag: str) -> str:
    """
    Export a notebook as HTML in the same location, adding a timestamp to file
    name.

    :param ipynb_file_name: path to the notebook file
        E.g., `.../event_relevance_exploration.ipynb`
    :return: path to the HTML file with a timestamp
        E.g., `event_relevance_exploration.20180802_162438.html`
    """
    # Extract file name and dir for the ipynb file.
    dir_path = os.path.dirname(os.path.realpath(ipynb_file_name))
    file_name = os.path.splitext(os.path.basename(ipynb_file_name))[0]
    # Create dst file name including timestamp.
    html_file_name = file_name + ".html"
    html_file_name = hsyste.append_timestamp_tag(html_file_name, tag)
    dst_file_name = os.path.join(dir_path, html_file_name)
    # Export notebook file to HTML format.
    cmd = (
        f"jupyter nbconvert {ipynb_file_name} --to html --output {dst_file_name}"
    )
    hsyste.system(cmd)
    _LOG.debug("Export notebook '%s' to HTML '%s'", file_name, dst_file_name)
    return dst_file_name


def _export_notebook_to_dir(ipynb_file_name: str, tag: str, dst_dir: str) -> str:
    """
    Export a notebook as HTML to a dst dir.

    :param ipynb_file_name: path to the notebook file
        E.g., `.../event_relevance_exploration.ipynb`
    :param dst_dir: destination folder
    """
    # Convert to HTML in the same location.
    html_src_path = _export_notebook_to_html(ipynb_file_name, tag)
    #
    html_file_name = os.path.basename(html_src_path)
    html_dst_path = os.path.join(dst_dir, html_file_name)
    # Move HTML.
    _LOG.debug("Export '%s' to '%s'", html_src_path, html_dst_path)
    hio.create_dir(dst_dir, incremental=True)
    cmd = f"mv {html_src_path} {html_dst_path}"
    hsyste.system(cmd)
    # Print info.
    _LOG.info("Generated HTML file '%s'", html_dst_path)
    cmd = f"""
        # To open the notebook run:
        > publish_notebook.py --file {html_dst_path} --action open
        """
    print(hprint.dedent(cmd))
    return html_dst_path


def _post_to_s3(local_src_path: str, s3_path: str, aws_profile: str) -> str:
    """
    Export a notebook as HTML to S3.

    :param local_src_path: the path of the local ipynb to export
    :param s3_path: full S3 path starting with `s3://` and ending with `/notebooks`
    :param aws_profile: the profile to use
    """
    dbg.dassert_file_exists(local_src_path)
    # TODO(gp): Pass s3_path through the credentials.
    hs3.check_valid_s3_path(s3_path)
    dbg.dassert(
        s3_path.endswith("/notebooks"),
        "S3 path needs to point to a `notebooks` dir, instead s3_path='%s'",
        s3_path,
    )
    # Compute the full S3 path.
    remote_path = os.path.join(s3_path, os.path.basename(local_src_path))
    # TODO(gp): Make sure the S3 dir exists.
    _LOG.info("Copying '%s' to '%s'", local_src_path, remote_path)
    s3fs = hs3.get_s3fs(aws_profile)
    s3fs.put(local_src_path, remote_path)
    return remote_path


# TODO(gp): This can be more general than this file.
def _post_to_webserver(local_src_path: str, remote_dst_path: str) -> None:
    """
    Copy file to a directory on the remote server using HTTP post.

    :param local_src_path: path to the local file
        E.g.: `.../relevance_and_event_relevance_exploration.html`
    :param remote_dst_path: folder in which the file will be copied
        E.g.: `user@server_ip:/http/notebook_publisher`
    """
    _NOTEBOOK_KEEPER_SRV = "http://notebook-keeper"
    _NOTEBOOK_KEEPER_ENTRY_POINT = f"{_NOTEBOOK_KEEPER_SRV}/save-file"
    # File copying.
    payload: dict = {"dst_path": remote_dst_path}
    files: List[Tuple[str, BinaryIO]] = [("file", open(local_src_path, "rb"))]
    response = requests.request(
        "POST", _NOTEBOOK_KEEPER_ENTRY_POINT, data=payload, files=files
    )
    _LOG.debug("Response: %s", response.text.encode("utf8"))


# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--file",
        action="store",
        required=True,
        type=str,
        help="The path to the ipynb file, a Jupyter URL, or a GitHub URL",
    )
    parser.add_argument(
        "--branch",
        action="store",
        type=str,
        help="The Git branch containing the notebook, if different than `master`",
    )
    parser.add_argument(
        "--publish_notebook_dir",
        action="store",
        type=str,
        default=None,
        help="Dir where to save the HTML file",
    )
    parser.add_argument(
        "--tag",
        action="store",
        default="",
        type=str,
        help="A tag that is added to the file (e.g., `RH1E_with_magic_parameters`)",
    )
    parser.add_argument(
        "--action",
        action="store",
        default=["convert"],
        choices=[
            "convert",
            "open",
            "publish_locally",
            "publish_on_s3",
            "publish_on_webserver",
        ],
        help="""
- convert (default): convert notebook to HTML in the current dir
- open: open an existing notebook on S3 it in the local browser
- publish_locally: publish notebook in a central local directory
- publish_on_s3: publish notebook on S3
- publish_on_webserver: publish notebook through a webservice
""",
    )
    parser = hs3.add_s3_args(parser)
    parser = prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level)
    if args.action == "open":
        # Open an existing HTML notebook.
        src_file_name = args.file
        if hs3.is_valid_s3_path(src_file_name):
            # We use AWS CLI to minimize the dependencies from Python packages.
            aws_profile = hs3.get_aws_profile(args.aws_profile)
            # Check that the file exists.
            cmd = f"aws s3 ls --profile {aws_profile} {src_file_name}"
            hsyste.system(cmd)
            # Copy.
            local_file_name = os.path.basename(src_file_name)
            cmd = f"aws s3 cp --profile {aws_profile} {src_file_name} {local_file_name}"
            hsyste.system(cmd)
            _LOG.info("Copied remote url to '%s'", local_file_name)
        else:
            local_file_name = src_file_name
        #
        opn.open_file(local_file_name)
        sys.exit(0)
    # Compute the path of the src file.
    if args.branch:
        src_file_name = _get_file_from_git_branch(args.branch, args.file)
    else:
        src_file_name = _get_path(args.file)
    # Process the action.
    if args.action == "convert":
        # Convert to HTML.
        dst_dir = "."
        html_file_name = _export_notebook_to_dir(src_file_name, args.tag, dst_dir)
        # Try to open.
        opn.open_file(html_file_name)
    elif args.action == "publish_locally":
        # Convert to HTML.
        if args.publish_notebook_dir is not None:
            dst_dir = args.publish_notebook_dir
        else:
            env_var = "AM_PUBLISH_NOTEBOOK_LOCAL_PATH"
            dbg.dassert_in(
                env_var, os.environ, "The env needs to set env var '%s'", env_var
            )
            dst_dir = os.environ[env_var]
        dbg.dassert_dir_exists(dst_dir)
        hio.create_dir(dst_dir, incremental=True)
        _export_notebook_to_dir(src_file_name, args.tag, dst_dir)
    elif args.action == "publish_on_s3":
        # Convert to HTML.
        dst_dir = "."
        html_file_name = _export_notebook_to_dir(src_file_name, args.tag, dst_dir)
        # Copy to S3.
        aws_profile = hs3.get_aws_profile(args.aws_profile)
        _LOG.debug("aws_profile='%s'", aws_profile)
        # Get the S3 path from command line.
        s3_path = args.s3_path
        _LOG.debug("s3_path=%s", s3_path)
        if s3_path is None:
            # The user didn't specified the path, so we derive it from the
            # credentials or from the env vars.
            _LOG.debug("Getting s3_path from credentials file")
            s3_path = hs3.get_key_value(aws_profile, "aws_s3_bucket")
        s3_path = "s3://" + s3_path + "/notebooks"
        s3_file_name = _post_to_s3(html_file_name, s3_path, aws_profile)
        # TODO(gp): Remove the file or save it directly in a temp dir.
        cmd = f"""
        # To open the notebook from S3 run:
        > publish_notebook.py --file {s3_file_name} --action open
        """
        print(hprint.dedent(cmd))
    elif args.action == "publish_on_webserver":
        remote_dst_path = os.path.basename(html_file_name)
        _post_to_webserver(html_file_name, remote_dst_path)
    else:
        dbg.dfatal(f"Invalid action='{args.action}'")


if __name__ == "__main__":
    _main(_parse())
