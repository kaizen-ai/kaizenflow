#!/usr/bin/env python

"""
Convert Docx file to Markdown.

Example:
Run this command in the same directory as the Markdown file:
> ../dev_scripts/convert_docx_to_markdown.py --docx_file Tools_Docker.docx --md_file Tools_Docker.md
"""

import argparse
import logging
import os
import shutil
import tempfile

import helpers.hdbg as hdbg
import helpers.hparser as hparser
import helpers.hsystem as hsystem

_LOG = logging.getLogger(__name__)


def _convert_docx_to_markdown(
    docx_file: str, md_file: str, md_file_figs: str
) -> None:
    """
    Convert Docx file to Markdown.

    :param docx_file: path to the Docx file
    :param md_file: path to the Markdown file
    """
    hdbg.dassert_file_exists(docx_file)
    # Create the Markdown file.
    hsystem.system(f"touch {md_file}")
    # Create temporary Dockerfile.
    docker_container_name = "convert_docx_to_markdown"
    with tempfile.NamedTemporaryFile(suffix=".Dockerfile") as temp_dockerfile:
        temp_dockerfile.write(
            b"""
                FROM ubuntu:latest

                RUN apt-get update && \
                    apt-get -y upgrade

                RUN apt-get install -y curl pandoc && \
                    apt-get clean && \
                    rm -rf /var/lib/apt/lists/*
            """
        )
        temp_dockerfile.flush()
        cmd = (
            f"docker build -f {temp_dockerfile.name} -t {docker_container_name} ."
        )
        hsystem.system(cmd)
    # Run Docker container.
    work_dir = os.getcwd()
    mount = f"type=bind,source={work_dir},target={work_dir}"
    remove_figs_folder_cmd = f"rm -rf {md_file_figs}"
    hsystem.system(remove_figs_folder_cmd)
    # Convert from Docx to Markdown.
    convert_docx_to_markdown_cmd = f"pandoc --extract-media {md_file_figs} -f docx -t markdown_strict -o {md_file} {docx_file}"
    docker_cmd = f"docker run --rm -it --workdir {work_dir} --mount {mount} {docker_container_name} {convert_docx_to_markdown_cmd}"
    hsystem.system(docker_cmd)


def _move_media(md_file_figs: str) -> None:
    """
    Move the media if it exists.
    """
    media_dir = os.path.join(md_file_figs, "media")
    if os.path.isdir(media_dir):
        # Move all the files in 'media' to 'md_file_figs'.
        for file_name in os.listdir(media_dir):
            file_path = os.path.join(media_dir, file_name)
            shutil.move(file_path, md_file_figs)
        # Remove the 'media' directory.
        shutil.rmtree(media_dir)
    else:
        _LOG.info("No media directory found.")


def _clean_up_artifacts(md_file: str, md_file_figs: str) -> None:
    """
    Remove the artifacts.

    :param md_file: path to the Markdown file
    :param md_file_figs: path to the folder containing the artifacts
    """
    perl_regex_replacements = [
        # # \# Running PyCharm remotely -> # Running PyCharm remotely.
        r"perl -pi -e 's:# (\\#)+ :# :g' {}".format(md_file),
        # \#\# Docker image"  -> ## Docker image.
        r"perl -pi -e 's:\\#:#:g' {}".format(md_file),
        # **## amp / cmamp container** -> ## amp / cmamp container.
        r"perl -pi -e 's:\*\*#(.*?)\*\*:#$1:g' {}".format(md_file),
        # -  Typically instructions include information about which packages and
        #    > their versions to install, e.g. list of python packages and their
        #    > corresponding versions
        r"perl -pi -e 's:^(\s+)> :$1:g' {}".format(md_file),
        # >
        # > botocore==1.24.37
        # >
        r"perl -pi -e 's:^>: :g' {}".format(md_file),
        # Remove the \ before - $ | " _ [ ].
        r"perl -pi -e 's:\\([-\$|\"\_\]\[\.]):$1:g' {}".format(md_file),
        # \' -> '.
        r'perl -pi -e "s:\\\':\':g" {}'.format(md_file),
        # \` -> `.
        r"perl -pi -e 's:\\\`:\`:g' {}".format(md_file),
        # \* -> *.
        r"perl -pi -e 's:\\\*:\*:g' {}".format(md_file),
        # “ -> ".
        r"perl -pi -e 's:“:\":g' {}".format(md_file),
        # ” -> ".
        r"perl -pi -e 's:”:\":g' {}".format(md_file),
        # Remove trailing \.
        r"perl -pi -e 's:\\$::g' {}".format(md_file),
        # Remove ========= and --------.
        r"perl -pi -e 's:======+::g' {}".format(md_file),
        r"perl -pi -e 's:------+::g' {}".format(md_file),
        # Translate HTML elements.
        r"perl -pi -e 's:\&gt;:\>:g' {}".format(md_file),
        r"perl -pi -e 's:\<\!\-\-.*\-\-\>::g' {}".format(md_file),
        # Fix image links.
        r"perl -pi -e 's:{}/media/:{}/:g' {}".format(
            md_file_figs, md_file_figs, md_file
        ),
    ]
    # Run the commands.
    for clean_cmd in perl_regex_replacements:
        hsystem.system(clean_cmd, suppress_output=False)


# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--docx_file",
        action="store",
        required=True,
        type=str,
        help="The Docx file that needs to be converted to Markdown",
    )
    parser.add_argument(
        "--md_file",
        action="store",
        required=True,
        type=str,
        help="The output Markdown file",
    )
    hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    docx_file = args.docx_file
    md_file = args.md_file
    # The folder for the figures.
    md_file_figs = md_file.replace(".md", "_figs")
    _LOG.info("Start converting Docx to Markdown.")
    _convert_docx_to_markdown(docx_file, md_file, md_file_figs)
    _move_media(md_file_figs)
    _clean_up_artifacts(md_file, md_file_figs)
    _LOG.info("Finished converting '%s' to '%s'.", docx_file, md_file)


if __name__ == "__main__":
    _main(_parse())
