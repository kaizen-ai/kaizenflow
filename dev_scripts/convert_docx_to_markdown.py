#!/usr/bin/env python

"""
Convert docx file to markdown.

Usage:
> dev_scripts/convert_docx_to_markdown.py --docx_file <docx_file> --md_file <md_file>

Example:
> dev_scripts/convert_docx_to_markdown.py --docx_file docs/Tools_Docker.docx --md_file docs/@Tools_Docker_Test.md

"""

import argparse
import logging
import tempfile
import os
import logging
import shutil

import helpers.hdbg as hdbg
import helpers.hparser as hparser
import helpers.hsystem as hsystem

_LOG = logging.getLogger(__name__)


def _move_media(md_file_figs: str) -> None:
    """
    Move the media if it exists.
    """
    if os.path.isdir(os.path.join(md_file_figs, "media")):
        # Move all the files in 'media' to 'out_figs'
        for file_name in os.listdir(os.path.join(md_file_figs, "media")):
            shutil.move(os.path.join(
                md_file_figs, "media", file_name), md_file_figs)
        # Remove the 'media' directory
        shutil.rmtree(os.path.join(md_file_figs, "media"))


def _clean_up_artifacts(md_file: str, md_file_figs: str) -> None:
    """
    Remove the artifacts.

    :param md_file_figs: path to the folder containing the artifacts
    """
    perl_regex_replacements = [
        
        # "# \# Running PyCharm remotely" -> "# Running PyCharm remotely"
        r"perl -pi -e 's:# (\\#)+ :# :g' {}".format(md_file),
        # \#\# Docker image  -> ## Docker image
        r"perl -pi -e 's:\\#:#:g' {}".format(md_file),
        # **## amp / cmamp container**
        r"perl -pi -e 's:\*\*#(.*?)\*\*:#$1:g' {}".format(md_file),
        # -  Typically instructions include information about which packages and
        #    > their versions to install, e.g. list of python packages and their
        #    > corresponding versions
        r"perl -pi -e 's:^(\s+)> :$1:g' {}".format(md_file),
        #
        # >
        # > botocore==1.24.37
        # >
        r"perl -pi -e 's:^>: :g' {}" .format(md_file),
        # Remove the \ before - $ | " _ [ ]. 
        r"perl -pi -e 's:\\([-\$|\"\_\]\[\.]):$1:g' {}" .format(md_file),
        # \' -> '
        r'perl -pi -e "s:\\\':\':g" {}'.format(md_file),
        # \` -> `
        r"perl -pi -e 's:\\\`:\`:g' {}".format(md_file),
        # \* -> *
        r"perl -pi -e 's:\\\*:\*:g' {}".format(md_file),
        # “ -> "
        r"perl -pi -e 's:“:\":g' {}".format(md_file),
        # ” -> "
        r"perl -pi -e 's:”:\":g' {}".format(md_file),
        # Remove trailing \
        r"perl -pi -e 's:\\$::g' {}" .format(md_file),
        # Remove ========= and -------.
        r"perl -pi -e 's:======+::g' {}" .format(md_file),
        r"perl -pi -e 's:------+::g' {}" .format(md_file),
        # Translate HTML elements.
        r"perl -pi -e 's:\&gt;:\>:g' {}" .format(md_file),
        r"perl -pi -e 's:\<\!\-\-.*\-\-\>::g' {}".format(md_file),
        r"perl -pi -e 's:{}/media/:{}/:g' {}" .format(md_file_figs, md_file_figs, md_file),
    ]
    for clean_cmd in perl_regex_replacements:
        hsystem.system(clean_cmd)


def _convert_docx_to_markdown(docx_file: str, md_file: str) -> None:
    """
    Convert docx file to markdown.

    :param docx_file: path to the docx file
    :param md_file: path to the markdown file
    """
    hdbg.dassert_file_exists(docx_file)
    # create the markdown file.
    hsystem.system(f"touch {md_file}")
    # Create temporary Dockerfile.
    md_file_figs = md_file.replace(".md", "_figs")
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
        cmd = f"docker build -f {temp_dockerfile.name} -t {docker_container_name} ."
        hsystem.system(cmd)
    # Run Docker container.
    work_dir = os.getcwd()
    mount = f"type=bind,source={work_dir},target={work_dir}"
    # Convert from docx to Markdown.
    remove_figs_folder_cmd = f"rm -rf {md_file_figs}"
    hsystem.system(remove_figs_folder_cmd)
    convert_docx_to_markdown_cmd = f"pandoc --extract-media {md_file_figs} -f docx -t markdown_strict -o {md_file} {docx_file}"
    docker_cmd = f"docker run --rm -it --workdir {work_dir} --mount {mount} {docker_container_name} {convert_docx_to_markdown_cmd}"
    _LOG.info("Start converting docx to markdown.")
    hsystem.system(docker_cmd)
    _LOG.info("Finished converting '%s' to '%s'.", docx_file, md_file)
    _LOG.info("Md figs '%s'", md_file_figs)
    _move_media(md_file_figs)
    _clean_up_artifacts(md_file, md_file_figs)

# #############################################################################


def add_download_args(parser: argparse.ArgumentParser,) -> argparse.ArgumentParser:
    parser.add_argument(
        "--docx_file",
        action="store",
        required=True,
        type=str,
        help="The docx file that needs to be converted to markdown",
    )
    parser.add_argument(
        "--md_file",
        action="store",
        required=True,
        type=str,
        help="The converted markdown file",
    )
    return parser


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser = add_download_args(parser)
    parser = hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    _convert_docx_to_markdown(args.docx_file, args.md_file)


if __name__ == "__main__":
    _main(_parse())
