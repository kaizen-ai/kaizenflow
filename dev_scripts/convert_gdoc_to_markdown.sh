#!/bin/bash -xe

set -eux

# Build the Docker container.
TMP_FILENAME="/tmp/tmp._convert_docx_to_markdown.Dockerfile"
cat >$TMP_FILENAME <<EOF
FROM ubuntu:latest

RUN apt-get update && \
    apt-get -y upgrade

RUN apt-get install -y curl pandoc && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

EOF

export DOCKER_CONTAINER_NAME="sorrentum_docx_to_md"
docker build -f $TMP_FILENAME -t $DOCKER_CONTAINER_NAME .

IN_FILE="docs/Epics_and_Sprints.docx"
OUT_PREFIX="docs/Sprint_planning_process"
OUT_FILE="${OUT_PREFIX}.md"
OUT_FIGS="${OUT_PREFIX}_figs"

WORKDIR="$(realpath .)"
MOUNT="type=bind,source=${WORKDIR},target=${WORKDIR}"

#git checkout -- $OUT_FILE

# Convert from docx to Markdown.
if [[ 0 == 0 ]]; then
    rm -rf $OUT_FIGS
    CMD="pandoc --extract-media $OUT_FIGS -f docx -t markdown -o $OUT_FILE $IN_FILE"
    docker run --rm -it --workdir "${WORKDIR}" --mount "${MOUNT}" ${DOCKER_CONTAINER_NAME} ${CMD}

    # # Move the media.
    # mv $OUT_FIGS/{media/*,}
    # rm -rf $OUT_FIGS/media
fi;

# Clean up artifacts.

# - Same as VNC, but instead of sending bitmaps through VNC, a
#   > \"compressed\" version of the GUI is sent to the local
#   > computer directly
perl -pi -e 's/^(\s+)> /\1/g' $OUT_FILE

# **\# Connecting via VNC**
perl -pi -e 's/^\*\*\\#+ /**/g' $OUT_FILE

# Remove the \ before - $ | < > " _ @ ) [ ].
perl -pi -e 's/\\([-\$|<>"\_\@\)\]\[\.])/\1/g' $OUT_FILE

# Let\'s -> Let's
perl -pi -e "s/\\\'/'/g" $OUT_FILE

# Remove trailing \
perl -pi -e 's/\\$//g' $OUT_FILE

# \# -> #
perl -pi -e "s/\\#/\#/g" $OUT_FILE

# "# \# Running PyCharm remotely" -> "# Running PyCharm remotely"
perl -pi -e 's/# \\#+ /# /g' $OUT_FILE

# \`nid\` -> `nid`
perl -pi -e "s/\\\\\`(.*?)\\\\\`/\`\1\`/g" $OUT_FILE

# Fix the links.
perl -pi -e "s|$OUT_FIGS/media|$OUT_FIGS|g" $OUT_FILE

# [[https://plugins.jetbrains.com/plugin/7234-wrap-to-column]{.underline}](https://plugins.jetbrains.com/plugin/7234-wrap-to-column)
perl -pi -e 's/\[\[(._)\](\{\.underline\})?]\((._)\)/[\1](\3)/g' $OUT_FILE

# Remove:
#   ```{=html}
#   <!-- -->
#   ```
#perl -pi -e 's/\s*```{=html}\n\s*<!-- -->\n\s*```/mg' $OUT_FILE

# Create a Python script without no substitution.
SCRIPT_NAME="/tmp/replace.py"
cat <<< '#! /usr/bin/env python

import re
import sys

filename = sys.argv[1]
# Read the entire file.
with open(filename, "r") as file:
    lines = file.readlines()
lines = "".join(lines)

regex = r"^\s*```{=html}\n\s*<!-- -->\n\s*```\n"

subst = ""

# You can manually specify the number of replacements by changing the 4th argument
lines = re.sub(regex, subst, lines, 0, re.MULTILINE)

# Write the modified content back to the file
with open(filename, "w") as file:
    file.write(lines)
' >$SCRIPT_NAME
chmod +x $SCRIPT_NAME
$SCRIPT_NAME $OUT_FILE

dev_scripts/lint_md.sh $OUT_FILE

gd $OUT_FILE

# Clean up temporary files.
rm $TMP_FILENAME