#!/bin/bash -e

# In order to get the link generated correctly we need to run gh-md-toc in the
# dir with the markdown.
cd documentation/notes

FILES=$(ls *.md)

../scripts/gh-md-toc --insert $FILES

# Clean up.
#git clean -fd
