#!/bin/bash -e

# In order to get the link generated correctly we need to run gh-md-toc in the
# dir with the markdown.
cd documentation/notes

FILES=$(ls *.md)
echo "files=$FILES"

for f in $FILES; do
    echo "======================================================="
    echo $f
    echo "======================================================="
    # Ignore failure.
    ../scripts/gh-md-toc --insert $f || true
    # Remove some artifacts when copying from gdoc.
    perl -i -pe "s/’/'/;" $f
    perl -i -pe 's/“/"/;' $f
    perl -i -pe 's/”/"/;' $f
    perl -i -pe 's/…/.../;' $f
    # Remind that we use 4 spaces indent like in python.
    perl -i -pe 's/^  \-/    -/;' $f
done

# Clean up.
#git clean -fd
