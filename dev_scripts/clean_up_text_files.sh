#!/bin/bash -xe

# Remove trailing spaces.
find . -name "*.py" -o -name "*.txt" -o -name "*.json" | xargs perl -pi -e 's/\s+$/\n/'

# Remove last line.
find . -name "*.py" -o -name "*.txt" -o -name "*.json" | xargs perl -pi -e 'chomp if eof'
