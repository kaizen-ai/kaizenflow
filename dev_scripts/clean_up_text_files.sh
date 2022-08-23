#!/bin/bash -xe

find . -name "*.py" -o -name "*.txt" -o -name "*.json" | xargs perl -pi -e 's/\s+$/\n/'
find . -name "*.txt" | xargs perl -pi -e 'chomp if eof'
