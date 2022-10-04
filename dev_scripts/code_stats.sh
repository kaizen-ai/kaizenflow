#!/bin/bash
echo "# Numer of files in amp"
find . -name "*.py" | \grep amp | wc -l

echo "# Numer of files in super-repo"
find . -name "*.py" | \grep -v amp | wc -l

echo "# LOC in amp"
find . -name "*.py" | \grep amp | xargs cat | wc -l

echo "# LOC in super-repo"
find . -name "*.py" | \grep -v amp | xargs cat | wc -l
