#!/bin/bash
# Rename based on a regular expression.
# Usage example:
# ./dev_scripts/rename.sh TestReadDataFromDisk TestDiskDataSource core/dataflow/test/TestReadDataFromDisk*

for path in "${@:3}"
do
  new_path="${path//$1/$2}"
  mv "$path" "$new_path"
  echo "$path -> $new_path"
done
