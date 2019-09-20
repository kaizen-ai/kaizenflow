#!/bin/bash -e

# ```
# Create a tarball of all the files added / modified.
# ```

source helpers.sh

FILES=$(git_files.sh)
DATETIME=$(date "+%Y%m%d-%H%M%S")
REPO_NAME=$(basename `git rev-parse --show-toplevel`)
BACKUP_FILE=~/src/backups/backup.$REPO_NAME.$DATETIME.tgz
echo "backup_file=$BACKUP_FILE"
tar czf $BACKUP_FILE $FILES
echo "Done"

echo
ls -l $BACKUP_FILE
tar tzf $BACKUP_FILE
