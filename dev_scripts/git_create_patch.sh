#!/bin/bash -e
#
# Create a patch file for the entire repo client from the base revision.
# This script accepts a list of files to package, if specified.
#
echo "Creating patch ..."
#hash_=$(svn info | \grep Revision | awk '{print $2}')
hash_=$(git rev-parse --short HEAD)
timestamp=$(timestamp)
git_root=$(git rev-parse --show-toplevel)
basename=$(basename $git_root)
echo "git_root=$git_root"
cd $git_root
dst_file="$git_root/patch.$basename.$hash_.$timestamp.txt"
echo "dst_file=$dst_file"
git st -s $*
# Find all files modified (instead of --cached).
git diff HEAD $* >$dst_file

echo
echo "To apply the patch do git checkout to the correct revision and execute:"
echo "> git apply "$dst_file

# Remote patch.
echo
echo "Remote patch:"
file=$(basename $dst_file)
#
server="server"
client_path="~/src/"
echo "> scp $file $server: && ssh $server 'cd $client_path && git apply ~/$file'"
