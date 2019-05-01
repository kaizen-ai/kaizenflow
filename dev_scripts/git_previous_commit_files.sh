if [[ -z $1 ]]; then
  num_lines=1
else
  num_lines=$1
fi;
#echo "num_lines=$num_lines"
git show --pretty="" --name-only $(git log --author $(git config user.name) -$num_lines | \grep commit | perl -pe 's/commit (.*)/$1/')
