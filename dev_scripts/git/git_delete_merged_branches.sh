#!/bin/bash -e

BRANCHES=$(git branch -a --merged |
    grep origin |
    grep -v '>' |
    grep -v master |
    xargs -L1 |
    cut -d"/" -f2-)
#xargs git push origin --delete
echo $BRANCHES

if [[ 1 == 1 ]]; then
    #for k in `git branch -r | perl -pe 's/^..(.*?)( ->.*)?$/\1/'`; do echo -e `git show --pretty=format:"%Cgreen%ci %Cblue%cr%Creset" $k -- | head -n 1`\\t$k; done | sort -r
    for k in $BRANCHES; do
        #git checkout $k
        echo -e `git show --pretty=format:"%Cgreen%ci %Cblue%cr%Creset" $k -- | head -n 1`\\t$k;
    done | sort -r
fi;
