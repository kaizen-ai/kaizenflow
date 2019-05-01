#!/bin/bash

git fetch

echo "# master - branch"
git ll ..origin/master

echo
echo "# branch - master"
git ll origin/master..
