#!/bin/bash -e

DIR_NAME=$(jupyter --data-dir)/nbextensions
echo "DIR_NAME=$DIR_NAME"

echo
echo "Extensions are:"
jupyter nbextension list 2>/dev/null | grep enabled | awk '{print $1}' | sort | uniq
