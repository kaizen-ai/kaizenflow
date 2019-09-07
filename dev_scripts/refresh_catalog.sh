#!/bin/bash -e
cd dev_scripts
(unset CLICOLOR; \ls -1 | grep -v "*.log$" | grep -v "test" | grep -v __init__ .py | grep -v __pycache__ | sort | perl -pe 's/^/\n# /') >_catalog.md
