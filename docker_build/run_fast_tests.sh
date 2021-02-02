#!/usr/bin/env bash

conda list
# Collect without execution
pytest --co -vv -rpa -m "not superslow and not slow and not broken_deps and not need_data_dir and not not_docker"
# Run tests
pytest --log-cli-level=ERROR -vv -rpa -m "not superslow and not slow and not broken_deps and not need_data_dir and not not_docker"
