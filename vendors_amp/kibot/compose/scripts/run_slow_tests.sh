#!/usr/bin/env bash

# Collect without execution
pytest --co -vv -rpa  -m "slow and not superslow and not broken_deps and not need_data_dir and not not_docker"
# Run tests
pytest -vv -rpa  -m "slow and not superslow and not broken_deps and not need_data_dir and not not_docker"
