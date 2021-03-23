#!/usr/bin/env bash

# Run tests.
pytest -m "not superslow and not slow and not broken_deps and not need_data_dir and not not_docker" \
    vendors_amp
