#!/usr/bin/env bash

# Collect without execution
pytest --co -vv -rpa \
	-m "not superslow and not slow and not broken_deps and not need_data_dir and not not_docker" \
	amp/vendors_amp/kibot
# Run tests
pytest -vv -rpa \
	-m "not superslow and not slow and not broken_deps and not need_data_dir and not not_docker" \
	amp/vendors_amp/kibot
