#!/usr/bin/env bash

set -e

jupyter notebook --ip=* --port=${J_PORT} --allow-root
