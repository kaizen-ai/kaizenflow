#!/bin/bash -xe

# Collect tests.

CMD="pytest --collect-only -qq -rs"
execute $CMD
