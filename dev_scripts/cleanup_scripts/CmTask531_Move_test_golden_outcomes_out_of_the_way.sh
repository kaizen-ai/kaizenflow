#!/bin/bash -xe

# CmTask531 - Move golden outcomes from `test` dirs to `test/outcomes` dirs.
./dev_scripts/replace_text.py \
  --action "rename" \
  --filter_by "/test/Test.+/" \
  --filter_on "dirname" \
  --old "/test/" \
  --new "/test/outcomes/" \
  --ext None \
