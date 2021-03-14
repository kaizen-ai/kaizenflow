#!/usr/bin/env bash

set -xe

echo "Install textblob dicts."

conda activate venv

python -m 'textblob.download_corpora'
