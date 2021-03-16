#!/usr/bin/env bash

set -e

echo "Install textblob dicts."

conda activate venv

python -m 'textblob.download_corpora'
