#!/usr/bin/env bash

echo "Install textblob dicts."

conda activate venv

python -m 'textblob.download_corpora'