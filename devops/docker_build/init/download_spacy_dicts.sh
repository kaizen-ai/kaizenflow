#!/usr/bin/env bash

echo "Install spacy dicts."

conda activate venv

python -m spacy download en_core_web_sm

echo "Done"
