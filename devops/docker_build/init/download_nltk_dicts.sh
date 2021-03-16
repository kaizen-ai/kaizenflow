#!/usr/bin/env bash

set -e

echo "Install nltk dicts."

conda activate venv

python -c "import nltk; \
         nltk.download('averaged_perceptron_tagger'); \
         nltk.download('punkt'); \
         nltk.download('stopwords'); \
         nltk.download('wordnet')"

echo "Done"
