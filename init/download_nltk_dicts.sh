#!/usr/bin/env bash

echo "Install nltk dicts."

python -c "import nltk; \
         nltk.download('averaged_perceptron_tagger'); \
         nltk.download('punkt'); \
         nltk.download('stopwords'); \
         nltk.download('wordnet')"

echo "Done"