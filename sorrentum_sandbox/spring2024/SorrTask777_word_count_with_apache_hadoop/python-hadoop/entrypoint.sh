#!/bin/bash

# Set NLTK data directory explicitly
export NLTK_DATA=/usr/local/share/nltk_data

# Run the NLTK downloader to obtain necessary resources and suppress output
python3 -c "import nltk; nltk.download('punkt', quiet=True)"
python3 -c "import nltk; nltk.download('stopwords', quiet=True)"

# Run the mapper script with sample input and pipe the output to the reducer script
cat text_sample.txt | python3 mapper.py | sort | python3 reducer.py > word_count_output_unsorted.txt

# Sort the word count output by count in descending order and save to a file
sort -k2,2nr word_count_output_unsorted.txt > word_count_output_sorted.txt

# Display the final word count output
cat word_count_output_sorted.txt