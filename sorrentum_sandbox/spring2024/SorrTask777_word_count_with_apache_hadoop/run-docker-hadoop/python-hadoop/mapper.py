#!/usr/bin/env python3
import sys
import string
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

# Download the English stop words if not already downloaded
import nltk
nltk.download('stopwords')

# Set of English stop words
stop_words = set(stopwords.words('english'))

# Translation table for removing punctuation
translator = str.maketrans("", "", string.punctuation)

print("\nStarting mapper \n", file=sys.stderr)
# Input comes from STDIN (standard input)
for line in sys.stdin:
    # Remove punctuation and convert to lowercase
    line = line.translate(translator).lower()
    # Tokenize the line into words
    words = word_tokenize(line)
    # Filter out stop words and emit each non-stop word with a count of 1
    for word in words:
        if word not in stop_words:
            print(f"{word}\t1")
