#!/usr/bin/env python 3
from operator import itemgetter
import sys

current_word = None
current_count = 0
word = None

print("\nStarting reducer \n", file=sys.stderr)
# Input comes from STDIN (standard input)
for line in sys.stdin:
    # Parse the input we got from mapper.py
    word, count = line.strip().split('\t', 1)
    # Convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # Count was not a number, so silently
        # ignore/discard this line
        continue

    # This assumes the input is sorted by key (word)
    if current_word == word:
        current_count += count
    else:
        if current_word:
            # Emit the count for the current word
            print(f"{current_word}\t{current_count}")
        current_count = count
        current_word = word

# Output the last word if needed
if current_word == word:
    print(f"{current_word}\t{current_count}")
