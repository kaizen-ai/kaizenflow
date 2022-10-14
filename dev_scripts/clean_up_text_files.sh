#!/bin/bash -xe

# https://stackoverflow.com/questions/36043061/automatically-add-newline-on-save-in-pycharm

# > tail -3 helpers/hplayback.py | xxd
# 00000000: 2320 2020 2020 7072 696e 7428 636f 6465  #     print(code
# 00000010: 290a 2320 2020 2020 7265 7475 726e 2072  ).#     return r
# 00000020: 6573 0a23 2060 6060 0a                   es.# ```.

# 0a = \n

echo "Remove trailing spaces in each line ..."
find . -name "*.py" -o -name "*.txt" -o -name "*.json" | xargs perl -pi -e 's/\s+$/\n/'

echo "Remove last line ..."
find . -name "*.py" -o -name "*.txt" -o -name "*.json" | xargs perl -pi -e 'chomp if eof'
