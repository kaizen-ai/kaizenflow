#!/bin/bash
res=$(find . -path ./\.git -prune -o -type f -print | wc -l)
echo "Number of files in repo: $res"

res=$(find . -name "*.py" | wc -l)
echo "Number of Python files in repo: $res"

res=$(find . -name "*.ipynb" | wc -l)
echo "Number of notebooks files in repo: $res"

res=$(find . -name "*.py" | xargs cat | wc -l)
echo "Python in repo: $res"

res=$(find . -name "*.sh" | xargs cat | wc -l)
echo "Shell in repo: $res"

res=$(find . -name "*.yaml" | xargs cat | wc -l)
echo "Yaml in repo: $res"

res=$(find . -name "*.md" | xargs cat | wc -l)
echo "Markdown in repo: $res"

res=$(find . -name "*.tex" | xargs cat | wc -l)
echo "Latex in repo: $res"

res=$(find . -name "*.py" -o -name "*.sh" -o -name "*.yaml" -o -name "*.md" -o -name "*.tex" | xargs cat | wc)
echo $res

res=$(jackpy "def test" | wc -l)
echo "Number of tests: $res"
