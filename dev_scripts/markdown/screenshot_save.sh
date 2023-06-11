#!/bin/bash
# Take a screenshot to the clipboard
# Save the image to a file in PNG format
# Print a short snippet to embed in Latex or Markdown.
#
# Usage: screenshot_save.sh [[path/]filename]
#
# If no filename is specified, the image will be saved to the present working
# directory and named using the current date and time. If more than one
# argument is given, all but the first argument are ignored.
#
# E.g.
# > screenshot_save.sh
# > screenshot_save.sh ~/Desktop
# > screenshot_save.sh ~/Desktop/foo.jpg
# > screenshot_save.sh /Users/saggese/src/git_gp1/notes/figs/game_theory/week4.pure_strategies_example.png

folder=$(pwd)
filename=fig-$(date +%Y-%m-%d-%H%M%S).png

if [ $# -ne 0 ]; then
    if [[ -d $1 ]]; then
        if [ "$1" != "." ]; then
            folder=$1
        fi
    else
        a=$(dirname "$1")
        b=$(basename "$1" .png)
        if [ "$b" != "" ]; then
            filename=$b.png
        fi
        if [ "$a" != "." ]; then
            folder=$a
        fi
    fi
fi
echo "filename=$filename"

if [[ 1 == 1 ]]; then
    screencapture -i -t png $filename
else
    # In practice it is
    # > screencapture -c -i -t png notes/figs/game_theory/week4.pure_strategies_example.png
    screencapture -c -i
    osascript -e "tell application \"System Events\" to ¬
                  write (the clipboard as PNG picture) to ¬
                  (make new file at folder \"$folder\" ¬
                  with properties {name:\"$filename\"})"
fi

# Copy reference for Latex.
val="
\$\$
\includegraphics[height=4cm]{$filename}
\$\$
"
echo $val

# Print reference for Markdown.
val="
![]($filename)
"
echo $val

# Copy last reference to clipboard.
echo $val | pbcopy
