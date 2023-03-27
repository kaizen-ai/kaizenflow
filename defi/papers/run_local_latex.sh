#!/bin/bash -xe
GIT_ROOT="/Users/saggese/src/cmamp1"
(cd $GIT_ROOT/defi/papers; pdflatex daocross_daoswap.tex && pdflatex daocross_daoswap.tex)

#rm -f daocross_daoswap.pdf
/usr/bin/osascript << EOF
set theFile to POSIX file "./daocross_daoswap.pdf" as alias
tell application "Skim"
activate
set theDocs to get documents whose path is (get POSIX path of theFile)
if (count of theDocs) > 0 then revert theDocs
open theFile
end tell
EOF

