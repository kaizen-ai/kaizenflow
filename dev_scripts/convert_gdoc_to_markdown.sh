#!/bin/bash -xe
IN_FILE="/Users/saggese/Downloads/Tools\ -\ PyCharm.docx"
OUT_PREFIX="docs/Tools-PyCharm"
OUT_FILE="${OUT_PREFIX}.md"
OUT_FIGS="${OUT_PREFIX}_figs"

# Convert.
rm -rf $OUT_FIGS
pandoc --extract-media $OUT_FIGS -f docx -t markdown -o $OUT_FILE $IN_FILE

# Move the media.
mv $OUT_FIGS/{media/*,}
rm -rf $OUT_FIGS/media

# Clean up.
# \_ -> _
perl -pi -e "s/\\\_/_/g" $OUT_FILE

# "# \# Running PyCharm remotely" -> "# Running PyCharm remotely"
perl -pi -e "s/\\\#\+ //g" $OUT_FILE

# \`nid\` -> `nid`
perl -pi -e "s/\\\\\`(.*?)\\\\\`/'\1'/g" $OUT_FILE
