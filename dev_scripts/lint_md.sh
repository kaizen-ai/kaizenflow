#npm install --save-dev --save-exact prettier

prettier --parser markdown \
    --prose-wrap always \
    --write \
    --tab-width 2 \
    $*
