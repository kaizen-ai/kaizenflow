#!/bin/bash
if [[ $1 == "wrap" ]]; then
    cmd='vimdiff -c "windo set wrap"'
else
    cmd='vimdiff'
fi;
cmd="$cmd helpers/test/outcomes/Test_dassert_increasing_index.test_dassert_increasing_index_on_df2/tmp.final.actual.txt helpers/test/outcomes/Test_dassert_increasing_index.test_dassert_increasing_index_on_df2/tmp.final.expected.txt"
eval $cmd
