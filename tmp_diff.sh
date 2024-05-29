#!/bin/bash
if [[ $1 == "wrap" ]]; then
    cmd='vimdiff -c "windo set wrap"'
else
    cmd='vimdiff'
fi;
cmd="$cmd helpers/test/outcomes/TestCheckDataFrame1.test_check_df_not_equal4/tmp.final.actual.txt helpers/test/outcomes/TestCheckDataFrame1.test_check_df_not_equal4/tmp.final.expected.txt"
eval $cmd
