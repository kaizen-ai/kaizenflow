#!/bin/bash
if [[ $1 == "wrap" ]]; then
    cmd='vimdiff -c "windo set wrap"'
else
    cmd='vimdiff'
fi;
cmd="$cmd core/signal_processing/test/outcomes/Test_compress_tails.test13/tmp.final.actual.txt core/signal_processing/test/outcomes/Test_compress_tails.test13/tmp.final.expected.txt"
eval $cmd
