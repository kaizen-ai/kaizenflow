#!/bin/bash
if [[ $1 == "wrap" ]]; then
    cmd='vimdiff -c "windo set wrap"'
else
    cmd='vimdiff'
fi;
cmd="$cmd dataflow/core/nodes/test/outcomes/TestMultiindexVolatilityModel.test2/tmp.final.actual.txt dataflow/core/nodes/test/outcomes/TestMultiindexVolatilityModel.test2/tmp.final.expected.txt"
eval $cmd
