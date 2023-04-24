#!/bin/bash
if [[ $1 == "wrap" ]]; then
    cmd='vimdiff -c "windo set wrap"'
else
    cmd='vimdiff'
fi;
cmd="$cmd defi/dao_cross/test/outcomes/TestMatchOrders1.test3/tmp.final.actual.txt defi/dao_cross/test/outcomes/TestMatchOrders1.test3/tmp.final.expected.txt"
eval $cmd
