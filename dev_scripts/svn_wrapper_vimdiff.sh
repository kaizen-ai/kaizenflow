#!/bin/bash
#diffprg='vimdiff'
diffprg='/usr/bin/vim -d'
fn_old=$6
fn_working_copy=$7
# arrange the args in the order diffprogram expects them
eval "$diffprg $fn_old $fn_working_copy"
exit 0
