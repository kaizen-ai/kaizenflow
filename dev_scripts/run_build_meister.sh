invoke git_clean -f
invoke git_pull

LOG_FILE="bm.log.txt"
rm -rf $LOG_FILE
#
invoke run_fast_tests $* 2>&1 | tee -a $LOG_FILE
if [[ $? != 0 ]]; then
    tg.py
fi;
#
invoke git_clean -f
invoke run_slow_tests $* 2>&1 | tee -a $LOG_FILE
if [[ $? != 0 ]]; then
    tg.py
fi;
#
invoke git_clean -f
invoke run_superslow_tests $* 2>&1 | tee -a $LOG_FILE
if [[ $? != 0 ]]; then
    tg.py
fi;

cmd="sudo -u spm-sasm rm ./tmp.pytest_repro.sh; i pytest_repro -f bm.log.txt"
echo "> $cmd"

if [[ -f ./tmp.pytest_repro.sh ]]; then
    sudo -u spm-sasm rm ./tmp.pytest_repro.sh
fi;
invoke pytest_repro -f bm.log.txt
