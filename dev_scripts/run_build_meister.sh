invoke git_clean -f
invoke git_pull
invoke run_fast_slow_superslow_tests 2>&1 | tee bm.log.txt
