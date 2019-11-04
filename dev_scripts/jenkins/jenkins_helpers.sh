CREATE_CONDA_PY="./dev_scripts/install/create_conda.py"
RUN_TESTS_PY="./dev_scripts/run_tests.py"
VERB="DEBUG"


function prepare_to_build_env() {
    # Activate conda base environment.
    CMD="conda activate base"
    execute $CMD

    ## Configure base environment.
    #echo "$EXEC_NAME: source $AMP/dev_scripts/setenv_amp.sh -e base"
    #source $AMP/dev_scripts/setenv.sh -e base

    # Print env.
    CMD="env"
    execute $CMD
}


function setenv() {
    CONDA_ENV=$1
    if [[ -z $CONDA_ENV ]]; then
        echo "ERROR: CONDA_ENV='$CONDA_ENV'"
        return 1
    fi;
    # Config environment.
    CMD="source dev_scripts/setenv_amp.sh -e $CONDA_ENV"
    execute $CMD

    # Check conda env.
    CMD="print_conda_packages.py --conda_env_name $CONDA_ENV"
    execute $CMD

    # Check packages.
    CMD="check_develop_packages.py"
    execute $CMD
}
