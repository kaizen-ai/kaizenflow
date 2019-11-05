# """
# Contain helpers for any Jenkins script from amp or any other repo based on
# amp.
# """

VERB="DEBUG"


function prepare_to_build_env() {
    frame "prepare_to_build_env()"
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
    frame "sentenv($*)"
    #
    SETENV_EXEC=$1
    shift
    if [[ -z $SETENV_EXEC ]]; then
        echo "ERROR: SETENV_EXEC='$SETENV_EXEC'"
        return 1
    fi;
    CONDA_ENV=$*
    if [[ -z $CONDA_ENV ]]; then
        echo "ERROR: CONDA_ENV='$CONDA_ENV'"
        return 1
    fi;
    # Config environment.
    CMD="source $AMP/dev_scripts/$SETENV_EXEC -e $CONDA_ENV"
    execute $CMD

    # Check conda env.
    CMD="$AMP/dev_scripts/install/print_conda_packages.py --conda_env_name $CONDA_ENV"
    execute $CMD

    # Check packages.
    CMD="$AMP/dev_scripts/install/check_develop_packages.py"
    execute $CMD
}


function create_conda() {
    # Create conda.
    frame "create_conda($*)"
    #
    AMP=$1
    shift
    if [[ -z $AMP ]]; then
        echo "ERROR: AMP='$AMP'"
        return 1
    fi;
    OPTS=$*
    if [[ -z $OPTS ]]; then
        echo "ERROR: OPTS='$OPTS'"
        return 1
    fi;
    #
    CREATE_CONDA_PY="$AMP/dev_scripts/install/create_conda.py"
    CMD="$CREATE_CONDA_PY $OPTS"
    execute $CMD
}


function run_tests() {
    # Run tests.
    frame "run_tests($*)"
    #
    AMP=$1
    shift
    if [[ -z $AMP ]]; then
        echo "ERROR: AMP='$AMP'"
        return 1
    fi;
    OPTS=$*
    if [[ -z $OPTS ]]; then
        echo "ERROR: OPTS='$OPTS'"
        return 1
    fi;
    #
    RUN_TESTS_PY="$AMP/dev_scripts/run_tests.py"
    OPTS=" $OPTS --jenkins"
    CMD="$RUN_TESTS_PY $OPTS"
    execute $CMD
}
