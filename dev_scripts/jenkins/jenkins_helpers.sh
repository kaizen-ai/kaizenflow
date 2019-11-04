function build_env() {
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
    # Config environment.
    CMD="source dev_scripts/setenv.sh -e $CONDA_ENV"
    source dev_scripts/setenv.sh -e $CONDA_ENV

    # Check conda env.
    CMD="print_conda_packages.py"
    execute $CMD

    # Check 
    CMD="check_develop_packages.py"
    execute $CMD
}
