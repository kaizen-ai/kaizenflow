# """
# Contains helpers for Jenkins scripts in amp.
# """

AMP="."

CREATE_CONDA_OPTS="--req_file dev_scripts/install/requirements/amp_develop.yaml --delete_env_if_exists"

function source_scripts() {
    CMD="source ~/.bashrc"
    echo "+ $CMD"
    eval $CMD

    CMD="source $AMP/dev_scripts/helpers.sh"
    echo "+ $CMD"
    eval $CMD

    CMD="source $AMP/dev_scripts/jenkins/jenkins_helpers.sh"
    echo "+ $CMD"
    eval $CMD
}
