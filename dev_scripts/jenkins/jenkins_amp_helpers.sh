# """
# Contains helpers for Jenkins scripts in amp.
# """

AMP="."


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
