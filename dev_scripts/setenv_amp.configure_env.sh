# #############################################################################
# Configure environment
# #############################################################################

echo "# Configure env"
echo "which gh="$(which gh)

# Select which profile to use by default.
export AM_AWS_PROFILE="am"

# These variables are propagated to Docker.
export AM_ECR_BASE_PATH="665840871993.dkr.ecr.us-east-1.amazonaws.com"
export AM_S3_BUCKET="alphamatic-data"

# Print the AM env vars.
printenv | egrep "AM_|AWS_" | sort

# TODO(gp): Factor out the common code in `amp/dev_scripts/configure_env_amp.sh`
# and execute it from here. For now keep the code in sync manually.
alias i="invoke"
alias it="invoke traceback"
alias itpb="pbpaste | traceback_to_cfile.py -i - -o cfile"
alias ih="invoke --help"
alias il="invoke --list"

# Print the aliases.
alias

if [[ $(whoami) == "saggese" ]]; then
    # > ls -l ~/.ssh/*.github
    # .ssh/id_rsa.cryptomtc.github
    # .ssh/id_rsa.gpsaggese.github
    GIT_REMOTE=$(git remote -v)
    if [[ $GIT_REMOTE =~ "/cmamp" || $GIT_REMOTE =~ "/dev_tools" ]]; then
        export GIT_SSH_COMMAND="ssh -i ~/.ssh/ck/id_rsa.ck.github"
    elif [[ $GIT_REMOTE =~ "/amp" || $GIT_REMOTE =~ "/lemonade" ]]; then
        export GIT_SSH_COMMAND="ssh -i ~/.ssh/id_rsa.gpsaggese.github"
    else
        echo "Can't parse GIT_REMOTE='$GIT_REMOTE'"
        return -1
    fi;
    echo "GIT_SSH_COMMAND=$GIT_SSH_COMMAND"
fi;

