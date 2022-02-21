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
