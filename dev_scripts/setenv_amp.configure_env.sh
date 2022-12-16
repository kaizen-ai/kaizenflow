# #############################################################################
# Configure environment
# #############################################################################

echo "# Configure env"
echo "which gh="$(which gh)

# AWS profiles which are propagated to Docker.
export AM_AWS_PROFILE="am"
export CK_AWS_PROFILE="ck"

# These variables are propagated to Docker.
export AM_ECR_BASE_PATH="665840871993.dkr.ecr.us-east-1.amazonaws.com"
export AM_AWS_S3_BUCKET="alphamatic-data"
# TODO(gp): Difference between amp and cmamp.
export CK_ECR_BASE_PATH="623860924167.dkr.ecr.eu-north-1.amazonaws.com"
export CK_AWS_S3_BUCKET="cryptokaizen-data"

# Print the AM env vars.
printenv | egrep "AM_|AWS_" | sort

# Set up custom path to the alembic.ini file
#  https://alembic.sqlalchemy.org/en/latest/tutorial.html#editing-the-ini-file
export ALEMBIC_CONFIG="alembic/alembic.ini"

# TODO(gp): Factor out the common code in `amp/dev_scripts/configure_env_amp.sh`
# and execute it from here. For now keep the code in sync manually.
alias i="invoke"
alias it="invoke traceback"
alias itpb="pbpaste | traceback_to_cfile.py -i - -o cfile"
alias ih="invoke --help"
alias il="invoke --list"

# Print the aliases.
alias
