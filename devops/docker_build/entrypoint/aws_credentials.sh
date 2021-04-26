#!/usr/bin/env bash
#
# If ~/.aws exists loads the AWS credentials into the corresponding environment
# variables.
# 

set -e

AWS_VOLUME="${HOME}/.aws"
if [[ ! -d $AWS_VOLUME ]]; then
    echo "Can't find $AWS_VOLUME: exiting"
else
    INI_FILE=$AWS_VOLUME/credentials
    while IFS=' = ' read key value
    do
        if [[ $key == \[*] ]]; then
            section=$key
        elif [[ $value ]] && [[ $section == '[default]' ]]; then
            if [[ $key == 'aws_access_key_id' ]]; then
                echo "Assigning AWS_ACCESS_KEY_ID='$value'"
                export AWS_ACCESS_KEY_ID=$value
            elif [[ $key == 'aws_secret_access_key' ]]; then
                echo "Assigning AWS_SECRET_ACCESS_KEY='$value'"
                export AWS_SECRET_ACCESS_KEY=$value
            fi
        fi
    done < $INI_FILE
    # Update the password file.
    # TODO(gp): Change default00 -> alphamatic-data.
    echo $AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY > /etc/passwd-s3fs-default00-bucket
    chmod 600 /etc/passwd-s3fs-default00-bucket
fi;

# TODO(gp): Load also the AWS_DEFAULT_REGION from ~/.aws/config.
export AWS_DEFAULT_REGION="us-east-1"
echo "Assigning AWS_DEFAULT_REGION='$AWS_DEFAULT_REGION'"
