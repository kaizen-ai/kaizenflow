#!/bin/bash
AWS_VOLUME="${HOME}/.aws/"
GSPREAD_PANDAS_VOLUME="${HOME}/.config/gspread_pandas/"

test_aws() {
  local _aws_cred_file="${AWS_VOLUME}credentials"
  local _aws_conf_file="${AWS_VOLUME}config"

  if [ ! -e "$_aws_cred_file" ]; then
    echo -e """
AWS credential setup failure.
\e[93mCan't find $_aws_cred_file file.\e[0m
Check your setup.
Instruction:
https://github.com/ParticleDev/commodity_research/blob/master/documentation_p1/technical/aws_personal_token.md"""

  fi

  if [ ! -e "$_aws_conf_file" ]; then
    echo -e """
AWS credential setup failure.
\e[93mCan't find $_aws_conf_file file.\e[0m
Check your setup.
Instruction:
https://github.com/ParticleDev/commodity_research/blob/master/documentation_p1/technical/aws_personal_token.md"""
  fi
}

test_gspread_pandas() {
  local _google_secret_file="${GSPREAD_PANDAS_VOLUME}google_secret.json"
  local _google_cred_file="${GSPREAD_PANDAS_VOLUME}creds/default"

  if [ ! -e "$_aws_cred_file" ]; then
    echo -e """
Google API credential setup failure.
\e[93mCan't find $_google_secret_file file.\e[0m
Check your setup.
Instruction:
https://github.com/alphamatic/amp/blob/master/documentation/technical/gsheet_into_pandas.md"""
  fi
  if [ ! -e "$_google_cred_file" ]; then
    echo -e """
Google API credential setup failure.
\e[93mCan't find $_google_cred_file file.\e[0m
Check your setup.
Instruction:
https://github.com/alphamatic/amp/blob/master/documentation/technical/gsheet_into_pandas.md"""
  fi

}

test_aws
test_gspread_pandas
