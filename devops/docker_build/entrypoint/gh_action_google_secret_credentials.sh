#!/usr/bin/env bash

set -e

if [ "$GOOGLE_SPREADSHEET_PRIVATE_KEY" ]; then
  mkdir -p ~/.config/gspread_pandas/
  # shellcheck disable=SC2059
  echo '{"type": "'"$GOOGLE_SPREADSHEET_TYPE"'",
  "project_id": "'"$GOOGLE_SPREADSHEET_PROJECT_ID"'",
  "private_key_id": "'"$GOOGLE_SPREADSHEET_PRIVATE_KEY_ID"'",
  "private_key": "'"$(echo "$GOOGLE_SPREADSHEET_PRIVATE_KEY" | xargs)"'",
  "client_email": "'"$GOOGLE_SPREADSHEET_CLIENT_EMAIL"'",
  "client_id": "'"$GOOGLE_SPREADSHEET_CLIENT_ID"'",
  "auth_uri": "'"$GOOGLE_SPREADSHEET_AUTH_URI"'",
  "token_uri": "'"$GOOGLE_SPREADSHEET_TOKEN_URI"'",
  "auth_provider_x509_cert_url": "'"$GOOGLE_SPREADSHEET_AUTH_PROVIDER_X509_CERT_URL"'",
  "client_x509_cert_url": "'"$GOOGLE_SPREADSHEET_CLIENT_X509_CERT_URL"'"}' >~/.config/gspread_pandas/google_secret.json
fi
