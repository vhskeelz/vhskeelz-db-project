#!/usr/bin/env bash

[ "${SERVICE_ACCOUNT_B64}" == "" ] && echo "SERVICE_ACCOUNT_B64 is not set" && exit 1
[ "${VHSKEELZ_DB_ARGS}" == "" ] && echo "VHSKEELZ_DB_ARGS is not set" && exit 1

echo "${SERVICE_ACCOUNT_B64}" | base64 -d > service_account.json &&\
gcloud auth activate-service-account --key-file=service_account.json &&\
bin/env_secret_restore.sh &&\
export SERVICE_ACCOUNT_FILE=service_account.json &&\
poetry install &&\
poetry run vhskeelz-db ${VHSKEELZ_DB_ARGS}
