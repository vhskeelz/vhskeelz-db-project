#!/usr/bin/env bash

echo Saving .env to secrets manager... &&\
gcloud --project skeelz-retrain-api secrets versions add vhskeelz-db-env --data-file=.env
for VERSION in $(gcloud --project skeelz-retrain-api secrets versions list vhskeelz-db-env --filter="state=enabled" --format="value(name)" | tail -n+2); do
  gcloud --project skeelz-retrain-api secrets versions destroy $VERSION --secret=vhskeelz-db-env --quiet
done &&\
echo OK
