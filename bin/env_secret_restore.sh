#!/usr/bin/env bash

echo Restoring .env from secrets manager... &&\
cp .env .env.bak &&\
gcloud --project skeelz-retrain-api secrets versions access \
  "$(gcloud --project skeelz-retrain-api secrets versions list vhskeelz-db-env --filter="state=enabled" --format="value(name)" | head -n1)" \
  --secret=vhskeelz-db-env \
    > .env &&\
echo OK
