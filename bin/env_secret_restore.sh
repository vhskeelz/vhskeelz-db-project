#!/usr/bin/env bash

echo Restoring .env from secrets manager... &&\
if [ -f .env ]; then cp .env .env.bak; fi &&\
gcloud --project skeelz-retrain-api secrets versions access \
  "$(gcloud --project skeelz-retrain-api secrets versions list vhskeelz-db-env --filter="state=enabled" --format="value(name)" | head -n1)" \
  --secret=vhskeelz-db-env \
    > .env &&\
echo OK
