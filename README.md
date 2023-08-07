# vhskeelz-db-project

## Prerequisites

* [Python 3.10+](https://www.python.org/downloads/)
* [Poetry](https://python-poetry.org/docs/#installation)

## Install

```
poetry install
```

## Secrets

Secrets are stored in Google Secrets Manager, you need gcloud CLI installed and authenticated with relevant permissions.

Restore secrets:

```
bin/env_secret_restore.sh
```

Save updated secrets from local .env:

```
bin/env_secret_save.sh
```
