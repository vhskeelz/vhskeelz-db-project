# vhskeelz-db-project

## Prerequisites

* [Python 3.10+](https://www.python.org/downloads/)
* [Poetry](https://python-poetry.org/docs/#installation)

## Install

```
poetry install
```

## ChromeDriver

Find you version of Chrome

```
google-chrome --version
```

Look for the nearest version of ChromeDriver

```
curl https://googlechromelabs.github.io/chrome-for-testing/known-good-versions-with-downloads.json | jq | grep 119.0.6045
```

Download the corresponding version of ChromeDriver

```
wget "https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/119.0.6045.59/linux64/chromedriver-linux64.zip"
unzip chromedriver-linux64.zip
sudo mv chromedriver-linux64/chromedriver /usr/local/bin/
rm -rf chromedriver-linux64*
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
