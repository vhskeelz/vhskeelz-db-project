import os
import json

import dotenv

dotenv.load_dotenv()


ROOT_DIR = os.environ.get('ROOT_DIR') or os.path.join(os.path.dirname(__file__), "..")
DATA_DIR = os.environ.get("DATA_DIR") or os.path.join(ROOT_DIR, ".data")

EXTRACT_DATA_USERNAME = os.environ.get('EXTRACT_DATA_USERNAME')
EXTRACT_DATA_PASSWORD = os.environ.get('EXTRACT_DATA_PASSWORD')
EXTRACT_DATA_TABLES = json.loads(os.environ.get('EXTRACT_DATA_TABLES', '{}'))
EXTRACT_DATA_PATH = os.path.join(DATA_DIR, 'extract_data')

SERVICE_ACCOUNT_FILE = os.environ.get('SERVICE_ACCOUNT_FILE')

PGSQL_USER = os.environ.get('PGSQL_USER', 'postgres')
PGSQL_PASSWORD = os.environ.get('PGSQL_PASSWORD', '123456')
PGSQL_HOST = os.environ.get('PGSQL_HOST', 'localhost')
PGSQL_PORT = os.environ.get('PGSQL_PORT', '5432')
PGSQL_DB = os.environ.get('PGSQL_DB', 'postgres')

PROCESSING_RECORD_ENABLED = os.environ.get('PROCESSING_RECORD_ENABLED') == 'true'
PROCESSING_RECORD_ID = os.environ.get('PROCESSING_RECORD_ID')
PROCESSING_RECORD_NAME = os.environ.get('PROCESSING_RECORD_NAME')

SMOOVE_API_KEY = os.environ.get('SMOOVE_API_KEY')
SENDGRID_API_KEY = os.environ.get('SENDGRID_API_KEY')
SENDGRID_UNSUSCRIBE_GROUP_ID = os.environ.get('SENDGRID_UNSUSCRIBE_GROUP_ID')

CANDIDATE_POSITION_CV_URL_TEMPLATE = os.environ.get('CANDIDATE_POSITION_CV_URL_TEMPLATE')
SKEELZ_USERNAME = os.environ.get('SKEELZ_USERNAME')
SKEELZ_PASSWORD = os.environ.get('SKEELZ_PASSWORD')

CANDIDATE_OFFERS_MAILING_CONFIG = json.loads(os.environ.get('CANDIDATE_OFFERS_MAILING_CONFIG', '{}'))
POSITION_DETAILS_URL_TEMPLATE = os.environ.get('POSITION_DETAILS_URL_TEMPLATE')

GCS_BUCKET_NAME = os.environ.get('GCS_BUCKET_NAME')

SALESFORCE_USERNAME = os.environ.get('SALESFORCE_USERNAME')
# SALESFORCE_PASSWORD = os.environ.get('SALESFORCE_PASSWORD')
# SALESFORCE_TOKEN = os.environ.get('SALESFORCE_TOKEN')
SALESFORCE_CONSUMER_KEY = os.environ.get('SALESFORCE_CONSUMER_KEY')
SALESFORCE_DOMAIN = os.environ.get('SALESFORCE_DOMAIN')
SALESFORCE_SERVER_KEY_B64 = os.environ.get('SALESFORCE_SERVER_KEY_B64')
