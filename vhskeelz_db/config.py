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
