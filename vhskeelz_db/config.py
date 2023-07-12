import os

import dotenv

dotenv.load_dotenv()


ROOT_DIR = os.environ.get('ROOT_DIR') or os.path.join(os.path.dirname(__file__), "..")
DATA_DIR = os.environ.get("DATA_DIR") or os.path.join(ROOT_DIR, ".data")

EXTRACT_DATA_USERNAME = os.environ.get('EXTRACT_DATA_USERNAME')
EXTRACT_DATA_PASSWORD = os.environ.get('EXTRACT_DATA_PASSWORD')
EXTRACT_DATA_REPORTS = [line.strip() for line in (os.environ.get('EXTRACT_DATA_REPORTS') or '').splitlines() if line.strip()]
EXTRACT_DATA_PATH = os.path.join(DATA_DIR, 'extract_data')
