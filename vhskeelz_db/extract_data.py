import os
import csv

from . import config

import gspread
from google.oauth2.service_account import Credentials as ServiceAccountCredentials


def main(only_table_name=None):
    print(f'Authorizing Google Sheets service account using delegation to {config.EXTRACT_DATA_USERNAME}...')
    gc = gspread.authorize(
        ServiceAccountCredentials.from_service_account_file(
            config.SERVICE_ACCOUNT_FILE, scopes=gspread.auth.READONLY_SCOPES
        ).with_subject(config.EXTRACT_DATA_USERNAME)
    )
    print('Fetching matching sheets...')
    matching_tables = {}
    spreadsheets = gc.list_spreadsheet_files()
    spreadsheets.sort(key=lambda x: x['createdTime'], reverse=True)
    extract_data_tables = {n: t for n, t in config.EXTRACT_DATA_TABLES.items() if t['type'] == "google_sheet"}
    for spreadsheet in spreadsheets:
        name = spreadsheet['name']
        for key, value in extract_data_tables.items():
            if value['google_sheet_name'].strip() == name.strip():
                matching_tables[key] = spreadsheet
                break
        if len(matching_tables) == len(extract_data_tables):
            break
    print(f'Found {len(matching_tables)} matching sheets')
    os.makedirs(config.EXTRACT_DATA_PATH, exist_ok=True)
    for table_name, spreadsheet in matching_tables.items():
        if only_table_name is None or only_table_name == table_name:
            if extract_data_tables[table_name].get('tab_name'):
                sheet = gc.open(spreadsheet['name']).worksheet(extract_data_tables[table_name]['tab_name'])
            else:
                sheet = gc.open(spreadsheet['name']).sheet1
            data = sheet.get_all_values()
            with open(os.path.join(config.EXTRACT_DATA_PATH, f'{table_name}.csv'), 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerows(data)
            yield table_name