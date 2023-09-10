import os
import csv

from . import config

import backoff
import gspread
import requests
from google.oauth2.service_account import Credentials as ServiceAccountCredentials


@backoff.on_exception(backoff.expo, gspread.exceptions.APIError, max_time=60*30)
def list_spreadsheets(gc):
    return gc.list_spreadsheet_files()


@backoff.on_exception(backoff.expo, gspread.exceptions.APIError, max_time=60*30)
def get_all_sheet_values(gc, spreadsheet, sheet_name=None):
    if sheet_name:
        sheet = gc.open(spreadsheet['name']).worksheet(sheet_name)
    else:
        sheet = gc.open(spreadsheet['name']).sheet1
    return sheet.get_all_values()


def extract_google_sheets(log, only_table_name=None):
    log(f'Authorizing Google Sheets service account using delegation to {config.EXTRACT_DATA_USERNAME}...')
    gc = gspread.authorize(
        ServiceAccountCredentials.from_service_account_file(
            config.SERVICE_ACCOUNT_FILE, scopes=gspread.auth.READONLY_SCOPES
        ).with_subject(config.EXTRACT_DATA_USERNAME)
    )
    log('Fetching matching sheets...')
    matching_tables = {}
    spreadsheets = list_spreadsheets(gc)
    spreadsheets.sort(key=lambda x: x['createdTime'], reverse=True)
    extract_data_tables = {n: t for n, t in config.EXTRACT_DATA_TABLES.items() if t['type'] == "google_sheet"}
    for spreadsheet in spreadsheets:
        name = spreadsheet['name']
        for key, value in extract_data_tables.items():
            if value['google_sheet_name'].strip() == name.strip():
                matching_tables[key] = spreadsheet
    log(f'Found {len(matching_tables)} matching sheets')
    os.makedirs(config.EXTRACT_DATA_PATH, exist_ok=True)
    for table_name, spreadsheet in matching_tables.items():
        if only_table_name is None or only_table_name == table_name:
            data = get_all_sheet_values(gc, spreadsheet, extract_data_tables[table_name].get('tab_name'))
            with open(os.path.join(config.EXTRACT_DATA_PATH, f'{table_name}.csv'), 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerows(data)
            yield table_name


def extract_smoove_blocklist(log):
    log('Extracting smoove_blocklist...')
    res = requests.get(
        'https://rest.smoove.io/v1/Contacts_Blacklisted?fields=email',
        headers={'Authorization': f'Bearer {config.SMOOVE_API_KEY}'}
    )
    assert res.status_code == 200, f'unexpected status_code: {res.status_code} - {res.text}'
    with open(os.path.join(config.EXTRACT_DATA_PATH, f'smoove_blocklist.csv'), 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['email'])
        for contact in res.json():
            writer.writerow([contact['email']])
    yield 'smoove_blocklist'


def main(log, only_table_name=None):
    if only_table_name != 'smoove_blocklist':
        yield from extract_google_sheets(log, only_table_name=only_table_name)
    if not only_table_name or only_table_name == 'smoove_blocklist':
        yield from extract_smoove_blocklist(log)
