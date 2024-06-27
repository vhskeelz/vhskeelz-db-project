import os
import csv
import time
import tempfile
import traceback
import contextlib
from urllib3.exceptions import HTTPError

import backoff
import gspread
import requests
from requests.exceptions import RequestException
from google.oauth2.service_account import Credentials as ServiceAccountCredentials

from . import config, download_position_candidate_cv


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


def extract_google_sheets(log, only_table_name=None, cache=None):
    if cache is not None and cache.get('extract_data_google_sheets'):
        matching_tables, gc, extract_data_tables = cache['extract_data_google_sheets']
    else:
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
        if cache is not None:
            cache['extract_data_google_sheets'] = matching_tables, gc, extract_data_tables
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


def download_post_streaming(url, target_filename, **kwargs):
    with requests.post(url, stream=True, **kwargs) as res:
        res.raise_for_status()
        with open(target_filename, 'wb') as file:
            for chunk in res.iter_content(chunk_size=8192):
                file.write(chunk)


def check_cookies(cookies):
    cookie_names = [c['name'] for c in cookies if c['domain'] == 'skeelz.retrain.ai']
    return 'id_token' in cookie_names and 'access_token' in cookie_names and '__rtr_sofi' in cookie_names and '__rtr_state' in cookie_names


@contextlib.contextmanager
def get_extract_skeelz_exports_context(cache, needs_skeelz_export=True):
    if needs_skeelz_export:
        if 'extract_skeelz_exports_context' in cache:
            yield cache['extract_skeelz_exports_context']
        else:
            headless = False
            proxy_server = None
            set_trace = False
            with tempfile.TemporaryDirectory() as download_path:
                with download_position_candidate_cv.driver_contextmanager(download_path, headless, proxy_server, set_trace) as driver:
                    cache['extract_skeelz_exports_context'] = download_path, driver
                    yield cache['extract_skeelz_exports_context']
    else:
        cache['extract_skeelz_exports_context'] = None
        yield cache['extract_skeelz_exports_context']


def get_skeelz_export_cookies(log):
    with get_extract_skeelz_exports_context({}, needs_skeelz_export=True) as (download_path, driver):
        driver.get(config.CANDIDATE_POSITION_CV_URL_TEMPLATE.format(position_id='', candidate_id=''))
        download_position_candidate_cv.login(log, driver)
        for i in range(20):
            time.sleep(1)
            if check_cookies(driver.get_cookies()):
                break
        cookies = driver.get_cookies()
    assert check_cookies(cookies)
    return cookies


@backoff.on_exception(backoff.constant, (RequestException, HTTPError), max_tries=5, interval=2)
def extract_skeelz_export_download(log, url, target_filename):
    cookies = get_skeelz_export_cookies(log)
    download_post_streaming(url, target_filename, cookies={c['name']: c['value'] for c in cookies})


def extract_skeelz_exports(log, only_table_name=None, cache=None):
    log('Extracting skeelz_exports...')
    for table_name, table_config in config.EXTRACT_DATA_TABLES.items():
        if table_config['type'] != 'skeelz_export':
            continue
        if only_table_name and only_table_name != table_name:
            continue
        target_filename = os.path.join(config.EXTRACT_DATA_PATH, f'{table_name}.csv')
        log(f'Downloading {table_name} to {target_filename}')
        try:
            extract_skeelz_export_download(log, table_config['url'], target_filename)
            yield table_name
        except:
            if table_config.get('on_failure') == 'skip':
                log(f'Failed to download {table_name}, but on_failure is set to "skip", so skipping...\n{traceback.format_exc()}')
            else:
                raise


def main(log, only_table_name=None, cache=None, only_table_types=None):
    os.makedirs(config.EXTRACT_DATA_PATH, exist_ok=True)
    if only_table_types and isinstance(only_table_types, str):
        only_table_types = [t.strip() for t in only_table_types.split(',') if t.strip()]
    if not only_table_types or 'google_sheet' in only_table_types:
        if not only_table_name or only_table_name in [n for n, t in config.EXTRACT_DATA_TABLES.items() if t['type'] == 'google_sheet']:
            yield from extract_google_sheets(log, only_table_name=only_table_name, cache=cache)
    smoove_blocklist_table_names = [n for n, t in config.EXTRACT_DATA_TABLES.items() if t['type'] == 'smoove_blocklist']
    assert len(smoove_blocklist_table_names) <= 1, f'Only one smoove_blocklist table is allowed'
    if not only_table_types or 'smoove_blocklist' in only_table_types:
        if not only_table_name or only_table_name in smoove_blocklist_table_names:
            yield from extract_smoove_blocklist(log)
    if not only_table_types or 'skeelz_export' in only_table_types:
        if not only_table_name or only_table_name in [n for n, t in config.EXTRACT_DATA_TABLES.items() if t['type'] == 'skeelz_export']:
            yield from extract_skeelz_exports(log, only_table_name=only_table_name, cache=cache)
