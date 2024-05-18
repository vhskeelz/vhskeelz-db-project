import os
import time
import json
import shutil
import datetime
import tempfile
import traceback
from contextlib import contextmanager


import google.cloud.storage
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options


from . import config


DOWNLOAD_DIRECTORY = os.path.join(config.DATA_DIR, 'download_position_candidate_cv')
SELENIUM_DEBUG_DUMPS_DIRECTORY = os.path.join(DOWNLOAD_DIRECTORY, 'selenium_debug_dumps')


def get_driver(download_path, headless, proxy_server):
    assert not headless, 'sorry, headless is not supported for printing'
    chrome_options = Options()
    print_settings = {
        "recentDestinations": [{
            "id": "Save as PDF",
            "origin": "local",
            "account": "",
        }],
        "selectedDestinationId": "Save as PDF",
        "version": 2,
        "isHeaderFooterEnabled": False,
        "isLandscapeEnabled": True
    }
    os.makedirs(download_path, exist_ok=True)
    # chrome_options.add_argument(f"user-data-dir={user_data_path}")
    chrome_options.add_argument('--enable-print-browser')
    chrome_options.add_experimental_option("prefs", {
        "printing.print_preview_sticky_settings.appState": json.dumps(print_settings),
        "savefile.default_directory": download_path,  # Change default directory for downloads
        "download.default_directory": download_path,  # Change default directory for downloads
        "download.prompt_for_download": False,  # To auto download the file
        "download.directory_upgrade": True,
        "profile.default_content_setting_values.automatic_downloads": 1,
        "safebrowsing.enabled": True
    })
    chrome_options.add_argument("--kiosk-printing")
    # chrome_options.add_argument("window-size=1200x1600")
    if headless:
        chrome_options.headless = True
    if proxy_server:
        print(f'Using proxy server: {proxy_server}')
        chrome_options.add_argument(f'--proxy-server={proxy_server}')
    chrome_options.set_capability("goog:loggingPrefs", {"performance": "ALL", "browser": "ALL"})
    return webdriver.Chrome(options=chrome_options)


@contextmanager
def driver_contextmanager(download_path, headless, proxy_server, set_trace, slow_network=False):
    driver = get_driver(download_path, headless, proxy_server)
    driver.execute_cdp_cmd('Network.enable', {})
    if slow_network:
        driver.execute_cdp_cmd('Network.emulateNetworkConditions', {
            'offline': False,
            'latency': 600,  # ms
            #                     kbps
            'downloadThroughput': 50000 * 1024 / 8,
            'uploadThroughput': 50000 * 1024 / 8
        })
    try:
        yield driver
    except Exception:
        traceback.print_exc()
        if set_trace:
            import pdb
            pdb.set_trace()
        debug_dump(driver, basename='exception')
        exit(1)
    finally:
        driver.quit()


def debug_dump(driver, basename='debug_dump'):
    os.makedirs(SELENIUM_DEBUG_DUMPS_DIRECTORY, exist_ok=True)
    prefix = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    screenshot_filename = os.path.join(SELENIUM_DEBUG_DUMPS_DIRECTORY, f"{prefix}_{basename}.png")
    pagedump_filename = os.path.join(SELENIUM_DEBUG_DUMPS_DIRECTORY, f"{prefix}_{basename}.html")
    driver.save_screenshot(screenshot_filename)
    with open(pagedump_filename, "w") as f:
        f.write(driver.page_source)
    print(f'browser screenshot and html saved to {screenshot_filename} and {pagedump_filename}')


def login(log, driver):
    log("Start Login")
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//h2[contains(., 'לכניסת מגייסים')]"))).click()
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//button[contains(., 'הרשאת כניסה')]"))).click()
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, 'identifier'))).send_keys(config.SKEELZ_USERNAME)
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, 'credentials.passcode'))).send_keys(config.SKEELZ_PASSWORD)
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='submit']"))).click()
    log("Login OK")


def upload_to_gcs(source_file_name, destination_blob_name):
    storage_client = google.cloud.storage.Client.from_service_account_json(config.SERVICE_ACCOUNT_FILE)
    bucket = storage_client.bucket(config.GCS_BUCKET_NAME)
    blob = bucket.blob(destination_blob_name)
    with open(source_file_name, "rb") as f:
        blob.upload_from_file(f)


def check_gcs_blob_exists(destination_blob_name):
    storage_client = google.cloud.storage.Client.from_service_account_json(config.SERVICE_ACCOUNT_FILE)
    bucket = storage_client.bucket(config.GCS_BUCKET_NAME)
    blob = bucket.blob(destination_blob_name)
    return blob.exists()


def download_from_gcs(source_blob_name, destination_file_name):
    storage_client = google.cloud.storage.Client.from_service_account_json(config.SERVICE_ACCOUNT_FILE)
    bucket = storage_client.bucket(config.GCS_BUCKET_NAME)
    blob = bucket.blob(source_blob_name)
    if blob.exists():
        blob.download_to_filename(destination_file_name)
        return True
    else:
        return False


def download(log, driver, download_path, position_id, candidate_id, save_to_gcs=False):
    elt = WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.XPATH, "//p[contains(., 'ייצוא פרטים')]")))
    time.sleep(2)
    elt.click()
    WebDriverWait(driver, 20).until(lambda _: any(filename.endswith('.pdf') for filename in os.listdir(download_path)))
    filenames = [filename for filename in os.listdir(download_path) if filename.endswith('.pdf')]
    assert len(filenames) == 1
    filename = filenames[0]
    log(f"Downloaded CV filename: {filename}")
    os.makedirs(DOWNLOAD_DIRECTORY, exist_ok=True)
    target_filename = os.path.join(DOWNLOAD_DIRECTORY, f'{position_id}_{candidate_id}.pdf')
    shutil.move(os.path.join(download_path, filename), target_filename)
    log(f'CV downloaded to {target_filename}')
    if save_to_gcs:
        upload_to_gcs(target_filename, f'cv/{position_id}_{candidate_id}.pdf')
        log(f'CV uploaded to GCS: cv/{position_id}_{candidate_id}.pdf')
    return filename


def main_multi(log, position_candidate_ids, headless=False, proxy_server=None, set_trace=False, save_to_gcs=False, force=False, skip_download_errors=False):
    with tempfile.TemporaryDirectory() as download_path:
        with driver_contextmanager(download_path, headless, proxy_server, set_trace) as driver:
            is_loggedin = False
            for position_id, candidate_id in position_candidate_ids:
                if not force and check_gcs_blob_exists(f'cv/{position_id}_{candidate_id}.pdf'):
                    log(f'CV already exists in GCS: cv/{position_id}_{candidate_id}.pdf')
                else:
                    url = config.CANDIDATE_POSITION_CV_URL_TEMPLATE.format(position_id=position_id, candidate_id=candidate_id)
                    log(f"Downloading CV from URL {url}")
                    driver.get(url)
                    if not is_loggedin:
                        login(log, driver)
                        is_loggedin = True
                    try:
                        download(log, driver, download_path, position_id, candidate_id, save_to_gcs)
                    except:
                        if skip_download_errors:
                            log(f"{traceback.format_exc()}\nError downloading position_id: {position_id} candidate_id: {candidate_id}")
                        else:
                            raise


def main(log, position_id, candidate_id, headless=False, proxy_server=None, set_trace=False, save_to_gcs=False, force=False, slow_network=False):
    if not force and check_gcs_blob_exists(f'cv/{position_id}_{candidate_id}.pdf'):
        log(f'CV already exists in GCS: cv/{position_id}_{candidate_id}.pdf')
    else:
        with tempfile.TemporaryDirectory() as download_path:
            log(f"downloading position_id: {position_id} candidate_id: {candidate_id}")
            with driver_contextmanager(download_path, headless, proxy_server, set_trace, slow_network=slow_network) as driver:
                url = config.CANDIDATE_POSITION_CV_URL_TEMPLATE.format(position_id=position_id, candidate_id=candidate_id)
                log(f"Downloading CV from URL {url}")
                driver.get(url)
                login(log, driver)
                download(log, driver, download_path, position_id, candidate_id, save_to_gcs)
