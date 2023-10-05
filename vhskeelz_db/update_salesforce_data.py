import json
import time
import base64
import datetime

import jwt
import requests

from . import config, db


ACCOUNT_FIELDS = {
    "comp_city": {},
    "comp_country": {},
    "comp_createdByEmployee": {},
    "comp_createdByOrganizationId": {},
    "comp_dateCreated": {},
    "comp_dateDeleted": {},
    "comp_dateUpdated": {},
    "comp_description": {},
    "comp_emailSuffix": {},
    "comp_employeesCount": {},
    "comp_id": {},
    "comp_industry": {},
    "comp_name": {},
    "comp_region": {},
    "comp_status": {},
    "companyId": {},
}


def salesforce_login():
    assertion = jwt.encode(
        {
            "iss": config.SALESFORCE_CONSUMER_KEY,
            "sub": config.SALESFORCE_USERNAME,
            "aud": 'https://login.salesforce.com',
            "exp": int(time.time()) + 300,
        }, base64.b64decode(config.SALESFORCE_SERVER_KEY_B64.encode()).decode(), algorithm='RS256'
    )
    res = requests.post('https://login.salesforce.com/services/oauth2/token', data={
        'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
        'assertion': assertion,
    })
    assert res.status_code == 200, res.content
    res = res.json()
    return res['instance_url'], res['access_token']


def process_company(row, sf_url, sf_token, log):
    name = row.pop('comp_name')
    row['Company_id'] = row.pop('companyId')
    comp_id = row.pop('comp_id')
    try:
        res = requests.patch(
            f'{sf_url}/services/data/v58.0/sobjects/Account/comp_id__c/{comp_id}',
            headers={
                'Authorization': f'Bearer {sf_token}',
                'Content-Type': 'application/json',
            },
            json={
                "Type": "HR_Company",
                "Name": name,
                **{
                    f'{k}__c': v
                    for k, v in row.items()
                }
            }
        )
        if res.status_code == 200:
            log(f"comp_id {comp_id}: updated ({res.json()['id']})")
        elif res.status_code == 201:
            log(f"comp_id {comp_id}: created ({res.json()['id']})")
        else:
            raise Exception(f"Unexpected status_code: {res.status_code}\n{res.content}")
        return res.json()['id']
    except Exception as e:
        raise Exception(f'Failed to process comp_id: {comp_id}') from e


def process_contact(comp_id, ta_email, row, sf_url, sf_token, sf_account_id, log):
    try:
        first_name, last_name = row['ta_name'].split(' ', 1)
        res = requests.patch(
            f'{sf_url}/services/data/v58.0/sobjects/Contact/ta_comp_id_email_idx__c/{comp_id}:{ta_email}',
            headers={
                'Authorization': f'Bearer {sf_token}',
                'Content-Type': 'application/json',
            },
            json={
                "AccountId": sf_account_id,
                "FirstName": first_name,
                "LastName": last_name,
                "Email": ta_email,
                **{
                    f'{k}__c': v
                    for k, v in row.items()
                    if k not in ['comp_id']
                }
            }
        )
        if res.status_code == 200:
            log(f"contact {comp_id}:{ta_email} updated ({res.json()['id']})")
        elif res.status_code == 201:
            log(f"contact {comp_id}:{ta_email} created ({res.json()['id']})")
        else:
            raise Exception(f"Unexpected status_code: {res.status_code}\n{res.content}")
    except Exception as e:
        raise Exception(f'Failed to process contact: {comp_id}:{ta_email}') from e


def main(log, reprocess_contact=None):
    if reprocess_contact:
        reprocess_contact_comp_id, reprocess_contact_ta_email = reprocess_contact.split(':')
    sf_url, sf_token = salesforce_login()
    with db.get_db_engine().connect() as conn:
        with conn.begin():
            processed_companies_sf_ids = {}
            sql_fields = ', '.join(f'"{field}"' for field in ACCOUNT_FIELDS)
            for row in conn.execute(f'select {sql_fields} from vehadarta_company_and_company_ta'):
                row = {
                    k: '' if v in ('null', None) else str(v).strip()
                    for k, v in dict(row).items()
                }
                if reprocess_contact and row['comp_id'] != reprocess_contact_comp_id:
                    continue
                row = json.loads(json.dumps(row).replace('\\u202f', ' '))
                for k in ['comp_dateCreated', 'comp_dateDeleted', 'comp_dateUpdated']:
                    if row[k]:
                        row[k] = datetime.datetime.strptime(row[k], "%b %d, %Y, %I:%M:%S %p").strftime('%Y-%m-%dT%H:%M:%SZ')
                assert row['comp_id']
                comp_id = row['comp_id']
                if comp_id not in processed_companies_sf_ids:
                    processed_companies_sf_ids[comp_id] = process_company(row, sf_url, sf_token, log)
            processed_comp_emails = set()
            for row in conn.execute(f'''
                select
                    comp_id, "ta_createdByEmployee", "ta_createdByOrganizationId", "ta_dateCreated", "ta_dateUpdated",
                    ta_email, ta_id, ta_name, "ta_phoneNumber", "ta_roleId"
                from vehadarta_company_and_company_ta
            '''):
                row = {
                    k: '' if v in ('null', None) else str(v).strip()
                    for k, v in dict(row).items()
                }
                row = json.loads(json.dumps(row).replace('\\u202f', ' '))
                for k in ['ta_dateCreated', 'ta_dateUpdated']:
                    if row[k]:
                        row[k] = datetime.datetime.strptime(row[k], "%b %d, %Y, %I:%M:%S %p").strftime('%Y-%m-%dT%H:%M:%SZ')
                if row['comp_id'] and row['ta_email']:
                    if reprocess_contact and (reprocess_contact_comp_id != row['comp_id'] or reprocess_contact_ta_email != row['ta_email']):
                        continue
                    sf_account_id = processed_companies_sf_ids[row['comp_id']]
                    assert sf_account_id, f'invalid sf_account_id: {row}'
                    if (comp_id, row['ta_email']) in processed_comp_emails:
                        log(f'WARNING: duplicate ta_email: {row}')
                    else:
                        processed_comp_emails.add((comp_id, row['ta_email']))
                        process_contact(comp_id, row['ta_email'], row, sf_url, sf_token, sf_account_id, log)
