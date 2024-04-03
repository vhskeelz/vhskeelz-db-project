import contextlib
import json
import time
import base64
import hashlib

import jwt
import requests

from . import config, db

CANDIDATE_CONTACT_SF_FIELDS = {
    "AccountId": {'lambda': lambda row: config.SALESFORCE_CANDIDATES_ACCOUNT_ID},
    "FirstName": {'db_field': 'first_name'},
    "LastName": {'db_field': 'last_name', 'required': True, 'default': '-'},
    "Email": {'db_field': 'email', 'required': True},
    "Candidate_id__c": {'db_field': 'candidate_id', 'required': True},
    "Gender__c": {'db_field': 'gender', 'lambda': lambda row: row['gender'] if row['gender'] in ('Male', 'Female') else ''},
    "City_c__c": {'db_field': "location"},
    "MobilePhone": {'db_field': 'phone_number'},
}

COMPANY_ACCOUNT_SF_FIELDS = {
    "Name": {'db_field': 'company_name', 'required': True},
    "comp_id__c": {'db_field': 'companyId', 'required': True},
    "Company_id__c": {'db_field': 'companyId', 'required': True},
    # "comp_city__c": {},
    # "comp_country__c": {},
    # "comp_createdByEmployee__c": {},
    # "comp_createdByOrganizationId__c": {},
    # "comp_dateCreated__c": {},
    # "comp_dateDeleted__c": {},
    # "comp_dateUpdated__c": {},
    # "comp_description__c": {},
    # "comp_emailSuffix__c": {},
    # "comp_employeesCount__c": {},
    # "comp_industry__c": {},
    # "comp_region__c": {},
    # "comp_status__c": {},
}

POSITION_CASE_SF_FIELDS = {
    "City__c": {'skeelz_db_field': 'City', 'db_field': 'city'},
    "Description": {'skeelz_db_field': 'Position description', 'db_field': 'position_description'},
    "Subject": {'skeelz_db_field': 'Position name', 'db_field': 'position_name'},
    # "actice__c": {'skeelz_db_field': '', 'db_field': 'status'},
    "employmentType__c": {'skeelz_db_field': 'Employment type', 'db_field': 'employmentType'},
    "hiringUser__c": {'skeelz_db_field': 'Hiring user', 'db_field': 'hiringUser'},
    "positionType__c": {'skeelz_db_field': 'Position type', 'db_field': 'positionType'},
    "Position_id__c": {'skeelz_db_field': 'Position id', 'db_field': 'position_id', 'required': True},
    'RecordTypeId': {'lambda': lambda row: config.SALESFORCE_CASE_RECORD_TYPE_ID}
}


class SalesforceException(Exception):

    def __init__(self, msg, res_text=None):
        super().__init__(msg)
        self.res_text = res_text if res_text else msg

    def parse_res_text(self):
        try:
            return json.loads(self.res_text)
        except:
            pass
        return None


def create_table(conn):
    with conn.begin():
        conn.execute('''
            create table if not exists salesforce_objects (
                object_type varchar(255),
                salesforce_id varchar(255),
                vhskeelz_id varchar(255),
                created_at timestamp,
                updated_at timestamp,
                data_hash varchar(64)
            );
            create index if not exists salesforce_objects_object_type_idx on salesforce_objects (object_type);
            create index if not exists salesforce_objects_salesforce_id_idx on salesforce_objects (salesforce_id);
            create index if not exists salesforce_objects_vhskeelz_id_idx on salesforce_objects (vhskeelz_id);
        ''')


def get_vhskeelz_ids_salesforce_ids(conn, object_type):
    return {
        row['vhskeelz_id']: (row['salesforce_id'], row['data_hash'])
        for row in conn.execute(f"select salesforce_id, vhskeelz_id, data_hash from salesforce_objects where object_type = '{object_type}'")
    }


def update_vhskeelz_id_sf_id(sql_execute, object_type, vhskeelz_id, sf_id, vhskeelz_ids_salesforce_ids, data_hash):
    if vhskeelz_id not in vhskeelz_ids_salesforce_ids:
        sql_execute(f"""
            insert into salesforce_objects (object_type, salesforce_id, vhskeelz_id, created_at, updated_at, data_hash)
            values ('{object_type}', '{sf_id}', '{vhskeelz_id}', now(), now(), '{data_hash}');
        """)
    else:
        assert sf_id == vhskeelz_ids_salesforce_ids[vhskeelz_id][0]
        sql_execute(f"""
            update salesforce_objects set updated_at = now(), data_hash = '{data_hash}' where object_type = '{object_type}' and salesforce_id = '{sf_id}' and vhskeelz_id = '{vhskeelz_id}';
        """)


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


def get_data_hash(data):
    return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()


def upsert_object(object_name, key_spec, data, sf_url, sf_token, existing_sf_id, existing_data_hash, log):
    data_hash = get_data_hash(data)
    if data_hash == existing_data_hash:
        return 'unchanged', existing_sf_id, existing_data_hash
    else:
        res = requests.patch(
            f'{sf_url}/services/data/v58.0/sobjects/{object_name}/{key_spec}',
            headers={'Authorization': f'Bearer {sf_token}', 'Content-Type': 'application/json', },
            json=data
        )
        if res.status_code == 200:
            return 'updated', res.json()['id'], data_hash
        elif res.status_code == 201:
            return 'created', res.json()['id'], data_hash
        elif res.status_code == 400:
            error_codes = [e['errorCode'] for e in res.json()]
            if error_codes == ['ENTITY_IS_DELETED']:
                log(f'WARNING: entity is deleted in SF, cannot update: {object_name}/{key_spec}')
                return 'updated', existing_sf_id, data_hash
            else:
                raise Exception(f"Unexpected 400 error: {res.content}")
        else:
            raise Exception(f"Unexpected status_code: {res.status_code}\n{res.content}")


def remove_object(object_name, key_spec, sf_url, sf_token):
    requests.delete(
        f'{sf_url}/services/data/v58.0/sobjects/{object_name}/{key_spec}',
        headers={'Authorization': f'Bearer {sf_token}', 'Content-Type': 'application/json', },
    ).raise_for_status()


def create_object(object_name, data, sf_url, sf_token):
    res = requests.post(
        f'{sf_url}/services/data/v60.0/sobjects/{object_name}/',
        headers={'Authorization': f'Bearer {sf_token}', 'Content-Type': 'application/json', },
        json=data
    )
    try:
        assert res.json()['success']
        return res.json()['id'], get_data_hash(data)
    except Exception as e:
        raise SalesforceException(res.text) from e


def soql_query(soql, sf_url, sf_token):
    res = requests.get(
        f'{sf_url}/services/data/v60.0/query',
        params={'q': soql},
        headers={'Authorization': f'Bearer {sf_token}', 'Content-Type': 'application/json', }
    )
    try:
        res.raise_for_status()
        return res.json()['records']
    except Exception as e:
        raise Exception(f'{res.text}\n\nsoql=\n{soql}') from e


def update_candidate_contacts(conn, sf_url, sf_token, log):
    with conn.begin():
        candidate_ids_sf_ids = get_vhskeelz_ids_salesforce_ids(conn, 'candidate_contact')
        rows = list(conn.execute('''
            select
                "Email" email, "Candidate first name" first_name, "Candidate last name" last_name,
                "Candidate id" candidate_id, "Gender" gender, "Candidate location" location,
                "Phone number" phone_number
            from skeelz_export_candidates
        '''))
    with db.conn_transaction_sql_handler(conn) as sql_execute:
        for row in rows:
            row, contact_data = preprocess_row(row, CANDIDATE_CONTACT_SF_FIELDS, log)
            candidate_id = row['candidate_id']
            try:
                if candidate_id in candidate_ids_sf_ids:
                    sf_id = candidate_ids_sf_ids[candidate_id][0]
                else:
                    log(f'candidate_id {candidate_id}: searching for related contacts in Salesforce...')
                    existing_contacts = list(soql_query("SELECT Id FROM Contact WHERE Candidate_id__c='{candidate_id}'", sf_url, sf_token))
                    assert len(existing_contacts) <= 1, f'Too many existing contacts for candidate_id {candidate_id}: {existing_contacts}'
                    sf_id = existing_contacts[0]['Id'] if len(existing_contacts) > 0 else None
                    if sf_id:
                        candidate_ids_sf_ids[candidate_id] = (sf_id, None)
                if not sf_id:
                    sf_id, data_hash = create_object('Contact', contact_data, sf_url, sf_token)
                    action = 'created'
                else:
                    action, sf_id, data_hash = upsert_object('Contact', f'Id/{sf_id}', contact_data, sf_url, sf_token, *candidate_ids_sf_ids[candidate_id], log)
                update_vhskeelz_id_sf_id(sql_execute, 'candidate_contact', candidate_id, sf_id, candidate_ids_sf_ids, data_hash)
                log(f'candidate_id {row["candidate_id"]}: {action} ({sf_id})')
            except Exception as e:
                ok = False
                if isinstance(e, SalesforceException):
                    res = e.parse_res_text()
                    if res and isinstance(res, list) and len(res) == 1 and isinstance(res[0], dict):
                        if res[0].get('errorCode') == 'INVALID_EMAIL_ADDRESS':
                            ok = True
                            log(f'WARNING failed to update candidate_id {candidate_id} invalid email: {row["email"]}')
                if not ok:
                    raise Exception(f'Failed to update candidate_id {candidate_id}') from e


def update_position_cases(conn, sf_url, sf_token, log):
    sql_fields = ','.join([
        f'"{conf["skeelz_db_field"]}" "{conf["db_field"]}"'
        for conf in POSITION_CASE_SF_FIELDS.values()
        if conf.get('db_field') and conf.get('skeelz_db_field')
    ])
    with conn.begin():
        position_ids_sf_ids = get_vhskeelz_ids_salesforce_ids(conn, 'position_case')
        rows = list(conn.execute(f'SELECT {sql_fields} FROM skeelz_export_positions'))
    with db.conn_transaction_sql_handler(conn) as sql_execute:
        for row in rows:
            row, case_data = preprocess_row(row, POSITION_CASE_SF_FIELDS, log)
            position_id = row['position_id']
            try:
                if position_id in position_ids_sf_ids:
                    sf_id = position_ids_sf_ids[position_id][0]
                else:
                    log(f'position_id {position_id}: searching for related cases in Salesforce...')
                    existing_cases = list(soql_query(f"SELECT Id FROM Case WHERE Position_id__c='{position_id}'", sf_url, sf_token))
                    assert len(existing_cases) <= 1, f'Too many existing cases for position_id {position_id}: {existing_cases}'
                    if len(existing_cases):
                        sf_id = existing_cases[0]['Id']
                    else:
                        sf_id = None
                    if sf_id:
                        position_ids_sf_ids[position_id] = (sf_id, None)
                if not sf_id:
                    sf_id, data_hash = create_object('Case', case_data, sf_url, sf_token)
                    action = 'created'
                else:
                    action, sf_id, data_hash = upsert_object('Case', f'Id/{sf_id}', case_data, sf_url, sf_token, *position_ids_sf_ids[position_id], log)
                update_vhskeelz_id_sf_id(sql_execute, 'position_case', position_id, sf_id, position_ids_sf_ids, data_hash)
                log(f'position_id {position_id}: {action} ({sf_id})')
            except Exception as e:
                raise Exception(f'Failed to update position_id {position_id}') from e


def update_company_accounts(conn, sf_url, sf_token, log):
    with conn.begin():
        company_ids_sf_ids = get_vhskeelz_ids_salesforce_ids(conn, 'company_account')
        rows = list(conn.execute('''
            WITH RankedItems AS (
                SELECT "Company id" "companyId", "Company name" company_name, ROW_NUMBER() OVER(PARTITION BY "Company id") AS rn FROM skeelz_export_positions
            ) SELECT "companyId", company_name FROM RankedItems WHERE rn = 1 and company_name != 'null';
        '''))
    with db.conn_transaction_sql_handler(conn) as sql_execute:
        for row in rows:
            row, account_data = preprocess_row(row, COMPANY_ACCOUNT_SF_FIELDS, log)
            company_name, company_id = row['company_name'], row['companyId']
            try:
                if company_id in company_ids_sf_ids:
                    sf_id = company_ids_sf_ids[company_id][0]
                else:
                    log(f'company_id {company_id}: searching for related accounts in Salesforce...')
                    existing_sf_ids = []
                    for search_field, search_value in [
                        ('Company_id__c', company_id),
                        ('comp_id__c', company_id),
                        ('Name', company_name),
                    ]:
                        vhskeelz_api_cmt_account_id = None
                        for account in soql_query('''
                            SELECT Id, vhskeelz_api_cmt__c FROM Account WHERE {search_field}='{search_value}'
                        '''.format(
                            search_field=search_field,
                            search_value=search_value.replace("'", "\\'")
                        ), sf_url, sf_token):
                            if account['vhskeelz_api_cmt__c'] == '{"managed_by_api": 2}':
                                assert not vhskeelz_api_cmt_account_id, f'multiple vhskeelz_api_cmt accounts for company {company_id}'
                                vhskeelz_api_cmt_account_id = account['Id']
                            if account['Id'] not in existing_sf_ids:
                                existing_sf_ids.append(account['Id'])
                    if len(existing_sf_ids) > 1:
                        if vhskeelz_api_cmt_account_id:
                            log(f'WARNING: Too many existing accounts, will use the api cmt account. company_id {company_id}: {vhskeelz_api_cmt_account_id}')
                            sf_id = vhskeelz_api_cmt_account_id
                        else:
                            log(f'WARNING: Too many existing accounts, will use the first one. company_id {company_id}: {existing_sf_ids}')
                            sf_id = existing_sf_ids[0]
                    elif len(existing_sf_ids) == 1:
                        sf_id = existing_sf_ids[0]
                    else:
                        sf_id = None
                    if sf_id:
                        company_ids_sf_ids[company_id] = (sf_id, None)
                if not sf_id:
                    sf_id, data_hash = create_object('Account', account_data, sf_url, sf_token)
                    action = 'created'
                else:
                    action, sf_id, data_hash = upsert_object('Account', f'Id/{sf_id}', account_data, sf_url, sf_token, *company_ids_sf_ids[company_id], log)
                update_vhskeelz_id_sf_id(sql_execute, 'company_account', company_id, sf_id, company_ids_sf_ids, data_hash)
                log(f'company_id {company_id}: {action} ({sf_id})')
            except Exception as e:
                raise Exception(f'Failed to process company_id {company_id}') from e


def preprocess_row(row, sf_fields_confs=None, log=None):
    row = {k: '' if v in ('null', None) else str(v).strip() for k, v in dict(row).items()}
    row = json.loads(json.dumps(row).replace('\\u202f', ' '))
    if sf_fields_confs:
        for conf in sf_fields_confs.values():
            if conf.get('required'):
                if not row[conf['db_field']]:
                    msg = f'Missing required field {conf["db_field"]}'
                    if conf.get('default'):
                        log(f'WARNING {msg}, setting value to "{conf["default"]}" ({row})')
                        row[conf['db_field']] = conf['default']
                    else:
                        raise Exception(f'{msg} ({row})')
        sf_data = {
            sf_field: conf.get('lambda', lambda r: r.get(conf.get('db_field')))(row)
            for sf_field, conf
            in sf_fields_confs.items()
        }
        return row, sf_data
    else:
        return row


def main(log, skip_companies=False, skip_candidates=False, skip_positions=False):
    sf_url, sf_token = salesforce_login()
    with db.get_db_engine().connect() as conn:
        create_table(conn)
        if not skip_companies:
            update_company_accounts(conn, sf_url, sf_token, log)
        if not skip_candidates:
            update_candidate_contacts(conn, sf_url, sf_token, log)
        if not skip_positions:
            update_position_cases(conn, sf_url, sf_token, log)
