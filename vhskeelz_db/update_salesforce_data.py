import json
import time
import base64
import datetime

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
    "City__c": {'db_field': 'city'},
    "Description": {'db_field': 'position_description'},
    "Subject": {'db_field': 'position_name'},
    # "actice__c": {'db_field': 'status'},
    "employmentType__c": {'db_field': 'employmentType'},
    "hiringUser__c": {'db_field': 'hiringUser'},
    "positionType__c": {'db_field': 'positionType'},
    "Position_id__c": {'db_field': 'position_id', 'required': True},
}


def create_table(conn):
    with conn.begin():
        conn.execute('''
            create table if not exists salesforce_objects (
                object_type varchar(255),
                salesforce_id varchar(255),
                vhskeelz_id varchar(255),
                created_at timestamp,
                updated_at timestamp
            );
            create index if not exists salesforce_objects_object_type_idx on salesforce_objects (object_type);
            create index if not exists salesforce_objects_salesforce_id_idx on salesforce_objects (salesforce_id);
            create index if not exists salesforce_objects_vhskeelz_id_idx on salesforce_objects (vhskeelz_id);
        ''')


def get_vhskeelz_ids_salesforce_ids(conn, object_type):
    return {
        row['vhskeelz_id']: row['salesforce_id']
        for row in conn.execute(f"select salesforce_id, vhskeelz_id from salesforce_objects where object_type = '{object_type}'")
    }


def update_vhskeelz_id_sf_id(conn, object_type, vhskeelz_id, sf_id, vhskeelz_ids_salesforce_ids):
    with conn.begin() as txn:
        if vhskeelz_id not in vhskeelz_ids_salesforce_ids:
            conn.execute(f"""
                insert into salesforce_objects (object_type, salesforce_id, vhskeelz_id, created_at, updated_at)
                values ('{object_type}', '{sf_id}', '{vhskeelz_id}', now(), now())
            """)
        else:
            assert sf_id == vhskeelz_ids_salesforce_ids[vhskeelz_id]
            conn.execute(f"""
                update salesforce_objects set updated_at = now() where object_type = '{object_type}' and salesforce_id = '{sf_id}' and vhskeelz_id = '{vhskeelz_id}';
            """)
        txn.commit()


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


def upsert_object(object_name, key_spec, data, sf_url, sf_token):
    res = requests.patch(
        f'{sf_url}/services/data/v58.0/sobjects/{object_name}/{key_spec}',
        headers={'Authorization': f'Bearer {sf_token}', 'Content-Type': 'application/json', },
        json=data
    )
    if res.status_code == 200:
        return 'updated', res.json()['id']
    elif res.status_code == 201:
        return 'created', res.json()['id']
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
        return res.json()['id']
    except Exception as e:
        raise Exception(res.text) from e


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
        rows = list(conn.execute(f'select email, first_name, last_name, candidate_id, gender, location, phone_number from vehadarta_candidate_data_uniques_candidates'))
    for row in rows:
        row, contact_data = preprocess_row(row, CANDIDATE_CONTACT_SF_FIELDS, log)
        candidate_id = row['candidate_id']
        if candidate_id in candidate_ids_sf_ids:
            sf_id = candidate_ids_sf_ids[candidate_id]
        else:
            existing_contacts = list(soql_query("SELECT Id FROM Contact WHERE Candidate_id__c='{candidate_id}'", sf_url, sf_token))
            assert len(existing_contacts) <= 1, f'Too many existing contacts for candidate_id {candidate_id}: {existing_contacts}'
            sf_id = existing_contacts[0]['Id'] if len(existing_contacts) > 0 else None
        if not sf_id:
            sf_id = create_object('Contact', contact_data, sf_url, sf_token)
            action = 'created'
        else:
            action, sf_id = upsert_object('Contact', f'Id/{sf_id}', contact_data, sf_url, sf_token)
        update_vhskeelz_id_sf_id(conn, 'candidate_contact', candidate_id, sf_id, candidate_ids_sf_ids)
        log(f'candidate_id {row["candidate_id"]}: {action} ({sf_id})')


def update_position_cases(conn, sf_url, sf_token, log):
    sql_fields = ','.join([
        ('"' + conf['db_field'] + '"')
        for conf in POSITION_CASE_SF_FIELDS.values()
        if conf.get('db_field')
    ])
    with conn.begin():
        position_ids_sf_ids = get_vhskeelz_ids_salesforce_ids(conn, 'position_case')
        rows = list(conn.execute(f'''
            WITH RankedItems AS (SELECT {sql_fields}, ROW_NUMBER() OVER(PARTITION BY "position_id") AS rn FROM vehadarta_positions_skills)
            SELECT {sql_fields} FROM RankedItems WHERE rn = 1;
        '''))
    for row in rows:
        row, case_data = preprocess_row(row, POSITION_CASE_SF_FIELDS, log)
        position_id = row['position_id']
        if position_id in position_ids_sf_ids:
            sf_id = position_ids_sf_ids[position_id]
        else:
            existing_cases = list(soql_query(f"SELECT Id FROM Case WHERE Position_id__c='{position_id}'", sf_url, sf_token))
            assert len(existing_cases) <= 1, f'Too many existing cases for position_id {position_id}: {existing_cases}'
            if len(existing_cases):
                sf_id = existing_cases[0]['Id']
            else:
                sf_id = None
        if not sf_id:
            sf_id = create_object('Case', case_data, sf_url, sf_token)
            action = 'created'
        else:
            action, sf_id = upsert_object('Case', f'Id/{sf_id}', case_data, sf_url, sf_token)
        update_vhskeelz_id_sf_id(conn, 'position_case', position_id, sf_id, position_ids_sf_ids)
        log(f'position_id {position_id}: {action} ({sf_id})')


def update_company_accounts(conn, sf_url, sf_token, log):
    with conn.begin():
        company_ids_sf_ids = get_vhskeelz_ids_salesforce_ids(conn, 'company_account')
        rows = list(conn.execute('''
            WITH RankedItems AS (SELECT "companyId", company_name, ROW_NUMBER() OVER(PARTITION BY "companyId") AS rn FROM vehadarta_positions_skills)
            SELECT "companyId", company_name FROM RankedItems WHERE rn = 1 and company_name != 'null';
        '''))
    for row in rows:
        row, account_data = preprocess_row(row, COMPANY_ACCOUNT_SF_FIELDS, log)
        company_name, company_id = row['company_name'], row['companyId']
        try:
            if company_id in company_ids_sf_ids:
                sf_id = company_ids_sf_ids[company_id]
            else:
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
            if not sf_id:
                sf_id = create_object('Account', account_data, sf_url, sf_token)
                action = 'created'
            else:
                action, sf_id = upsert_object('Account', f'Id/{sf_id}', account_data, sf_url, sf_token)
            update_vhskeelz_id_sf_id(conn, 'company_account', company_id, sf_id, company_ids_sf_ids)
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
                    msg = f'Missing required field {conf["db_field"]} in row: {row}'
                    if conf.get('default'):
                        log(msg)
                        log(f'Setting value to {conf["default"]}')
                        row[conf['db_field']] = conf['default']
                    else:
                        raise Exception(msg)
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
