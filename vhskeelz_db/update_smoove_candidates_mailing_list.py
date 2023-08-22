import time
import datetime

import requests

from .db import get_db_engine
from . import config


STUDIO_LIST_ID = 865719


def process_input_row(row):
    for k, v in row.items():
        if v == 'null':
            v = None
        if k == 'gender':
            v = {'0': 'male', '1': 'female'}.get(v, 'other')
        elif k == 'birth_date':
            v = datetime.datetime.strptime(v, '%b %d, %Y').date() if v else None
        else:
            v = v.strip() if v else None
            if not v:
                v = None
        row[k] = v
    return row


def process_output_row(row):
    try:
        id_number = int(row['id_number']) if row['id_number'] else None
    except ValueError:
        id_number = None
    return {
        "email": row['email'],
        "firstName": row['first_name'],
        "lastName": row['last_name'],
        "phone": row['phone_number'],
        "city": row['city'],
        "dateOfBirth": '{}T12:00:00.0000000Z'.format(row['birth_date'].strftime('%Y-%m-%d')) if row['birth_date'] else None,
        "customFields": {
            "i39": id_number,
            "i50": {'male': 'זכר', 'female': 'נקבה', 'other': None}[row['gender']]
        },
        "lists_ToSubscribe": [STUDIO_LIST_ID]
    }


def iterate_candidates():
    with get_db_engine().connect() as conn:
        for row in conn.execute("""
            select email, "firstName" first_name, "lastName" last_name, "phoneNumber" phone_number, city, "birthDate" birth_date, gender, "idNumber" id_number
            from vehadarta_db_candidate
        """):
            yield process_input_row(dict(row))


def main(log, only_emails=None):
    if only_emails:
        only_emails = [e.strip() for e in only_emails.split(',') if e.strip()]
    uuids = {}
    for candidate in iterate_candidates():
        smoove_candidate = process_output_row(candidate)
        if only_emails and smoove_candidate['email'] not in only_emails:
            continue
        assert smoove_candidate['email'] not in uuids, f'duplicate email {smoove_candidate["email"]}'
        try:
            res = requests.post(
                'https://rest.smoove.io/v1/async/contacts?updateIfExists=false&restoreIfDeleted=false&restoreIfUnsubscribed=false&overrideNullableValue=false',
                json=smoove_candidate,
                headers={'Authorization': f'Bearer {config.SMOOVE_API_KEY}'}
            )
            assert res.status_code == 202, res.text
            uuids[smoove_candidate['email']] = res.json()
        except:
            log(f'failed to update: {smoove_candidate}')
            raise
    log(f'processed {len(uuids)} candidates, checking status')
    start_time = datetime.datetime.now()
    while True:
        num_updated = 0
        for email, update_res in uuids.items():
            if update_res is not None:
                num_updated += 1
                res = requests.get(
                    f'https://rest.smoove.io/v1/async/contacts/{update_res["Uuid"]}/{update_res["Timestamp"]}/status',
                    headers={'Authorization': f'Bearer {config.SMOOVE_API_KEY}'}
                )
                assert res.status_code == 200, f'{email} {res.text}'
                if res.json()['status'] == 'Succeeded':
                    uuids[email] = None
        if num_updated == 0:
            break
        if datetime.datetime.now() - start_time > datetime.timedelta(hours=1):
            raise Exception('timeout')
        time.sleep(60)
