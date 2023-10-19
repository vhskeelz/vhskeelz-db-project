import time
import datetime

from .db import get_db_engine
from . import config, common


STUDIO_LIST_ID = 865719


def process_input_row(row):
    for k, v in row.items():
        if v == 'null':
            v = None
        if k == 'gender':
            v = {'Male': 'male', 'Female': 'female'}.get(v, 'other')
        # elif k == 'birth_date':
        #     v = datetime.datetime.strptime(v, '%b %d, %Y').date() if v else None
        else:
            v = v.strip() if v else None
            if not v:
                v = None
        row[k] = v
    return row


def process_output_row(row):
    # try:
    #     id_number = int(row['id_number']) if row['id_number'] else None
    # except ValueError:
    #     id_number = None
    return {
        "email": row['email'],
        "firstName": row['first_name'],
        "lastName": row['last_name'],
        "phone": row['phone_number'],
        "city": row['city'],
        # "dateOfBirth": '{}T12:00:00.0000000Z'.format(row['birth_date'].strftime('%Y-%m-%d')) if row['birth_date'] else None,
        "customFields": {
            # "i39": id_number,
            "i50": {'male': 'זכר', 'female': 'נקבה', 'other': None}[row['gender']]
        },
        "lists_ToSubscribe": [STUDIO_LIST_ID]
    }


def iterate_candidates():
    with get_db_engine().connect() as conn:
        with conn.begin():
            for row in conn.execute("""
                select email, first_name, last_name, phone_number, city, gender
                from vehadarta_candidate_data_uniques_candidates
            """):
                yield process_input_row(dict(row))


def case_insensitive_get(d, key):
    for k, v in d.items():
        if k.lower() == key.lower():
            return v
    return None


def main(log, only_emails=None, limit=None, debug=False):
    if only_emails:
        only_emails = [e.strip() for e in only_emails.split(',') if e.strip()]
    uuids = {}
    requests_session = common.requests_session_retry(status_forcelist=(500, 502, 503, 504))
    for candidate in iterate_candidates():
        smoove_candidate = process_output_row(candidate)
        if only_emails and smoove_candidate['email'] not in only_emails:
            continue
        elif smoove_candidate['email'] in uuids:
            log(f'skipping duplicate email: {smoove_candidate["email"]}')
            continue
        if debug:
            log(smoove_candidate)
        try:
            # first we only create new contacts, we don't update existing contacts
            res = requests_session.post(
                'https://rest.smoove.io/v1/async/contacts?updateIfExists=false&restoreIfDeleted=false&restoreIfUnsubscribed=false&overrideNullableValue=false',
                json=smoove_candidate,
                headers={'Authorization': f'Bearer {config.SMOOVE_API_KEY}'}
            )
            assert res.status_code == 202, f'unexpected status_code: {res.status_code} - {res.text}'
            res_json = res.json()
            assert case_insensitive_get(res_json, 'Uuid'), f'no Uuid in contacts create: {res_json}\n{smoove_candidate}'
            assert case_insensitive_get(res_json, 'Timestamp'), f'no Timestamp in contacts create: {res_json}\n{smoove_candidate}'
            uuids[smoove_candidate['email']] = [res_json]
        except:
            log(f'failed to create: {smoove_candidate}')
            raise
        try:
            # then we update all contacts to add them to the mailing list
            res = requests_session.post(
                'https://rest.smoove.io/v1/async/contacts?updateIfExists=true&restoreIfDeleted=false&restoreIfUnsubscribed=false&overrideNullableValue=false',
                json={
                    "email": smoove_candidate['email'],
                    "lists_ToSubscribe": [STUDIO_LIST_ID]
                },
                headers={'Authorization': f'Bearer {config.SMOOVE_API_KEY}'}
            )
            assert res.status_code == 202, f'unexpected status_code: {res.status_code} - {res.text}'
            res_json = res.json()
            assert case_insensitive_get(res_json, 'Uuid'), f'no Uuid in contacts update: {res_json}\n{smoove_candidate}'
            assert case_insensitive_get(res_json, 'Timestamp'), f'no Timestamp in contacts update: {res_json}\n{smoove_candidate}'
            uuids[smoove_candidate['email']].append(res_json)
        except:
            log(f'failed to update: {smoove_candidate}')
            raise
        if limit and len(uuids) >= limit:
            break
    log(f'processed {len(uuids)} candidates, checking status')
    time.sleep(2)
    start_time = datetime.datetime.now()
    while True:
        num_updated = 0
        for email, update_results in uuids.items():
            if debug:
                log(email)
            for i, update_res in enumerate(update_results):
                if update_res is not None:
                    num_updated += 1
                    res = requests_session.get(
                        f'https://rest.smoove.io/v1/async/contacts/{case_insensitive_get(update_res, "Uuid")}/status',
                        headers={'Authorization': f'Bearer {config.SMOOVE_API_KEY}'}
                    )
                    assert res.status_code == 200, f'{email} - unexpected status_code: {res.status_code} - {res.text}'
                    if res.json()['status'] == 'Succeeded':
                        uuids[email][i] = None
        if any([any(update_results) for email, update_results in uuids.items()]):
            log("Waiting for smoove to finish processing...")
            for email, update_results in uuids.items():
                log(f'{email}: {update_results}')
            if datetime.datetime.now() - start_time > datetime.timedelta(minutes=30):
                raise Exception('timeout')
            time.sleep(60)
        else:
            break
