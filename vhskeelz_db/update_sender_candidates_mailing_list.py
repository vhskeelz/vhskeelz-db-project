from .db import get_db_engine
from . import config, common


VHSKEELZ_DB_API_GROUP_ID = 'dN7PqD'


def process_input_row(row):
    for k, v in row.items():
        if v == 'null':
            v = None
        if k == 'gender':
            v = {'Male': 'male', 'Female': 'female'}.get(v, 'other')
        else:
            v = v.strip() if v else None
            if not v:
                v = None
        row[k] = v
    return row


def process_output_row(row):
    return {
        "email": row['email'],
        "firstname": row['first_name'],
        "lastname": row['last_name'],
        # "phone": row['phone_number'],  # not used because many rows don't have a valid phone number
        "fields": {
            "city": row['city'],
            "gender": row['gender'],
            "phonenumber": row['phone_number']
        },
        "groups": [VHSKEELZ_DB_API_GROUP_ID]
    }


def iterate_candidates():
    candidates = []
    with get_db_engine().connect() as conn:
        with conn.begin():
            for row in conn.execute("""
                select "Email" email, "Candidate first name" first_name, "Candidate last name" last_name,
                       "Phone number" phone_number, "Candidate location" city, "Gender" gender
                from skeelz_export_candidates
            """):
                candidates.append(process_input_row(dict(row)))
    return candidates


def main(log, only_emails=None, limit=None, debug=False):
    if only_emails:
        only_emails = [e.strip() for e in only_emails.split(',') if e.strip()]
    created_emails = set()
    requests_session = common.requests_session_retry(status_forcelist=(500, 502, 503, 504))
    for candidate in iterate_candidates():
        sender_candidate = process_output_row(candidate)
        if only_emails and sender_candidate['email'] not in only_emails:
            continue
        elif sender_candidate['email'] in created_emails:
            log(f'skipping duplicate email: {sender_candidate["email"]}')
            continue
        if debug:
            log(sender_candidate)
        try:
            res = requests_session.post(
                'https://api.sender.net/v2/subscribers',
                json=sender_candidate,
                headers={
                    'Authorization': f'Bearer {config.SENDER_API_TOKEN}',
                    "Content-Type": "application/json", "Accept": "application/json",
                }
            )
            if res.status_code == 422:
                log(f'failed to create due to invalid fields data, will continue anyway: {sender_candidate} - {res.text}')
            else:
                assert res.status_code == 200, f'unexpected status_code: {res.status_code} - {res.text}'
                res_json = res.json()
                assert res_json['success'] is True, f'failed to create: {res_json}\n{sender_candidate}'
                created_emails.add(sender_candidate['email'])
        except:
            log(f'failed to create: {sender_candidate}')
            raise
        if limit and len(created_emails) >= limit:
            break
    log(f'created {len(created_emails)} candidates')
