import os
import base64
import datetime
from textwrap import dedent

import dataflows as df
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Asm, Attachment, FileContent, FileName, FileType, Disposition

from .db import get_db_engine
from . import config, download_position_candidate_cv, load_data


# we use this to ensure that the tables we use are updated before running
DEPENDANT_TABLES = {
    t: t for t in [
        'vehadarta_positions_skills',
        'vehadarta_company_and_company_ta',
        'vehadarta_candidate_offers_list_only_for_mailing',
        'smoove_blocklist',
        'aggregation_candidates',
        'aggregation_positions',
        'aggregation_candidate_positions',
    ]
}


def get_dry_run_save_path(mailing_type):
    dirname = {
        'num_fits': 'candidate_offers_num_fits_mailing',
        'interested': 'candidate_offers_interested_mailing',
        'new_matches': 'candidate_offers_new_matches_mailing',
        'new_position': 'candidate_offers_new_position_mailing',
    }[mailing_type]
    return os.path.join(config.DATA_DIR, dirname, 'dry_run')


def get_db_status_table_name(mailing_type):
    return {
        'num_fits': 'candidate_offers_num_fits_mailing_status',
        'interested': 'candidate_offers_interested_mailing_status',
        'new_matches': 'candidate_offers_new_matches_mailing_status',
        'new_position': 'candidate_offers_new_position_mailing_status',
    }[mailing_type]


def run_migrations(log, mailing_type):
    log(f'Running migrations for {mailing_type}...')
    table_name = get_db_status_table_name(mailing_type)
    with get_db_engine().connect() as conn:
        with conn.begin():
            conn.execute(dedent(f'''
                create table if not exists {table_name} (
                    candidate_id text not null,
                    "positionOfferId" text not null,
                    status text not null,
                    date timestamp not null default now()
                );
            '''))
    log('Migrations done.')


def dry_run_save_rows(log, mailing_type, grouped_rows, mail_data):
    save_path = get_dry_run_save_path(mailing_type)

    def grouped_rows_iterator():
        for key, rows in grouped_rows.items():
            for row in rows:
                yield {
                    'group_key': key,
                    **row
                }

    def mail_data_iterator():
        for row in mail_data:
            dynamic_template_data = row.pop('dynamic_template_data')
            yield {
                **row,
                **dynamic_template_data,
            }

    df.Flow(
        grouped_rows_iterator(),
        mail_data_iterator(),
        df.dump_to_path(save_path),
    ).process()
    log(f"Saved to {save_path}")


def get_fit_desc_sql_interested_fit_percentage_cases(mailing_config):
    cases = []
    for key, from_to in config.CANDIDATE_OFFERS_MAILING_CONFIG['fit_percentages'].items():
        cases.append(f'when cast(l."fitPercentage" as float) > {from_to[0]} and cast(l."fitPercentage" as float) <= {from_to[1]} then \'{mailing_config["fit_labels"][key]}\'')
    return '\n'.join(cases)


def get_fit_desc_sql(mailing_type):
    mailing_config = config.CANDIDATE_OFFERS_MAILING_CONFIG[mailing_type]
    if mailing_type == 'num_fits':
        return dedent(f'''
            case
                when cast(l."fitPercentage" as float) < {mailing_config["high_min_fit_percentage"]} then 'medium'
                else 'high'
            end
        ''')
    elif mailing_type == 'interested':
        return dedent(f'''
            case
                {get_fit_desc_sql_interested_fit_percentage_cases(mailing_config)}
                else '{mailing_config["fit_labels"]["other"]}'
            end
        ''')
    elif mailing_type == 'new_matches':
        return dedent(f'''
            case
                {get_fit_desc_sql_interested_fit_percentage_cases(mailing_config)}
            end
        ''')
    else:
        raise NotImplementedError()


def get_where_sql(mailing_type):
    mailing_config = config.CANDIDATE_OFFERS_MAILING_CONFIG[mailing_type]
    if mailing_type == 'num_fits':
        return dedent(f'''
            l."fitPercentage" != 'null'
            and cast(l."fitPercentage" as float) >= {mailing_config["medium_min_fit_percentage"]}
            and cast(l."fitPercentage" as float) <= 1
            and l.candidate_id || '_' || l."positionOfferId" not in (
                select candidate_id || '_' || "positionOfferId" 
                from candidate_offers_num_fits_mailing_status
                where status = 'sent'
            )
        ''')
    elif mailing_type == 'interested':
        return dedent('''
            l."Interested" = 'Yes'
            and l.candidate_id || '_' || l."positionOfferId" not in (
                select candidate_id || '_' || "positionOfferId" 
                from candidate_offers_interested_mailing_status
                where status = 'sent'
            )
        ''')
    elif mailing_type == 'new_matches':
        return dedent(f'''
            l."fitPercentage" != 'null'
            and cast(l."fitPercentage" as float) >= {mailing_config["min_fit_percentage"]}
            and cast(l."fitPercentage" as float) <= 1
            and l.candidate_id || '_' || l."positionOfferId" not in (
                select candidate_id || '_' || "positionOfferId" 
                from candidate_offers_new_matches_mailing_status
                where status = 'sent'
            )
        ''')
    else:
        raise NotImplementedError()


def get_group_key(row, mailing_type):
    return {
        'num_fits': (row['company_email'], row['position_id'], row['city']),
        'interested': (row['company_email'],),
        'new_matches': (row['candidate_email'],),
        'new_position': (row['company_email'], row['position_id']),
    }[mailing_type]


def get_email_field(mailing_type):
    return {
        'num_fits': 'company_email',
        'interested': 'company_email',
        'new_matches': 'candidate_email',
        'new_position': 'company_email',
    }[mailing_type]


def download_cvs(position_candidate_ids, log):
    log(f'Downloading CVs for {len(position_candidate_ids)} candidate positions')
    download_position_candidate_cv.main_multi(log, position_candidate_ids, save_to_gcs=True)


def get_mail_data(grouped_rows, mailing_type, log):
    data = []
    from_email = config.CANDIDATE_OFFERS_MAILING_CONFIG[mailing_type]['from_email']
    template_id = config.CANDIDATE_OFFERS_MAILING_CONFIG[mailing_type]['template_id']
    if mailing_type == 'num_fits':
        for rows in grouped_rows.values():
            first_row = rows[0]
            data.append({
                "from_email": from_email,
                "to_emails": first_row[get_email_field(mailing_type)],
                "template_id": template_id,
                "dynamic_template_data": {
                    "company_name": first_row['company_name'],
                    "position_name": first_row['position_name'],
                    "city": first_row['city'] if first_row['city'] != 'null' else '-',
                    "details_url": config.POSITION_DETAILS_URL_TEMPLATE.format(position_id=first_row['position_id']),
                    "num_high_fits": sum([1 for row in rows if row['fit_desc'] == 'high']),
                    "num_medium_fits": sum([1 for row in rows if row['fit_desc'] == 'medium']),
                },
                'candidate_position_ids': [[row['candidate_id'], row['position_id']] for row in rows]
            })
    elif mailing_type == 'interested':
        os.makedirs(os.path.join(config.DATA_DIR, 'candidate_offers_interested_mailing', 'cv'), exist_ok=True)
        position_candidate_ids = set()
        for rows in grouped_rows.values():
            for row in rows:
                position_candidate_ids.add((row['position_id'], row['candidate_id']))
        if position_candidate_ids:
            download_cvs(position_candidate_ids, log)
        for rows in grouped_rows.values():
            for row in rows:
                cv_filename = os.path.join(config.DATA_DIR, 'candidate_offers_interested_mailing', 'cv', f'{row["position_id"]}_{row["candidate_id"]}.pdf')
                if not download_position_candidate_cv.download_from_gcs(f'cv/{row["position_id"]}_{row["candidate_id"]}.pdf', cv_filename):
                    cv_filename = None
                position_candidate_ids.add((row['position_id'], row['candidate_id']))
                data.append({
                    "from_email": from_email,
                    "to_emails": row[get_email_field(mailing_type)],
                    "template_id": template_id,
                    "pdf_attachment_filename": cv_filename,
                    'pdf_attachment_name': f"{row['candidate_name']} - {row['position_name']}.pdf",
                    "dynamic_template_data": {
                        "company_name": row['company_name'],
                        "position_name": row['position_name'],
                        "fit_desc": row['fit_desc'],
                        "city": row['city'] if row['city'] != 'null' else '-',
                        "details_url": config.CANDIDATE_POSITION_CV_URL_TEMPLATE.format(position_id=row['position_id'], candidate_id=row['candidate_id']),
                        "candidate_name": row['candidate_name'],
                    },
                    'candidate_position_ids': [[row['candidate_id'], row['position_id']]]
                })
    elif mailing_type == 'new_matches':
        for rows in grouped_rows.values():
            first_row = rows[0]
            fits = {}
            for row in rows:
                fits[row['position_name']] = {
                    "date": row['creation_date'].strftime('%d/%m/%Y'),
                    "name": row['position_name'],
                    "fit": row['fit_desc']
                }
            fits = list(fits.values())
            data.append({
                "from_email": from_email,
                "to_emails": first_row[get_email_field(mailing_type)],
                "template_id": template_id,
                "dynamic_template_data": {
                    "candidate_name": first_row['candidate_name'],
                    "num_fits": len(fits),
                    "fits": fits
                },
                'candidate_position_ids': [[row['candidate_id'], row['position_id']] for row in rows]
            })
    elif mailing_type == 'new_position':
        for rows in grouped_rows.values():
            first_row = rows[0]
            data.append({
                "from_email": from_email,
                "to_emails": first_row[get_email_field(mailing_type)],
                "template_id": template_id,
                "dynamic_template_data": {
                    "company_name": first_row['company_name'],
                    "position_name": first_row['position_name'],
                },
                'candidate_position_ids': [[row['candidate_id'], row['position_id']] for row in rows]
            })
    else:
        raise NotImplementedError()
    return data


def get_dynamic_template_data(mailing_type, row):
    dynamic_template_data = row['dynamic_template_data']
    if 'city' in dynamic_template_data and dynamic_template_data['city'] == '-':
        dynamic_template_data['city'] = ''
    return dynamic_template_data


def send_mails(log, mailing_type, mail_data, allow_send, test_email_to, test_email_limit, test_email_update_db):
    if test_email_to == 'default':
        test_email_to = config.CANDIDATE_OFFERS_MAILING_CONFIG['default_test_email_to']
    log(f'Sending {len(mail_data)} mails for {mailing_type} (allow_send={allow_send}, test_email_to={test_email_to}, test_email_limit={test_email_limit}, test_email_update_db={test_email_update_db})')
    if allow_send:
        assert not test_email_to and not test_email_limit and not test_email_update_db
    else:
        assert test_email_to is not None
    for i, row in enumerate(mail_data):
        if not allow_send and test_email_limit is not None and i >= test_email_limit:
            log(f'Breaking after {test_email_limit} test mails')
            break
        from_email = row['from_email']
        to_emails = row['to_emails'] if allow_send else test_email_to
        template_id = row['template_id']
        dynamic_template_data = get_dynamic_template_data(mailing_type, row)
        log(f'Sending mail from {from_email} to {to_emails} with template_id {template_id} and dynamic_template_data {dynamic_template_data}')
        message = Mail(from_email=from_email, to_emails=to_emails)
        message.add_bcc(config.CANDIDATE_OFFERS_MAILING_CONFIG['bcc'])
        message.dynamic_template_data = dynamic_template_data
        message.template_id = template_id
        if row.get('pdf_attachment_filename') and row.get('pdf_attachment_name'):
            with open(row['pdf_attachment_filename'], 'rb') as f:
                data = f.read()
                f.close()
            encoded_file = base64.b64encode(data).decode()
            message.attachment = Attachment(
                FileContent(encoded_file),
                FileName(row['pdf_attachment_name']),
                FileType('application/pdf'),
                Disposition('attachment')
            )
        message.asm = Asm(int(config.SENDGRID_UNSUSCRIBE_GROUP_ID))
        sendgrid_client = SendGridAPIClient(config.SENDGRID_API_KEY)
        sendgrid_client.send(message)
        if allow_send or test_email_update_db:
            log(f'Updating {len(row["candidate_position_ids"])} db statuses to sent')
            db_insert_statuses(mailing_type, row['candidate_position_ids'], 'sent')


def db_insert_statuses(mailing_type, candidate_position_ids, status):
    table_name = get_db_status_table_name(mailing_type)
    with get_db_engine().connect() as conn:
        with conn.begin():
            for candidate_id, position_id in candidate_position_ids:
                conn.execute(f'''
                    insert into {table_name} (candidate_id, "positionOfferId", status)
                    values ('{candidate_id}', '{position_id}', '{status}')
                ''')


def remove_blocklists(log, mailing_type, rows):
    with get_db_engine().connect() as conn:
        with conn.begin():
            block_candidate_id_position_id = [
                (row.candidate_id.strip(), row.position_id.strip())
                for row in conn.execute(f'''select "Candidate_id" candidate_id, "Position_id" position_id from {DEPENDANT_TABLES['aggregation_candidate_positions']} where "Remove" = 'TRUE' ''')
                if row.candidate_id and row.position_id
            ]
            log(f'Found {len(block_candidate_id_position_id)} block candidate_id_position_id')
            block_candidate_id = [
                row.candidate_id.strip()
                for row in conn.execute(f'''select "Candidate_id" candidate_id from {DEPENDANT_TABLES['aggregation_candidates']} where "Remove_can" = 'TRUE' ''')
                if row.candidate_id
            ]
            log(f'Found {len(block_candidate_id)} block candidate_id')
            block_position_id = [
                row.position_id.strip()
                for row in conn.execute(f'''select "P_ID" position_id from {DEPENDANT_TABLES['aggregation_positions']} where "Remove_P" = 'TRUE' ''')
                if row.position_id
            ]
            log(f'Found {len(block_position_id)} block position_id')
            smoove_blocklist_emails = [
                row.email.strip()
                for row in conn.execute(f'select email from {DEPENDANT_TABLES["smoove_blocklist"]}')
                if row.email
            ]
            log(f'Found {len(smoove_blocklist_emails)} smoove_blocklist_emails')
    valid_rows = []
    blocked_candidate_id_position_id = 0
    blocked_candidate_id = 0
    blocked_position_id = 0
    blocked_smoove_email = 0
    for row in rows:
        if (row['candidate_id'].strip(), row['position_id'].strip()) in block_candidate_id_position_id:
            blocked_candidate_id_position_id += 1
        elif row['candidate_id'].strip() in block_candidate_id:
            blocked_candidate_id += 1
        elif row['position_id'].strip() in block_position_id:
            blocked_position_id += 1
        elif row[get_email_field(mailing_type)].strip() in smoove_blocklist_emails:
            blocked_smoove_email += 1
        else:
            valid_rows.append(row)
    log(f'Blocked {blocked_candidate_id_position_id} candidate_id_position_id, {blocked_candidate_id} candidate_id, {blocked_position_id} position_id, {blocked_smoove_email} smoove emails')
    return valid_rows


def get_candidate_position_rows(log, mailing_type):
    with get_db_engine().connect() as conn:
        with conn.begin():
            log("Fetching candidate positions...")
            details_url_sql = config.CANDIDATE_POSITION_CV_URL_TEMPLATE.format(
                position_id="' || l.\"positionOfferId\" || '",
                candidate_id="' || l.\"candidate_id\" || '",
            )
            fit_desc_sql = get_fit_desc_sql(mailing_type)
            where_sql = get_where_sql(mailing_type)
            return [
                {
                    'candidate_id': row.candidate_id,
                    'position_id': row.position_id,
                    'candidate_name': row.candidate_name,
                    'company_email': row.ta_email,
                    'company_name': row.ta_name,
                    'position_name': row.position_name,
                    'city': row.city,
                    'details_url': row.details_url,
                    'fit_desc': row.fit_desc,
                    'candidate_email': row.email,
                    'creation_date': datetime.datetime.strptime(row.date_created, '%b %d, %Y, %I:%M:%S %p')
                }
                for row
                in conn.execute(dedent(f'''
                    with numbered_positions as (
                        select *, row_number() over (partition by "position_id" order by "position_id") as rn
                        from {DEPENDANT_TABLES['vehadarta_positions_skills']}
                        where active != '0'
                    )
                    select
                        l.candidate_id, l."positionOfferId" position_id, l."Candidate name" candidate_name,
                        t.ta_email, t.ta_name, p.position_name, l."fitPercentage" fit_percentage, p.city,
                        l.email, p."dateCreated" date_created,
                        '{details_url_sql}' details_url,
                        {fit_desc_sql} fit_desc,
                        count(1) cnt
                    from {DEPENDANT_TABLES['vehadarta_candidate_offers_list_only_for_mailing']} l
                    join numbered_positions p on l."positionOfferId" = p.position_id and p.rn = 1
                    join {DEPENDANT_TABLES['vehadarta_company_and_company_ta']} t
                        on p."companyId" != 'null' and (p."companyId" = t."companyId" or p."companyId" = t.comp_id)
                    where {where_sql}
                    group by
                        l.candidate_id, l."positionOfferId", l."Candidate name", t.ta_email, t.ta_name, p.position_name, 
                        l."fitPercentage", p.city, l.email, p."dateCreated"
                '''))
            ]


def get_new_position_candidate_position_rows(log):
    with get_db_engine().connect() as conn:
        with conn.begin():
            log("Fetching candidate positions...")
            return [
                {
                    'candidate_id': '-',
                    'position_id': row.position_id,
                    'candidate_name': '-',
                    'company_email': row.ta_email,
                    'company_name': row.ta_name,
                    'position_name': row.position_name,
                    'city': '-',
                    'details_url': '-',
                    'fit_desc': '-',
                    'candidate_email': '-',
                    'creation_date': datetime.datetime.now()
                } for row
                in conn.execute(dedent(f'''
                    select p.position_id, p.position_name, t.ta_email, t.ta_name
                    from {DEPENDANT_TABLES['vehadarta_positions_skills']} p
                    join {DEPENDANT_TABLES['vehadarta_company_and_company_ta']} t
                        on p."companyId" != 'null' and (p."companyId" = t."companyId" or p."companyId" = t.comp_id)
                    where p.position_id not in (
                        select "positionOfferId"
                        from candidate_offers_new_position_mailing_status
                        where status = 'sent'
                    ) and p.active != '0'
                    group by p.position_id, p.position_name, t.ta_email, t.ta_name
                '''))
            ]


def main(log, mailing_type, dry_run=False, allow_send=False, test_email_to=None, test_email_limit=None, test_email_update_db=False,
         only_candidate_position_ids=None, ensure_updated_tables=False):
    if ensure_updated_tables:
        load_data.ensure_updated_tables(log, DEPENDANT_TABLES.keys())
    run_migrations(log, mailing_type)
    if mailing_type == 'new_position':
        rows = get_new_position_candidate_position_rows(log)
    else:
        rows = get_candidate_position_rows(log, mailing_type)
        if only_candidate_position_ids:
            rows = [row for row in rows if [row['candidate_id'], row['position_id']] in only_candidate_position_ids]
    log(f"Fetched {len(rows)} candidate positions")
    rows = remove_blocklists(log, mailing_type, rows)
    grouped_rows = {}
    for row in rows:
        group_key = ';;'.join(get_group_key(row, mailing_type))
        grouped_rows.setdefault(group_key, []).append(row)
    log(f"Grouped to {len(grouped_rows)} groups")
    mail_data = get_mail_data(grouped_rows, mailing_type, log)
    if dry_run:
        log("Dry run, not sending emails")
        dry_run_save_rows(log, mailing_type, grouped_rows, mail_data)
    else:
        send_mails(log, mailing_type, mail_data, allow_send, test_email_to, test_email_limit, test_email_update_db)
