import os
from textwrap import dedent

import dataflows as df

from .db import get_db_engine
from . import config


def get_dry_run_save_path(mailing_type):
    dirname = {
        'num_fits': 'candidate_offers_num_fits_mailing',
        'interested': 'candidate_offers_interested_mailing',
        'new_matches': 'candidate_offers_new_matches_mailing',
    }[mailing_type]
    return os.path.join(config.DATA_DIR, dirname, 'dry_run')


def run_migrations(log, mailing_type):
    log(f'Running migrations for {mailing_type}...')
    table_name = {
        'num_fits': 'candidate_offers_num_fits_mailing_status',
        'interested': 'candidate_offers_interested_mailing_status',
        'new_matches': 'candidate_offers_new_matches_mailing_status',
    }[mailing_type]
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


def dry_run_save_rows(log, grouped_rows, mailing_type):
    save_path = get_dry_run_save_path(mailing_type)

    def iterator():
        for key, rows in grouped_rows.items():
            for row in rows:
                yield {
                    'group_key': key,
                    **row
                }

    df.Flow(
        iterator(),
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
    return {
        'num_fits': dedent(f'''
            case
                when cast(l."fitPercentage" as float) < {mailing_config["high_min_fit_percentage"]} then 'medium'
                else 'high'
            end
        '''),
        'interested': dedent(f'''
            case
                {get_fit_desc_sql_interested_fit_percentage_cases(mailing_config)}
                else '{mailing_config["fit_labels"]["other"]}'
            end
        '''),
        'new_matches': dedent(f'''
            case
                {get_fit_desc_sql_interested_fit_percentage_cases(mailing_config)}
            end
        ''')
    }[mailing_type]


def get_where_sql(mailing_type):
    mailing_config = config.CANDIDATE_OFFERS_MAILING_CONFIG[mailing_type]
    return {
        'num_fits': dedent(f'''
            l."fitPercentage" != 'null'
            and cast(l."fitPercentage" as float) >= {mailing_config["medium_min_fit_percentage"]}
            and cast(l."fitPercentage" as float) <= 1
            and l.candidate_id || '_' || l."positionOfferId" not in (
                select candidate_id || '_' || "positionOfferId" 
                from candidate_offers_num_fits_mailing_status
                where status = 'sent'
            )
        '''),
        'interested': dedent('''
            l."Interested" = 'Yes'
            and l.candidate_id || '_' || l."positionOfferId" not in (
                select candidate_id || '_' || "positionOfferId" 
                from candidate_offers_interested_mailing_status
                where status = 'sent'
            )
        '''),
        'new_matches': dedent(f'''
            l."fitPercentage" != 'null'
            and cast(l."fitPercentage" as float) >= {mailing_config["min_fit_percentage"]}
            and cast(l."fitPercentage" as float) <= 1
            and l.candidate_id || '_' || l."positionOfferId" not in (
                select candidate_id || '_' || "positionOfferId" 
                from candidate_offers_new_matches_mailing_status
                where status = 'sent'
            )
        ''')
    }[mailing_config['type']]


def get_group_key(row, mailing_type):
    return {
        'num_fits': (row['company_email'], row['position_id'], row['city']),
        'interested': (row['company_email'],),
        'new_matches': (row['candidate_email'],),
    }[mailing_type]


def main(log, mailing_type, dry_run=False):
    run_migrations(log, mailing_type)
    with get_db_engine().connect() as conn:
        with conn.begin():
            log("Fetching candidate positions...")
            details_url_sql = config.CANDIDATE_POSITION_CV_URL_TEMPLATE.format(
                position_id="' || l.\"positionOfferId\" || '",
                candidate_id="' || l.\"candidate_id\" || '",
            )
            fit_desc_sql = get_fit_desc_sql(mailing_type)
            where_sql = get_where_sql(mailing_type)
            rows = [
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
                    'creation_date': row.date_created
                }
                for row
                in conn.execute(dedent(f'''
                    with numbered_positions as (
                        select *, row_number() over (partition by "position_id" order by "position_id") as rn
                        from vehadarta_positions_skills
                    )
                    select
                        l.candidate_id, l."positionOfferId" position_id, l."Candidate name" candidate_name,
                        t.ta_email, t.ta_name, p.position_name, l."fitPercentage" fit_percentage, p.city,
                        l.email, p."dateCreated" date_created,
                        '{details_url_sql}' details_url,
                        {fit_desc_sql} fit_desc,
                        count(1) cnt
                    from vehadarta_candidate_offers_list l
                    join numbered_positions p on l."positionOfferId" = p.position_id and p.rn = 1
                    join vehadarta_company_and_company_ta t on p."companyId" = t."companyId" and t."companyId" != 'null'
                    where {where_sql}
                    group by
                        l.candidate_id, l."positionOfferId", l."Candidate name", t.ta_email, t.ta_name, p.position_name, 
                        l."fitPercentage", p.city, l.email, p."dateCreated"
                '''))
            ]
    log(f"Fetched {len(rows)} candidate positions")
    grouped_rows = {}
    for row in rows:
        group_key = ';;'.join(get_group_key(row, mailing_type))
        grouped_rows.setdefault(group_key, []).append(row)
    log(f"Grouped to {len(grouped_rows)} groups")
    if dry_run:
        log("Dry run, not sending emails")
        dry_run_save_rows(log, grouped_rows, mailing_type)
    else:
        raise NotImplementedError("TODO: implement sending emails")
