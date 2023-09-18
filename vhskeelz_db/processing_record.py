import traceback
from textwrap import dedent
from functools import partial
from contextlib import contextmanager

from . import config
from .db import get_db_engine


def start(process_name, process_id):
    with get_db_engine().connect() as conn:
        with conn.begin():
            conn.execute(dedent('''
                create table if not exists processing_record (
                    process_id varchar(255),
                    process_name varchar(255),
                    started_at timestamp not null default now(),
                    finished_at timestamp
                );
                create table if not exists processing_record_log (
                    process_id varchar(255),
                    process_name varchar(255),
                    log_at timestamp not null default now(),
                    log text
                );
                insert into processing_record (process_id, process_name) values (%s, %s);
            '''), (process_id, process_name))


def log(process_name, process_id, log):
    with get_db_engine().connect() as conn:
        with conn.begin():
            conn.execute(dedent('''
                insert into processing_record_log (process_id, process_name, log) values (%s, %s, %s);
            '''), (process_id, process_name, log))


def finish(process_name, process_id):
    with get_db_engine().connect() as conn:
        with conn.begin():
            conn.execute(dedent('''
                update processing_record set finished_at = now() where process_id = %s and process_name = %s;
            '''), (process_id, process_name))


@contextmanager
def processing_record(delayed_start=False):
    if config.PROCESSING_RECORD_ENABLED:
        if not delayed_start:
            start(config.PROCESSING_RECORD_NAME, config.PROCESSING_RECORD_ID)
        log_partial = partial(log, config.PROCESSING_RECORD_NAME, config.PROCESSING_RECORD_ID)
        try:
            if delayed_start:
                yield partial(start, config.PROCESSING_RECORD_NAME, config.PROCESSING_RECORD_ID), log_partial
            else:
                yield log_partial
        except:
            log_partial(traceback.format_exc())
            raise Exception()
        finally:
            finish(config.PROCESSING_RECORD_NAME, config.PROCESSING_RECORD_ID)
    elif delayed_start:
        yield lambda: None, print
    else:
        yield print


def clear():
    with get_db_engine().connect() as conn:
        with conn.begin():
            if conn.execute('select count(1) from processing_record_log;').first().count > 100000:
                print('Clearing processing_record_log')
                conn.execute('''
                    drop table processing_record_log;
                ''')
            else:
                print('Skipping clearing processing_record_log')


def get_last_finished_at(process_name):
    with get_db_engine().connect() as conn:
        with conn.begin():
            row = conn.execute(dedent(f'''
                select * from processing_record
                where process_name = '{process_name}' and finished_at is not null
                order by finished_at desc
                limit 1
            ''')).first()
            return dict(row) if row else None
