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
def processing_record():
    if config.PROCESSING_RECORD_ENABLED:
        start(config.PROCESSING_RECORD_NAME, config.PROCESSING_RECORD_ID)
        try:
            yield partial(log, config.PROCESSING_RECORD_NAME, config.PROCESSING_RECORD_ID)
        finally:
            finish(config.PROCESSING_RECORD_NAME, config.PROCESSING_RECORD_ID)
    else:
        yield print


def clear():
    with get_db_engine().connect() as conn:
        with conn.begin():
            conn.execute('drop table processing_record_log; drop table processing_record;')
