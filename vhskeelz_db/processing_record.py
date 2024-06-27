import datetime
import traceback
from textwrap import dedent
from functools import partial
from contextlib import contextmanager

from . import config
from .db import get_db_engine, conn_transaction_sql_handler


class ProcessingRecordLogCommandSetStatusSuccess:
    pass


def start(process_name, process_id):
    with get_db_engine().connect() as conn:
        with conn.begin():
            conn.execute(dedent('''
                create table if not exists processing_record (
                    process_id varchar(255),
                    process_name varchar(255),
                    started_at timestamp not null default now(),
                    finished_at timestamp,
                    status varchar(255)
                );
                create table if not exists processing_record_log (
                    process_id varchar(255),
                    process_name varchar(255),
                    log_at timestamp not null default now(),
                    log text
                );
                insert into processing_record (process_id, process_name) values (%s, %s);
            '''), (process_id, process_name))


def log(sql_execute, process_name, process_id, log_):
    if isinstance(log_, ProcessingRecordLogCommandSetStatusSuccess):
        sql_execute(dedent(f'''
            update processing_record set status = 'success' where process_id = '{process_id}' and process_name = '{process_name}';        
        '''), force_commit=True)
    else:
        log_ = log_.replace("'", "''").replace('%', '%%')
        now = datetime.datetime.now().isoformat()
        sql_execute(dedent(f'''
            insert into processing_record_log (process_id, process_name, log, log_at) values ('{process_id}', '{process_name}', '{log_}', '{now}');
        '''))


def finish(process_name, process_id):
    with get_db_engine().connect() as conn:
        with conn.begin():
            conn.execute(dedent('''
                update processing_record set finished_at = now() where process_id = %s and process_name = %s;
            '''), (process_id, process_name))


def ignore_exceptions_if_recent_success(process_name):
    if config.PROCESSING_RECORD_CONTEXT_IGNORE_EXCEPTIONS_IF_RECENT_SUCCESS:
        interval = config.PROCESSING_RECORD_CONTEXT_IGNORE_EXCEPTIONS_IF_RECENT_SUCCESS_INTERVAL
        with get_db_engine().connect() as conn:
            with conn.begin():
                row = conn.execute(dedent(f'''
                    select 1 from processing_record
                    where process_name = '{process_name}' and status = 'success' and finished_at >= now() - interval '{interval}'
                ''')).first()
                if row:
                    return True
                else:
                    return False
    else:
        return False


@contextmanager
def processing_record(delayed_start=False):
    if config.PROCESSING_RECORD_ENABLED:
        with get_db_engine().connect() as conn:
            with conn_transaction_sql_handler(conn) as sql_execute:
                if not delayed_start:
                    start(config.PROCESSING_RECORD_NAME, config.PROCESSING_RECORD_ID)
                log_partial = partial(log, sql_execute, config.PROCESSING_RECORD_NAME, config.PROCESSING_RECORD_ID)
                try:
                    if delayed_start:
                        yield partial(start, config.PROCESSING_RECORD_NAME, config.PROCESSING_RECORD_ID), log_partial
                    else:
                        yield log_partial
                except:
                    log_partial(traceback.format_exc())
                    if not ignore_exceptions_if_recent_success(config.PROCESSING_RECORD_NAME):
                        raise Exception()
                else:
                    log_partial(ProcessingRecordLogCommandSetStatusSuccess())
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
