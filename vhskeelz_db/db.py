import time
import contextlib
from urllib.parse import quote_plus

from sqlalchemy.engine.create import create_engine
from sqlalchemy import pool

from . import config


def get_db_engine():
    return create_engine(
        f'postgresql+psycopg2://{config.PGSQL_USER}:{quote_plus(config.PGSQL_PASSWORD)}@{config.PGSQL_HOST}:{config.PGSQL_PORT}/{config.PGSQL_DB}',
        echo=False,
        poolclass=pool.StaticPool
    )


def execute_sqls(conn, sqls):
    if sqls:
        sql = '\n'.join(sqls)
        try:
            with conn.begin():
                conn.execute(sql)
        except Exception as e:
            raise Exception(f'Error executing sql:\n{sql}') from e


@contextlib.contextmanager
def conn_transaction_sql_handler(conn):
    state = {
        'first_execute': None,
        'last_commit': time.time(),
        'sqls': [],
    }

    def sql_execute(sql, force_commit=False):
        if state['first_execute'] is None:
            state['first_execute'] = time.time()
        if time.time() - state['first_execute'] > 60 * 60 * 5:
            force_commit = True
        assert isinstance(sql, str), f'sql_execute argument must be a string, not {type(sql)}: {sql}'
        state['sqls'].append(sql)
        if force_commit or time.time() - state['last_commit'] > 120:
            execute_sqls(conn, state['sqls'])
            state['sqls'] = []
            state['last_commit'] = time.time()

    try:
        yield sql_execute
    finally:
        execute_sqls(conn, state['sqls'])
