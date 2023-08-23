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
