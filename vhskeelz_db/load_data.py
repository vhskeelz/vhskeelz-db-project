import os
from textwrap import dedent
from urllib.parse import quote_plus

import dataflows as DF
from sqlalchemy.engine.create import create_engine

from . import config, extract_data


def get_db_engine():
    return create_engine(
        f'postgresql+psycopg2://{config.PGSQL_USER}:{quote_plus(config.PGSQL_PASSWORD)}@{config.PGSQL_HOST}:{config.PGSQL_PORT}/{config.PGSQL_DB}',
        echo=False
    )


class df_load_reduplicate_headers(DF.load):

    @staticmethod
    def rename_duplicate_headers(duplicate_headers):
        return duplicate_headers


def load_table(table_name):
    file_path = os.path.join(config.EXTRACT_DATA_PATH, f'{table_name}.csv')
    assert os.path.exists(file_path), f'File not found: {file_path}'
    temp_table_name = f'__temp__{table_name}'
    DF.Flow(
        df_load_reduplicate_headers(file_path, name=table_name, infer_strategy=DF.load.INFER_STRINGS, deduplicate_headers=True),
        DF.dump_to_sql(
            {temp_table_name: {'resource-name': table_name}},
            get_db_engine(),
            batch_size=100000,
        )
    ).process()
    with get_db_engine().connect() as conn:
        with conn.begin():
            conn.execute(dedent(f'''
                    drop table if exists {table_name};
                    alter table {temp_table_name} rename to {table_name};
                '''))


def main(extract=False, only_table_name=None):
    if extract:
        for table_name in extract_data.main(only_table_name=only_table_name):
            print(f'Extracted {table_name}')
            load_table(table_name)
            yield table_name
    else:
        for table_name in config.EXTRACT_DATA_TABLES.keys():
            if only_table_name is None or only_table_name == table_name:
                load_table(table_name)
                yield table_name
