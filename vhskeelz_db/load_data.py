import os
import time
import random
import traceback
from textwrap import dedent


import dataflows as DF


from . import config, extract_data
from .db import get_db_engine
from .processing_record import processing_record


class df_load_reduplicate_headers(DF.load):

    @staticmethod
    def rename_duplicate_headers(duplicate_headers):
        return duplicate_headers


def validate_data(table_name):
    if config.EXTRACT_DATA_TABLES[table_name]['type'] == 'skeelz_export':
        max_num_rows = None
    else:
        max_num_rows = 150000

    def _iterator(rows):
        num_rows = 0
        for row in rows:
            num_rows += 1
            if max_num_rows and num_rows >= max_num_rows:
                raise Exception(f"Got {max_num_rows} rows, Looker Studio doesn't support more then {max_num_rows} rows, so we can't know for sure if the data is complete")
            yield row

    return _iterator


def load_table(table_name):
    file_path = os.path.join(config.EXTRACT_DATA_PATH, f'{table_name}.csv')
    assert os.path.exists(file_path), f'File not found: {file_path}'
    temp_table_name = f'__temp__{table_name}'
    DF.Flow(
        df_load_reduplicate_headers(file_path, name=table_name, infer_strategy=DF.load.INFER_STRINGS, deduplicate_headers=True),
        validate_data(table_name),
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


def main(log, extract=False, only_table_name=None, cache=None, only_table_types=None):
    if only_table_types:
        only_table_types = [t.strip() for t in only_table_types.split(',') if t.strip()]
    if extract:
        for table_name in extract_data.main(log, only_table_name=only_table_name, cache=cache, only_table_types=only_table_types):
            log(f'Extracted {table_name}')
            load_table(table_name)
            yield table_name
    else:
        for table_name, table_config in config.EXTRACT_DATA_TABLES.items():
            if table_config['type'] == 'view':
                continue
            if only_table_types and table_config['type'] not in only_table_types:
                continue
            if only_table_name is None or only_table_name == table_name:
                load_table(table_name)
                yield table_name
    for name, table in config.EXTRACT_DATA_TABLES.items():
        if only_table_types and table['type'] not in only_table_types:
            continue
        if only_table_name is None or only_table_name == name:
            if table['type'] == 'view':
                tables_start_with = table['tables_start_with']
                view_table_names = [n for n in config.EXTRACT_DATA_TABLES.keys() if n.startswith(tables_start_with) and n != name]
                sql = f'drop table if exists __temp__{name};\n'
                sql += f'create table __temp__{name} as\n'
                for i, view_table_name in enumerate(view_table_names):
                    if i > 0:
                        sql += 'union all\n'
                    sql += f'select * from {view_table_name}\n'
                sql += ';\n'
                sql += f'drop table if exists {name};\n'
                sql += f'alter table __temp__{name} rename to {name};\n'
                with get_db_engine().connect() as conn:
                    with conn.begin():
                        conn.execute(sql)
                log(f'Updated view {name}')


def ensure_updated_tables(log, table_names):
    needs_skeelz_export = False
    for table_name in table_names:
        if config.EXTRACT_DATA_TABLES[table_name]['type'] == 'skeelz_export':
            needs_skeelz_export = True
            break
    cache = {}
    with extract_data.get_extract_skeelz_exports_context(cache, needs_skeelz_export=needs_skeelz_export) as _:
        for table_name in table_names:
            log(f'Updating table {table_name}...')
            i = 0
            updated_table_names = []
            while True:
                i += 1
                try:
                    updated_table_names = list(main(log, extract=True, only_table_name=table_name, cache=cache))
                    break
                except:
                    if i > 3:
                        raise
                    else:
                        log(traceback.format_exc())
                        log(f'Failed to update table {table_name}, retrying ({i})...')
                        time.sleep(random.randint(2, 10))
            assert len(updated_table_names) == 1, updated_table_names
            assert updated_table_names[0] == table_name, updated_table_names
            log('OK')
