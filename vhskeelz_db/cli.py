import click
from .processing_record import processing_record


@click.group()
def main():
    pass


@main.command()
@click.argument('PROCESS_NAME')
@click.argument('PROCESS_ID')
def processing_record_start(**kwargs):
    from . import processing_record
    processing_record.start(**kwargs)


@main.command()
@click.argument('PROCESS_NAME')
@click.argument('PROCESS_ID')
@click.argument('LOG')
def processing_record_log(**kwargs):
    from . import processing_record
    processing_record.log(**kwargs)


@main.command()
@click.argument('PROCESS_NAME')
@click.argument('PROCESS_ID')
def processing_record_finish(process_name, process_id):
    from . import processing_record
    processing_record.finish(process_name, process_id)


@main.command()
def processing_record_clear():
    from . import processing_record
    processing_record.clear()
    print("OK")


@main.command()
@click.option('--only-table-name')
def extract_data(**kwargs):
    from . import extract_data
    with processing_record() as log:
        for table_name in extract_data.main(log, **kwargs):
            log(f'Extracted {table_name}')
    print("OK")


@main.command()
@click.option('--extract', is_flag=True)
@click.option('--only-table-name')
def load_data(**kwargs):
    from . import load_data
    with processing_record() as log:
        for table_name in load_data.main(log, **kwargs):
            log(f'Loaded {table_name}')
    print("OK")


@main.command()
def populate_mailing_list_data():
    from . import populate_mailing_list_data
    with processing_record() as log:
        populate_mailing_list_data.main(log)
    print("OK")
