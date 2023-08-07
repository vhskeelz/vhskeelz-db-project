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
def update_smoove_contacts():
    from . import update_smoove_contacts
    with processing_record() as log:
        update_smoove_contacts.main(log)
    print("OK")


@main.command()
@click.argument('POSITION_ID')
@click.argument('CANDIDATE_ID')
@click.option('--headless', is_flag=True)
def download_position_candidate_cv(**kwargs):
    from . import download_position_candidate_cv
    with processing_record() as log:
        download_position_candidate_cv.main(log, **kwargs)
    print("OK")


@main.command()
@click.argument('POSITION_CANDIDATE_IDS_JSON')
@click.option('--headless', is_flag=True)
def download_position_candidate_cv_multi(**kwargs):
    kwargs['position_candidate_ids'] = json.loads(kwargs.pop('position_candidate_ids_json'))
    from . import download_position_candidate_cv
    with processing_record() as log:
        download_position_candidate_cv.main_multi(log, **kwargs)
    print("OK")
