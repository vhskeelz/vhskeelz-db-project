import click


@click.group()
def main():
    pass


@main.command()
@click.option('--force', is_flag=True)
@click.option('--debug', is_flag=True)
@click.option('--headless', is_flag=True)
def extract_data(**kwargs):
    from . import extract_data
    for report_url, report_filename in extract_data.main(**kwargs):
        print(f'{report_url} -> {report_filename}')
