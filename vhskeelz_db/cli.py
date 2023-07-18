import click


@click.group()
def main():
    pass


@main.command()
@click.option('--only-table-name')
def extract_data(**kwargs):
    from . import extract_data
    for table_name in extract_data.main(**kwargs):
        print(f'Extracted {table_name}')
    print("OK")


@main.command()
@click.option('--extract', is_flag=True)
@click.option('--only-table-name')
def load_data(**kwargs):
    from . import load_data
    for table_name in load_data.main(**kwargs):
        print(f'Loaded {table_name}')
    print("OK")
