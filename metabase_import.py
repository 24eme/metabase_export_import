import metabase
from pathlib import Path

from typer import Typer, Option
from typing_extensions import Annotated

app = Typer()

db_name: str
data_dir: Path
metabaseAPI: metabase.MetabaseApi


@app.command('all')
def import_all(collection: str):
    fields()
    metrics()
    cards(collection)
    dashboards(collection)


@app.command()
def fields():
    metabaseAPI.import_fields_from_csv(db_name, str(data_dir))


@app.command()
def metrics():
    metabaseAPI.import_metrics_from_json(db_name, str(data_dir))


@app.command()
def cards(collection: str):
    metabaseAPI.import_cards_from_json(db_name, str(data_dir), collection)


@app.command()
def dashboards(collection: str):
    metabaseAPI.import_dashboards_from_json(db_name, str(data_dir), collection)


@app.callback()
def common(api_url: Annotated[str, Option(envvar='MB_EXPORT_HOST')],
           username: Annotated[str, Option(envvar='MB_EXPORT_USERNAME')],
           password: Annotated[str, Option(envvar='MB_EXPORT_PASSWORD')],
           database: Annotated[str, Option(envvar='MB_EXPORT_DB')],
           data: Annotated[Path, Option(envvar='MB_DATA_DIR')],
           verbose: bool = False):
    global db_name, data_dir, metabaseAPI

    metabaseAPI = metabase.MetabaseApi(api_url, username, password)
    metabaseAPI.debug = verbose

    db_name = database
    data_dir = data


def main():
    app()


if __name__ == '__main__':
    main()
