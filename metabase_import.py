from pathlib import Path
from typing import Optional

from typer import Argument, Option, Typer
from typing_extensions import Annotated

import metabase

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
def fields(field_ids: Annotated[Optional[list[str]], Argument()] = None):
    metabaseAPI.import_fields_from_csv(db_name, str(data_dir), field_ids)


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
def common(api_url: Annotated[str, Option(envvar='MB_IMPORT_HOST')],
           username: Annotated[str, Option(envvar='MB_IMPORT_USERNAME')],
           password: Annotated[str, Option(envvar='MB_IMPORT_PASSWORD')],
           database: Annotated[str, Option(envvar='MB_IMPORT_DB')],
           data: Annotated[Path, Option(envvar='MB_DATA_DIR')],
           verbose: bool = False,
           dry_run: bool = False):
    global db_name, data_dir, metabaseAPI

    metabaseAPI = metabase.MetabaseApi(api_url, username, password, verbose, dry_run)

    db_name = database
    data_dir = data


def main():
    app()


if __name__ == '__main__':
    main()
