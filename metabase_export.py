import metabase
from pathlib import Path

from typer import Typer, Option
from typing_extensions import Annotated

app = Typer()

db_name: str
data_dir: Path
raw_mode: bool
metabaseAPI: metabase.MetabaseApi


@app.command('all')
def export_all():
    fields()
    cards()
    dashboards()
    metrics()


@app.command()
def fields():
    metabaseAPI.export_fields_to_csv(db_name, str(data_dir))


@app.command()
def metrics():
    metabaseAPI.export_metrics_to_json(db_name, str(data_dir), raw_mode)


@app.command()
def cards():
    metabaseAPI.export_cards_to_json(db_name, str(data_dir), raw_mode)


@app.command()
def dashboards():
    metabaseAPI.export_dashboards_to_json(db_name, str(data_dir), raw_mode)


@app.callback()
def common(api_url: Annotated[str, Option(envvar='MB_EXPORT_HOST')],
           username: Annotated[str, Option(envvar='MB_EXPORT_USERNAME')],
           password: Annotated[str, Option(envvar='MB_EXPORT_PASSWORD')],
           database: Annotated[str, Option(envvar='MB_EXPORT_DB')],
           data: Annotated[Path, Option(envvar='MB_DATA_DIR')],
           verbose: bool = False,
           dry_run: bool = False,
           raw: bool = False):
    global db_name, data_dir, metabaseAPI, raw_mode

    metabaseAPI = metabase.MetabaseApi(api_url, username, password, verbose, dry_run)

    db_name = database
    data_dir = data
    raw_mode = raw

    data_dir.mkdir(exist_ok=True)


def main():
    app()


if __name__ == '__main__':
    main()
