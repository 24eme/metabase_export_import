# Metabase Export/Import

This python library allows to export and import a community version instance of Metabase

## Example Scripts

Two scripts are provided to import and export fields, cards and dashboards of a specific database configuration of metabase:

```shell
# Export each component separately
python3 metabase_export.py [cards|fields|metrics|dashboards]

# Export everything together
python3 metabase_export.py all

# Export everything together and store the raw JSON without parsing it
python3 metabase_export.py --raw all
```

The script produces 3 files for each exported elements (the name of the database is user as prefix) : `my_database_fields_exported.csv`, `my_database_cards_exported.json` and `my_database_dashboard_exported.json`

```shell
# Import each component separately
python3 metabase_import.py [cards|fields|metrics|dashboards]

# Export everything together
python3 metabase_import.py all 
```

The script imports from 3 files, one for each elements : `my_database_fields_forimport.csv`, `my_database_cards_forimport.json` and `my_database_dashboard_forimport.json`

## Configuration

Available flags for the scripts:

| flag      | description                                     |
|-----------|-------------------------------------------------|
| --verbose | increase output verbosity of each command       |
| --dry-run | run the script without making POST/PUT requests |
| --raw     | store the raw JSON without parsing it           |

It is recommended to predefine all the following environment variables in a `.env` file:

| env var              | description                                               |
|----------------------|-----------------------------------------------------------|
| `MB_DATA_DIR`        | Directory to use for the export/import operations         |
| `MB_EXPORT_HOST`     | Source Metabase instance (`https://<url>/api/`)           |
| `MB_EXPORT_USERNAME` | Admin username                                            |
| `MB_EXPORT_PASSWORD` | Admin username password                                   |
| `MB_EXPORT_DB`       | The database name to export                               |
| `MB_IMPORT_HOST`     | Destination Metabase instance (`https://<url>/api/`)      |
| `MB_IMPORT_USERNAME` | Admin username                                            |
| `MB_IMPORT_PASSWORD` | Admin username password                                   |
| `MB_IMPORT_DB`       | The name of the same database in the destination instance |

## Library calls

### database creation/deletion

```python
import metabase

# connect to metabase
ametabase = metabase.MetabaseApi("http://localhost:3000/api/", "metabase_username", "metabase_password")

# add a sqlite database located at /path/to/database.sqlite. The metabase associated name is my_database
ametabase.create_database("my_database", 'sqlite', {"db":"/path/to/database.sqlite"})

#ametabase.delete_database('my_database')
```


### users and permisssions

```python
ametabase.create_user("user@example.org", "the_password", {'first_name': 'John', 'last_name': 'Doe'})

# Add a group and associate it with our new user
ametabase.membership_add('user@example.org', 'a_group')

# allow read data and create interraction with my_database for users members of our new group (a_group)
ametabase.permission_set_database('a_group', 'my_database', True, True)
```

### collections and permissions

```python
# create a collection and its sub collection
ametabase.create_collection('sub_collection', 'main_collection')

# allow write right on the new collections to the membres of a_group
ametabase.permission_set_collection('main_collection', 'a_group', 'write')
ametabase.permission_set_collection('sub_collection', 'a_group', 'write')
```


### schema

```python
# export and import the schema of fields
ametabase.export_fields_to_csv('my_database', 'my_database_fields.csv')
ametabase.import_fields_from_csv('my_database', 'my_database_fields.csv')
```

### cards and dashboards

```python
ametabase.export_cards_to_json('my_database', 'my_database_cards.json')
ametabase.export_dashboards_to_json('my_database', 'my_database_dashboard.json')

ametabase.import_cards_from_json('my_database', 'my_database_cards.json')
ametabase.import_dashboards_from_json('my_database', 'my_database_dashboard.json')
```

## Development

Development of this repository is done with [Poetry](https://python-poetry.org/docs/).

### Install dependencies

```shell
make install
```

### Test

The tests are running with pytest

```shell
make test

# Run a tests matching an expression TEST_FUNC=<expression>
TEST_FUNC="invalid" make test
```
